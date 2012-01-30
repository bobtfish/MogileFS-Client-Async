package MogileFS::Client::CallbackFile;
use strict;
use warnings;
use URI;
use Carp qw/confess/;
use IO::Socket::INET;
use File::Slurp qw/ slurp /;
use Try::Tiny;
use Socket qw/ SO_SNDBUF SOL_SOCKET IPPROTO_TCP /;
use Time::HiRes qw/ gettimeofday tv_interval /;

use base qw/ MogileFS::Client::Async /;

use constant TCP_CORK => ($^O eq "linux" ? 3 : 0); # XXX

use namespace::clean;

=head1 NAME

MogileFS::Client::CallbackFile

=head1 SYNOPSIS

    my $mogfs = MogileFS::Client::Callback->new( ... )

    my $f = $mogfs->store_file_from_callback($class, $length, \%opts);

    $f->($data, 0);

    $f->("", 1); # indicate EOF

=head1 DESCRIPTION

This package inherits from L<MogileFS::Client::Async> and provides an additional
blocking API in which the data you wish to upload is supplied to a callback
function, allowing other processing to take place on data as you read it from
disc or elsewhere.

=head1 SEE ALSO

=over

=item L<MogileFS::Client::Async>

=back

=cut

sub store_file_from_fh {
    my $self = shift;
    return undef if $self->{readonly};

    my ($key, $class, $read_fh, $eventual_length, $opts) = @_;
    $opts ||= {};

    # Extra args to be passed along with the create_open and create_close commands.
    # Any internally generated args of the same name will overwrite supplied ones in
    # these hashes.
    my $create_open_args =  $opts->{create_open_args} || {};
    my $create_close_args = $opts->{create_close_args} || {};

    my @dests;  # ( [devid,path,fid], [devid,path,fid], ... )
    my $get_new_dest = sub {
        if (@dests) {
            return pop @dests;
        }

        foreach (1..5) {
            $self->run_hook('store_file_start', $self, $key, $class, $opts);
            $self->run_hook('new_file_start', $self, $key, $class, $opts);

            my $res = $self->{backend}->do_request(
                create_open => {
                    %$create_open_args,
                    domain => $self->{domain},
                    class  => $class,
                    key    => $key,
                    fid    => $opts->{fid} || 0, # fid should be specified, or pass 0 meaning to auto-generate one
                    multi_dest => 1,
                }
            );

            next unless $res;

            for my $i (1..$res->{dev_count}) {
                push @dests, [ $res->{"devid_$i"}, $res->{"path_$i"}, $res->{fid} ];
            }
            if (@dests) {
                return pop @dests;
            }
        }
        die "Fail to get a destination to write to.";
    };

    my $socket;
    my $last_written_point;
    my $current_dest;

    return sub {
        my ($available_to_read, $eof) = @_;

        my $last_error;

        foreach (1..5) {
            $last_error = undef;

            # Create a connection to the storage backend
            unless ($socket) {
                try {
                    $last_written_point = 0;
                    sysseek($read_fh, 0, 0) or die "seek failed: $!";
                    $last_written_point = 0;
                    $current_dest = $get_new_dest->();

                    my $uri = URI->new($current_dest->[1]);
                    $socket = IO::Socket::INET->new(
                        Timeout => 10,
                        Proto => "tcp",
                        PeerPort => $uri->port,
                        PeerHost => $uri->host,
                    ) or die "connect to ".$current_dest->[1]." failed: $!";
                    my $buf = 'PUT ' . $uri->path . " HTTP/1.0\r\nConnection: close\r\nContent-Length: $eventual_length\r\n\r\n";
                    setsockopt($socket, SOL_SOCKET, SO_SNDBUF, 65536) or warn "could enlarge socket buffer: $!" if (unpack("I", getsockopt($socket, SOL_SOCKET, SO_SNDBUF)) < 65536);
                    setsockopt($socket, IPPROTO_TCP, TCP_CORK, 1) or warn "could not set TCP_CORK" if TCP_CORK;
                    syswrite($socket, $buf)==length($buf) or die "Could not write all: $!";
                }
                catch {
                    $last_error = $_;
                    warn $last_error;
                    $socket = undef;
                };
            }

            # Write as much data as we have
            if ($socket) {
                my $bytes_to_write = $available_to_read - $last_written_point;

                if ($bytes_to_write > 0) {
                    my $c = syssplice($read_fh, $socket, $bytes_to_write, 0);
                    if ($c == $bytes_to_write) {
                        $last_written_point += $c;
                    }
                    else {
                        $last_error = $!;
                        warn "syssplice failed, only $c out of $bytes_to_write written: $!";
                        $socket = undef;
                    }
                }
                elsif ($bytes_to_write < 0) {
                    die "unpossible!";
                }
            }

            if ($socket && $eof) {
                die "Cannot be at eof, only $last_written_point out of $eventual_length written!" unless ($last_written_point == $eventual_length);

                $self->run_hook('new_file_end', $self, $key, $class, $opts);

                my $buf = slurp($socket);
                setsockopt($socket, IPPROTO_TCP, TCP_CORK, 0) or warn "could not unset TCP_CORK" if TCP_CORK;
                unless(close($socket)) {
                    $last_error = $!;
                    warn "could not close socket: $!";
                    $socket = undef;
                    next;
                }

                my ($top, @headers) = split /\r?\n/, $buf;
                if ($top =~ m{HTTP/1.[01]\s+2\d\d}) {
                    # Woo, 200!

                    my $rv = $self->{backend}->do_request
                        ("create_close", {
                            fid    => $current_dest->[2],
                            devid  => $current_dest->[0],
                            domain => $self->{domain},
                            size   => $eventual_length,
                            key    => $key,
                            path   => $current_dest->[1],
                        });

                    if ($rv) {
                        $self->run_hook('store_file_end', $self, $key, $class, $opts);
                        return $eventual_length;
                    }
                    else {
                        $last_error =  "create_close failed";
                        warn $last_error;
                        $socket = undef;
                    }
                }
                else {
                    $last_error = "Got non-200 from remote server $top";
                    warn $last_error;
                    $socket = undef;
                    next;
                }
            }
        }

        die "Mogile write failed: $last_error";
    };
}

1;

