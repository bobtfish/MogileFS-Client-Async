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
use Linux::PipeMagic qw/ syssendfile /;
use IO::AIO qw/ fadvise /;
use LWP::Simple qw/ head /;

use base qw/ MogileFS::Client::Async /;

use constant TCP_CORK => ($^O eq "linux" ? 3 : 0); # XXX

use namespace::clean;

=head1 NAME

MogileFS::Client::CallbackFile

=head1 SYNOPSIS

    my $mogfs = MogileFS::Client::Callback->new( ... )

    open(my $read_fh, "<", "...") or die ...
    my $eventual_length = -s $read_fh;
    my $f = $mogfs->store_file_from_fh($class, $read_fh, $eventual_length, \%opts);

    $f->($eventual_length, 0);

    $f->("", 1); # indicate EOF

=head1 DESCRIPTION

This package inherits from L<MogileFS::Client::Async> and provides an additional
blocking API in which the data you wish to upload is read from a file when
commanded by a callback function.
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

    # Hint to Linux that doubling readahead will probably pay off.
    fadvise($read_fh, 0, 0, IO::AIO::FADV_SEQUENTIAL());

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

            # Calls to the backend may be explodey.
            my $res = $self->{backend}->do_request(
                create_open => {
                    %$create_open_args,
                    domain => $self->{domain},
                    class  => $class,
                    key    => $key,
                    fid    => $opts->{fid} || 0, # fid should be specified, or pass 0 meaning to auto-generate one
                    multi_dest => 1,
                    size   => $eventual_length, # not supported by current version
                }
            );

            next unless $res;

            for my $i (1..$res->{dev_count}) {
                push @dests, {
                    devid => $res->{"devid_$i"},
                    path  => $res->{"path_$i"},
                    fid   => $res->{fid},
                };
            }
            if (@dests) {
                return pop @dests;
            }
        }
        die "Fail to get a destination to write to.";
    };

    # When we have a hiccough in your connection, we mark $socket as undef to
    # indicate that we should reconnect.
    my $socket;


    # We keep track of where we last wrote to.
    my $last_written_point;

    # The pointing to the arrayref we're currently writing to.
    my $current_dest;

    return sub {
        my ($available_to_read, $eof) = @_;

        my $last_error;

        my $fail_write_attempt = sub {
            my ($msg) = @_;
            $last_error = $msg;
            warn $msg;
            $socket = undef;
            $opts->{on_failure}->() if $opts->{on_failure};
        };


        foreach (1..5) {
            $last_error = undef;

            # Create a connection to the storage backend
            unless ($socket) {
                sysseek($read_fh, 0, 0) or die "seek failed: $!";
                try {
                    $last_written_point = 0;
                    $last_written_point = 0;
                    $current_dest = $get_new_dest->();

                    $opts->{on_new_attempt}->($current_dest) if $opts->{on_new_attempt};

                    my $uri = URI->new($current_dest->{path});
                    $socket = IO::Socket::INET->new(
                        Timeout => 10,
                        Proto => "tcp",
                        PeerPort => $uri->port,
                        PeerHost => $uri->host,
                    ) or die "connect to ".$current_dest->{path}." failed: $!";

                    $opts->{on_connect}->() if $opts->{on_connect};

                    my $buf = 'PUT ' . $uri->path . " HTTP/1.0\r\nConnection: close\r\nContent-Length: $eventual_length\r\n\r\n";
                    setsockopt($socket, SOL_SOCKET, SO_SNDBUF, 65536) or warn "could not enlarge socket buffer: $!" if (unpack("I", getsockopt($socket, SOL_SOCKET, SO_SNDBUF)) < 65536);
                    setsockopt($socket, IPPROTO_TCP, TCP_CORK, 1) or warn "could not set TCP_CORK" if TCP_CORK;
                    syswrite($socket, $buf)==length($buf) or die "Could not write all: $!";
                }
                catch {
                    $fail_write_attempt->($_);
                };
            }

            # Write as much data as we have
            if ($socket) {
                my $bytes_to_write = $available_to_read - $last_written_point;

                if ($bytes_to_write > 0) {
                    my $c = syssendfile($socket, $read_fh, $bytes_to_write);
                    if ($c == $bytes_to_write) {
                        $last_written_point += $c;
                    }
                    else {
                        $fail_write_attempt->($_);
                        warn "syssendfile failed, only $c out of $bytes_to_write written: $!";
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
                    $fail_write_attempt->($!);
                    warn "could not close socket: $!";
                    next;
                }

                my ($top, @headers) = split /\r?\n/, $buf;
                if ($top =~ m{HTTP/1.[01]\s+2\d\d}) {
                    # Woo, 200!

                    $opts->{on_http_done}->() if $opts->{http_done};

                    try {
                        my $probe_length = (head($current_dest->{path}))[1];
                        die "probe failed: $probe_length vs $eventual_length" if $probe_length != $eventual_length;
                    }
                    catch {
                        $fail_write_attempt->("HEAD check on newly written file failed: $_");
                        next;
                    };


                    my $rv;
                    try {
                        $rv = $self->{backend}->do_request
                            ("create_close", {
                                fid    => $current_dest->{fid},
                                devid  => $current_dest->{devid},
                                domain => $self->{domain},
                                size   => $eventual_length,
                                key    => $key,
                                path   => $current_dest->{path},
                            });
                    }
                    catch {
                        warn "create_close exploded: $!";
                    };

                    # TODO we used to have a file check to query the size of the
                    # file which we just uploaded to MogileFS.

                    if ($rv) {
                        $self->run_hook('store_file_end', $self, $key, $class, $opts);
                        return $eventual_length;
                    }
                    else {
                        $fail_write_attempt->("create_close failed");
                    }
                }
                else {
                    $self->fail_write_attempt->("Got non-200 from remote server $top");
                    next;
                }
            }

            if ($last_written_point == $available_to_read) {
                return;
            }
        }

        die "Mogile write failed: $last_error";
    };
}

1;

