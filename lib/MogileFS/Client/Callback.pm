package MogileFS::Client::Callback;
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

MogileFS::Client::Callback

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

sub store_file_from_callback {
    my $self = shift;
    return undef if $self->{readonly};

    my ($key, $class, $length, $opts) = @_;
    $opts ||= {};

    # Extra args to be passed along with the create_open and create_close commands.
    # Any internally generated args of the same name will overwrite supplied ones in
    # these hashes.
    my $create_open_args =  $opts->{create_open_args} || {};
    my $create_close_args = $opts->{create_close_args} || {};

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

    ) or return undef;

    my $dests = [];  # [ [devid,path], [devid,path], ... ]

    # determine old vs. new format to populate destinations
    unless (exists $res->{dev_count}) {
        push @$dests, [ $res->{devid}, $res->{path} ];
    } else {
        for my $i (1..$res->{dev_count}) {
            push @$dests, [ $res->{"devid_$i"}, $res->{"path_$i"} ];
        }
    }

    my ($error, $devid, $path);
    my @dests = (@$dests, @$dests, @$dests); # 2 retries
    my $try = 0;

    my $socket;
    my $t0;
    my $t1;

    foreach my $dest (@dests) {
        $try++;
        ($devid, $path) = @$dest;
        my $uri = URI->new($path);
         try {
            $t0 = [gettimeofday];
            $socket = IO::Socket::INET->new(
                Timeout => 10,
                Proto => "tcp",
                PeerPort => $uri->port,
                PeerHost => $uri->host,
            ) or die "connect to $path failed: $!";
            $t1 = [gettimeofday];
            my $buf = 'PUT ' . $uri->path . " HTTP/1.0\r\nConnection: close\r\nContent-Length: $length\r\n\r\n";
            setsockopt($socket, SOL_SOCKET, SO_SNDBUF, 65536) or warn "could enlarge socket buffer: $!" if (unpack("I", getsockopt($socket, SOL_SOCKET, SO_SNDBUF)) < 65536);
            setsockopt($socket, IPPROTO_TCP, TCP_CORK, 1) or warn "could not set TCP_CORK" if TCP_CORK;
            syswrite($socket, $buf)==length($buf) or die "Could not write all: $!";
        }
        catch {
            $socket = undef;
            warn "connect to $dest failed: $_";
            if ($opts->{on_failure}) {
                $opts->{on_failure}->({
                    url => $path,
                    bytes_sent => 0,
                    total_bytes => $length,
                    client => 'callback',
                    time_elapsed => tv_interval($t0, [gettimeofday]),
                });
            }
        };
        last if $socket;
    }

    if ($socket) {
        my $eof_condition;
        my $written_bytes = 0;
        return sub {
            my ($data, $eof) = @_;

            # We don't notify you about this error, for it is your fault not the network's.
            die "We've already hit EOF!" if $eof_condition;
            my $length_of_data = length(ref($data) ? $$data : $data);

            # We don't notify you about this error, for it is your fault not the network's.
            die "you're trying to write $written_bytes out of $length!" if $written_bytes > $length;


            # Write failed, so log an error
            if (syswrite($socket, ref($data) ? $$data : $data)!=$length_of_data) {
                if ($opts->{on_failure}) {
                    $opts->{on_failure}->({
                        url => $path,
                        bytes_sent => $written_bytes,
                        total_bytes => $length,
                        client => 'callback',
                        time_elapsed => tv_interval($t0, [gettimeofday]),
                    });
                }

                die "could not write entire buffer: $!";
            }

            $written_bytes += $length_of_data;

            if ($eof) {
                $self->run_hook('new_file_end', $self, $key, $class, $opts);
                $eof_condition = 1;

                # Your fault
                die "EOF at $written_bytes out of $length!" if $written_bytes != $length;

                my $buf = slurp($socket);
                setsockopt($socket, IPPROTO_TCP, TCP_CORK, 0) or warn "could not unset TCP_CORK" if TCP_CORK;
                unless(close($socket)) {
                    if ($opts->{on_failure}) {
                        $opts->{on_failure}->({
                            url => $path,
                            bytes_sent => $written_bytes,
                            total_bytes => $length,
                            client => 'callback',
                            time_elapsed => tv_interval($t0, [gettimeofday]),
                        });
                    }

                    die "could not close socket: $!";
                }
                my ($top, @headers) = split /\r?\n/, $buf;
                if ($top =~ m{HTTP/1.[01]\s+2\d\d}) {
                    # Woo, 200!
                    $self->run_hook('store_file_end', $self, $key, $class, $opts);

                    my $t2 = [gettimeofday];

                    my $rv = $self->{backend}->do_request
                        ("create_close", {
                            fid    => $res->{fid},
                            devid  => $devid,
                            domain => $self->{domain},
                            size   => $length,
                            key    => $key,
                            path   => $path,
                    });

                    if ($opts->{on_success}) {
                        $opts->{on_success}->({
                            url => $path,
                            total_bytes => $written_bytes,
                            connect_time => tv_interval($t0, $t1),
                            time_elapsed => tv_interval($t0, $t2),
                            client => 'callback',
                        });
                    }
                }
                else {
                    if ($opts->{on_failure}) {
                        $opts->{on_failure}->({
                            url => $path,
                            bytes_sent => $written_bytes,
                            total_bytes => $length,
                            client => 'callback',
                            time_elapsed => tv_interval($t0, [gettimeofday]),
                        });
                    }

                    die "Got non-200 from remote server $top";
                }
            }
            return;
        };
    }

    # We failed to get any working hosts to upload to.
    return;
}

1;

