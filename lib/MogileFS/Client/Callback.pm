package MogileFS::Client::Callback;
use strict;
use warnings;
use URI;
use Carp qw/confess/;
use IO::Socket::INET;
use File::Slurp qw/ slurp /;

use base qw/ MogileFS::Client::Async /;

use namespace::clean;

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
    foreach my $dest (@dests) {
        $try++;
        ($devid, $path) = @$dest;
        my $uri = URI->new($path);
        my $socket;
        try {
            $socket = IO::Socket::INET->new(
                Timeout => 10,
                Proto => "tcp",
                PeerPort => $uri->port,
                PeerHost => $uri->host,
            ) or die "connect to $path failed: $!"
            my $buf = 'PUT ' . $uri->path . " HTTP/1.0\r\nConnection: close\r\nContent-Length: $length\r\n\r\n";
            syswrite($socket, $buf)==length($buf) or die "Could not write all: $!";
        }
        catch {
            $socket = undef;
            warn "connect to $dest failed: $!";
        };

        if ($socket) {
            my $eof_condition;
            my $written_bytes = 0;
            return sub {
                my ($data, $eof) = @_;

                die "We've already hit EOF!" if $eof_condition;
                $written_bytes += length($data);

                die "you're trying to write $written_bytes out of $length!" if $written_bytes > $length;
                syswrite($socket, $data)==length($data) or die "could not write entire buffer: $!";

                if ($eof) {
                    $self->run_hook('new_file_end', $self, $key, $class, $opts);
                    $eof_condition = 1;
                    die "EOF at $written_bytes out of $length!" if $written_bytes != $length;

                    my $buf = slurp($socket);
                    my ($top, @headers) = split /\r?\n/, $buf;
                    if ($top =~ m{HTTP/1.[01]\s+2\d\d}) {
                        # Woo, 200!
                        $self->run_hook('store_file_end', $self, $key, $class, $opts);

                        my $rv = $self->{backend}->do_request
                            ("create_close", {
                                fid    => $res->{fid},
                                devid  => $devid,
                                domain => $self->{domain},
                                size   => $length,
                                key    => $key,
                                path   => $path,
                        });


                    }
                    else {
                        die "Got non-200 from remote server $top";
                    }
                }
                return;
            };
        }
    };

    # We failed to get any working hosts to upload to.
    return;
}

1;

