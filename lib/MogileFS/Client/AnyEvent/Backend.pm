package MogileFS::Client::AnyEvent::Backend;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Socket;

use base qw/ MogileFS::Backend /;

sub do_request {
    my $self = shift;
    my $cv = $self->do_request_async(@_);
    $cv->recv;
}

sub do_request_async {
    my $self = shift;
    my ($cmd, $args, $cb) = @_;

    MogileFS::Backend::_fail("invalid arguments to do_request")
        unless $cmd && $args;

    my $hdl = $self->{handle};
    my $argstr = MogileFS::Backend::_encode_url_string(%$args);
    my $req = "$cmd $argstr\r\n";

    $self->_get_sock(sub {
        my $cv = shift;
        $self->run_hook('do_request_start', $cmd, $self->{last_host_connected});
        my $hdl; $hdl = AnyEvent::Handle->new(
              fh => $self->{sock},
              on_error => sub {
                 my ($hdl, $fatal, $msg) = @_;
                 warn "got error $msg\n";
                 $self->run_hook('do_request_send_error', $cmd, $self->{last_host_connected});
                 $hdl->destroy;
                 $cv->send;
              }
        );

        my $timer = AnyEvent->timer( after => 3, cb => sub {
            $self->run_hook('do_request_read_timeout', $cmd, $self->{last_host_connected});
            $cv->throw("timed out after $self->{timeout}s against $self->{last_host_connected} when sending command: [$req])");
        });

        $hdl->push_write($req);

        $hdl->push_read (line => sub {
            my ($hdl, $line) = @_;

            undef $timer;

            warn "got line <$line>\n";
            $self->run_hook('do_request_finished', $cmd, $self->{last_host_connected});

            # ERR <errcode> <errstr>
            if ($line =~ /^ERR\s+(\w+)\s*(\S*)/) {
                $self->{'lasterr'} = $1;
                $self->{'lasterrstr'} = $2 ? MogileFS::Backend::_unescape_url_string($2) : undef;
                $cv->send(undef);
            }

            # OK <arg_len> <response>
            if ($line =~ /^OK\s+\d*\s*(\S*)/) {
                my $args = MogileFS::Backend::_decode_url_string($1);
                if ($cb) {
                    $cb->($cv, $args);
                }
                else {
                    $cv->send($args);
                }
            }

            $cv->throw("invalid response from server: [$line]");
         });
     });
}

# return a new mogilefsd socket, trying different hosts until one is found,
# or undef if they're all dead
sub _get_sock {
    my ($self, $cb) = shift;

    my $cv = AnyEvent->condvar;

    if ($self->{sock}) {
        $cb->($cv);
        return $cv;
    }

    my $size = scalar(@{$self->{hosts}});
    my $tries = $size > 15 ? 15 : $size;
    my $idx = int(rand() * $size);

    my $now = time();
    my $sock;

    my $retry_count = 1;
    my $hostgen; $hostgen = sub {
        if ($retry_count++ <= $tries) {
            my $host = $self->{hosts}->[$idx++ % $size];

            # try dead hosts every 5 seconds
            return $hostgen->() if $self->{host_dead}->{$host} &&
                                $self->{host_dead}->{$host} > $now - 5;

            return $host;
        }
        return;
    };

    my $get_conn; $get_conn = sub {
        my $host = $hostgen->();
        if (! $host ) {
            $cv->send(undef);
            return $cv;
        }

        my ($ip, $port) = $host =~ /^(.*):(\d+)$/;

        tcp_connect $host, $port, sub {
            my $fh = shift;
            if (! $fh) {
                $self->{host_dead}->{$host} = time();
                return $get_conn->();
            }
            $self->{sock} = $fh;
            $self->{last_host_connected} = $host;
            $cb->($cv);
        }, sub { 0.25 };
    };
    $get_conn->();

    return $cv;
}

1;
