package MogileFS::Client::AnyEvent::Backend;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Socket;

use base qw/ MogileFS::Backend /;

use fields ('connecting', 'command_queue', 'handle_cache', 'idle_watcher');

sub _init {
    my ($class, @args) = @_;
    my $self = $class->SUPER::_init(@args);
    $self->_start_idle_watcher;
    $self->{command_queue} = [];
    return $self;
}

sub _start_idle_watcher {
    my $self = shift;
    return if $self->{idle_watcher};
    $self->{idle_watcher} = AnyEvent->idle(cb => sub {
        $self->_get_sock or return;
        unless (scalar( @{ $self->{command_queue } } )) {
            undef $self->{idle_watcher};
            return;
        }
        my $cmd = shift( @{ $self->{command_queue} } );
        my ($req, $cb, $cv) = @$cmd;
        $self->run_hook('do_request_start', $cmd, $self->{last_host_connected});
        my $timer = AnyEvent->timer( after => $self->{timeout}, cb => sub {
            $self->run_hook('do_request_read_timeout', $cmd, $self->{last_host_connected});
            $cb->throw("timed out after $self->{timeout}s against $self->{last_host_connected} when sending command: [$req])");
            # FIXME - Kill socket!
        });
        warn("PUSH READ");
        $self->{handle_cache}->push_read(line => sub {
            my ($hdl, $line) = @_;
            warn("GOT LINE $line");
            undef $timer;
        
           warn "got line <$line>\n";
           $self->run_hook('do_request_finished', $cmd, $self->{last_host_connected});
       
           # ERR <errcode> <errstr>
           if ($line =~ /^ERR\s+(\w+)\s*(\S*)/) {
                $self->{'lasterr'} = $1;
                $self->{'lasterrstr'} = $2 ? MogileFS::Backend::_unescape_url_string($2) : undef;
                return;
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
                return;
            }

            $cv->throw("invalid response from server: [$line]");
         });

         warn ("PUSH WRITE " . $req );
         $self->{handle_cache}->push_write($req);
     });
}

sub do_request {
    my MogileFS::Backend $self = shift;
    my $cv = $self->do_request_async(@_);
    $cv->recv;
}

sub do_request_async {
    my $self = shift;
    my ($cmd, $args, $cb) = @_;
    my $cv = AnyEvent->condvar;
    MogileFS::Backend::_fail("invalid arguments to do_request")
        unless $cmd && $args;
    Carp::cluck("CMD $cmd");
    my $argstr = MogileFS::Backend::_encode_url_string(%$args);
    my $req = "$cmd $argstr\r\n";
    push(@{ $self->{command_queue} }, [$req, $cb, $cv]);
    $self->_start_idle_watcher;
    return $cv;
}

# return a new mogilefsd socket, trying different hosts until one is found,
# or undef if they're all dead
sub _get_sock {
    my $self = shift;

    if ($self->{connecting} || $self->{sock_cache}) {
        return $self->{sock_cache};;
    }
    $self->{connecting} = 1;

    my $size = scalar(@{$self->{hosts}});
    my $tries = $size > 15 ? 15 : $size;
    my $idx = int(rand() * $size);

    my $now = time();
    my $sock;

    my $retry_count = 1;
    my $hostgen; $hostgen = sub {
        Carp::cluck("In host gen");
        if ($retry_count++ <= $tries) {
            my $host = $self->{hosts}->[$idx++ % $size];

            # try dead hosts every 5 seconds
            return $hostgen->() if $self->{host_dead}->{$host} &&
                                $self->{host_dead}->{$host} > $now - 5;
            warn("Got host $host");
            return $host;
        }
        warn("No hosts left");
        return;
    };

    my $get_conn; $get_conn = sub {
        my $host = $hostgen->();
        warn("Connecting to " . ($host||'undef - give up'));
        if (! $host ) {
            return;
        }

        my ($ip, $port) = $host =~ /^(.*):(\d+)$/;

        tcp_connect $ip, $port, sub {
            my $fh = shift;
            warn("Connect CB $host $port");
            if (! $fh) {
                warn("Host dead");
                $self->{host_dead}->{$host} = time();
                return $get_conn->();
            }
            $self->{sock_cache} = $fh;
            $self->{last_host_connected} = $host;
            $self->{handle_cache} = AnyEvent::Handle->new(
                fh => $self->{sock_cache},
                on_error => sub {
                    my ($hdl, $fatal, $msg) = @_;
                    warn "got error $msg\n";
                    $self->run_hook('do_request_send_error', '$cmd', $self->{last_host_connected});
                    $hdl->destroy;
                    undef $self->{sock_cache};
                    undef $self->{handle_cache};
                },
                on_read => sub {
                    my ($hdl, $data) = @_;
                    warn("GENERIC READ GOT $data");
                },
            );
            undef $self->{connecting};
            warn("Connected");
        }, sub { 0.25 };
    };
    $get_conn->();

    return $self->{sock_cache};
}

1;
