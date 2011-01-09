package MogileFS::Client::Async::Backend;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::Socket;

use base qw/ MogileFS::Backend /;

use fields ('connecting', 'command_queue', 'handle_cache', 'idle_watcher', 'connect_guard');

sub _init {
    my ($class, @args) = @_;
    my $self = $class->SUPER::_init(@args);
    $self->{idle_watcher} = undef;
    delete($self->{connect_guard});
    if ($self->{handle_cache}) {
        $self->{handle_cache}->destroy;
        $self->{handle_cache} = undef;
    }
    $self->{sock_cache} = undef;
    $self->{connecting} = undef;
    $self->{command_queue} = [];
    return $self;
}

sub reload {
    my ($self, @args) = @_;
    my $ret = $self->SUPER::reload(@args);
    $self->_get_sock;
    $self->_start_idle_watcher;
}

sub _cancel_idle_watcher {
    my ($self) = @_;
    undef $self->{idle_watcher};
}

sub _get_response_reader {
    my ($self, $on_read, $on_success, $on_error) = @_;
    return sub {
        my ($hdl, $line) = @_;
        warn("Have response line $line");
        $on_read->();
       # ERR <errcode> <errstr>
       if ($line =~ /^ERR\s+(\w+)\s*(\S*)/) {
           warn("Call error");
            $on_error->($1, MogileFS::Backend::_unescape_url_string($2));
            return;
        }

        # OK <arg_len> <response>
        if ($line =~ /^OK\s+\d*\s*(\S*)/) {
            warn("Call on success");
            $on_success->(MogileFS::Backend::_decode_url_string($1));
            return;
        }

        my $message = "invalid response from server: [$line]";
        $on_error->("invalid response from server: [$line]");
        return;
     }
}

sub _start_idle_watcher {
    my $self = shift;
    return if $self->{idle_watcher};
    warn("Starting idle watcher");
    $self->{idle_watcher} = AnyEvent->idle(cb => sub {
        $self->_cancel_idle_watcher;
        my $cmd = $self->{command_queue}->[0];
        unless ($cmd) {
            warn("No command, kill idle watcher");
            return;
        }

        my ($req, $on_success, $on_error) = @$cmd;
        warn(" REQ $req");
        $on_error ||= sub { warn shift(); };

        my $my_on_success = sub { warn("Success"); shift(@{ $self->{command_queue} }); goto $on_success; };
        my $my_on_error = sub { warn("Error"); shift(@{ $self->{command_queue} }); goto $on_error; };

        $self->_get_sock($my_on_error) or return;
        warn("Got sock");
        $self->run_hook('do_request_start', $cmd, $self->{last_host_connected});
        my $timer = AnyEvent->timer( after => $self->{timeout}, cb => sub {
            $self->run_hook('do_request_read_timeout', $cmd, $self->{last_host_connected});
            $on_error->("timed out after $self->{timeout}s against $self->{last_host_connected} when sending command: [$req])");
            $self->_disconnect_sock;
            $self->_start_idle_watcher;
        });
        $self->{handle_cache}->push_read(line => $self->_get_response_reader(
            sub { # on_read
                undef $timer;
                $self->run_hook('do_request_finished', $cmd, $self->{last_host_connected});
            },
            $my_on_success,
            $my_on_error,
        ));

        $self->{handle_cache}->push_write($req);
     });
}

=head2 do_request ($cmd, $args, [$callback, $cv])

Do a blocking request on the mogile tracker.

Executes the C< $cmd >
string against the tracker with the supplied args hash.

When a response returned by the tracker then if it is successful,
the callback is called and passed the
condvar as it's first argument and the deserialized results as its
subsequent arguments.

The default callback (if one is not supplied) will immediately call
C<< ->send >> on the condvar, passing the arguments.

If the response returned by the tracker is an error then C<< ->croak >>
will be called on the condvar (with the returned arguemnts), then
the callback will be invoked as above.

The C<< ->recv >> method will be called on the condvar to cause this
method to block until complete (and the values returned or an exception
thrown).

=cut

sub do_request {
    my ($self, $cmd, $args) = @_;
    MogileFS::Backend::_fail("invalid arguments to do_request")
        unless $cmd && $args;
    my $cv = AnyEvent->condvar;
    $self->do_request_async($cmd, $args,
        sub { $cv->send(@_) },
        sub {
            my ($lasterr, $lasterrstr ) = @_;
            $self->{lasterr} = $lasterr;
            $self->{lasterrstr} = $lasterrstr;
            $cv->send(undef);
        },
    );
    $cv->recv;
}

=head2 do_request_async ($cmd, $args, $cb, $on_error)

Fully async method for performing a request on the mogile backend.

As above, the callback will be invoked with the condvar and the deserialized
return arguments, and the user may use this to schedule another event.

By passing a custom callback and cv in it is possible to chain multiple
async actions together and (if the condvar is passed from the first to the
last, return results from the end of the chain).

=cut

sub do_request_async {
    my $self = shift;
    my ($cmd, $args, $cb, $on_error) = @_;
    use Data::Dumper;
    local $Data::Dumper::Deparse = 1;
    warn Dumper($on_error);
    MogileFS::Backend::_fail("invalid arguments to do_request_async")
        unless $cmd && $args && $cb && $on_error;
    my $argstr = MogileFS::Backend::_encode_url_string(%$args);
    my $req = "$cmd $argstr\r\n";
    push(@{ $self->{command_queue} }, [$req, $cb, $on_error]);
    $self->_start_idle_watcher;
    return 1;
}

sub _disconnect_sock {
    my ($self) = @_;
    if ($self->{handle_cache}) {
        $self->{handle_cache}->destroy;
    }
    undef $self->{sock_cache};
    undef $self->{handle_cache};
    delete($self->{connect_guard});
}

# Called by the idle watcher
# Return a mogilefs socket if one is available right now.
# Otherwise the idle watcher is cancelled, and we return false (and try to asynchronosely
#   make a connection)
# Once a connection is established, or we have given up, the idle watcher is re-enabled
sub _get_sock {
    my ($self, $on_error) = @_;
    if ($self->{connecting} || $self->{sock_cache}) {
        return $self->{sock_cache};
    }
    $self->_cancel_idle_watcher;
    $self->{connecting} = 1;

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
        warn("No hosts left");
        return;
    };

    my $get_conn; $get_conn = sub {
        my $host = $hostgen->();
        warn("Connecting to " . ($host||'undef - give up'));
        if (! $host ) {
            $on_error->('Cannot establish a connection to any mogile trackers');
            $self->_start_idle_watcher;
            return;
        }

        my ($ip, $port) = $host =~ /^(.*):(\d+)$/;

        $self->{connect_guard} = tcp_connect $ip, $port, sub {
            my $fh = shift;
            if (! $fh) {
                delete($self->{connect_guard});
                $self->{host_dead}->{$host} = time();
                goto $get_conn;
            }
            $self->{last_host_connected} = $host;
            $self->{handle_cache} = AnyEvent::Handle->new(
                fh => $fh,
                on_error => sub {
                    my ($hdl, $fatal, $msg) = @_;
                    warn "got error $msg\n";
                    $self->run_hook('do_request_send_error', '$cmd', $self->{last_host_connected});
                    $self->_disconnect_sock;
                    $self->_start_idle_watcher;
                },
                on_read => sub {
                    my ($hdl, $data) = @_;
                    warn("UNEXPECTED GENERIC READ GOT $data");
                    $self->_disconnect_sock;
                    $self->_start_idle_watcher;
                },
            );
            undef $self->{connecting};
            $self->{sock_cache} = $fh;
            warn("Am connected, start idle watcher");
            $self->_start_idle_watcher;
        }, sub { 0.25 };
    };
    $get_conn->();

    return $self->{sock_cache};
}

1;
