################################################################################
# MogileFS::Client::Async::HTTPFile object
# NOTE: This is meant to be used within IO::WrapTie...
#

package MogileFS::Client::Async::HTTPFile;

use strict;
use warnings;

use Carp;
use AnyEvent;
use AnyEvent::Socket;
use Net::HTTP;

use base qw/ MogileFS::NewHTTPFile /;

use fields qw/
    hdl
    on_hdl_cb
/;

BEGIN {
    no strict 'refs';
    foreach my $name (qw/ format_chunk format_chunk_eof /) {
        *{$name} = Net::HTTP->can($name);
    }
}

sub _sock_to_host { # (host)
    my MogileFS::Client::Async::HTTPFile $self = shift;
    my $host = shift;

    # setup
    my ($ip, $port) = $host =~ /^(.*):(\d+)$/;
    my $sock = "Sock_$host";
    my $proto = getprotobyname('tcp');
    my $sin;

    # create the socket
    socket($sock, PF_INET, SOCK_STREAM, $proto);
    $sin = Socket::sockaddr_in($port, Socket::inet_aton($ip));

    # unblock the socket
    IO::Handle::blocking($sock, 0);

    # attempt a connection
    my $ret = connect($sock, $sin);
    if (!$ret) { # && $! == EINPROGRESS) {
        my $win = '';
        vec($win, fileno($sock), 1) = 1;

        # watch for writeability
        if (select(undef, $win, undef, 3) > 0) {
            $ret = connect($sock, $sin);

            # EISCONN means connected & won't re-connect, so success
            #$ret = 1 if !$ret && $! == EISCONN;
        }
    }

    # just throw back the socket we have
    return $sock if $ret;
    return undef;
}

sub _connect_sock {
    my MogileFS::Client::Async::HTTPFile $self = shift;
    return 1 if $self->{sock};

    my @down_hosts;

    while (!$self->{sock} && $self->{host}) {
        # attempt to connect
        return 1 if
            $self->{sock} = $self->_sock_to_host($self->{host});

        push @down_hosts, $self->{host};
        if (my $dest = shift @{$self->{backup_dests}}) {
            # dest is [$devid,$path]
            _debug("connecting to $self->{host} (dev $self->{devid}) failed; now trying $dest->[1] (dev $dest->[0])");
            $self->_parse_url($dest->[1]) or _fail("bogus URL");
            $self->{devid} = $dest->[0];
        } else {
            $self->{host} = undef;
        }
    }

    _fail("unable to open socket to storage node (tried: @down_hosts): $!");
}

# abstracted read; implements what ends up being a blocking read but
# does it in terms of non-blocking operations.
sub _getline {
    my MogileFS::Client::Async::HTTPFile $self = shift;
    my $timeout = shift || 3;
    return undef unless $self->{sock};

    # short cut if we already have data read
    if ($self->{data_in} =~ s/^(.*?\r?\n)//) {
        return $1;
    }

    my $rin = '';
    vec($rin, fileno($self->{sock}), 1) = 1;

    # nope, we have to read a line
    my $nfound;
    my $t1 = Time::HiRes::time();
    while ($nfound = select($rin, undef, undef, $timeout)) {
        my $data;
        my $bytesin = sysread($self->{sock}, $data, 1024);
        if (defined $bytesin) {
            # we can also get 0 here, which means EOF.  no error, but no data.
            $self->{data_in} .= $data if $bytesin;
        } else {
            #next if $! == EAGAIN;
            _fail("error reading from node for device $self->{devid}: $!");
        }

        # return a line if we got one
        if ($self->{data_in} =~ s/^(.*?\r?\n)//) {
            return $1;
        }

        # and if we got no data, it's time to return EOF
        unless ($bytesin) {
            $@ = "\$bytesin is 0";
            return undef;
        }
    }

    # if we got here, nothing was readable in our time limit
    my $t2 = Time::HiRes::time();
    $@ = sprintf("not readable in %0.02f seconds", $t2-$t1);
    return undef;
}

# abstracted write function that uses non-blocking I/O and checking for
# writeability to ensure that we don't get stuck doing a write if the
# node we're talking to goes down.  also handles logic to fall back to
# a backup node if we're on our first write and the first node is down.
# this entire function is a blocking function, it just uses intelligent
# non-blocking write functionality.
#
# this function returns success (1) or it croaks on failure.
sub _write {
    my MogileFS::Client::Async::HTTPFile $self = shift;
    return undef unless $self->{sock};

    my $win = '';
    vec($win, fileno($self->{sock}), 1) = 1;

    # setup data and counters
    my $data = shift();
    my $bytesleft = length($data);
    my $bytessent = 0;

    # main sending loop for data, will keep looping until all of the data
    # we've been asked to send is sent
    my $nfound;
    while ($bytesleft && ($nfound = select(undef, $win, undef, 3))) {
        my $bytesout = syswrite($self->{sock}, $data, $bytesleft, $bytessent);
        if (defined $bytesout) {
            # update our myriad counters
            $bytessent += $bytesout;
            $self->{bytes_out} += $bytesout;
            $bytesleft -= $bytesout;
        } else {
            # if we get EAGAIN, restart the select loop, else fail
            #next if $! == EAGAIN;
            _fail("error writing to node for device $self->{devid}: $!");
        }
    }
    return 1 unless $bytesleft;

    # at this point, we had a socket error, since we have bytes left, and
    # the loop above didn't finish sending them.  if this was our first
    # write, let's try to fall back to a different host.
    unless ($self->{bytes_out}) {
        if (my $dest = shift @{$self->{backup_dests}}) {
            # dest is [$devid,$path]
            $self->_parse_url($dest->[1]) or _fail("bogus URL");
            $self->{devid} = $dest->[0];
            $self->_connect_sock;

            # now repass this write to try again
            return $self->_write($data);
        }
    }

    # total failure (croak)
    $self->{sock} = undef;
    _fail(sprintf("unable to write to any allocated storage node, last tried dev %s on host %s uri %s. Had sent %s bytes, %s bytes left", $self->{devid}, $self->{host}, $self->{uri}, $self->{path}, $self->{bytes_out}, $bytesleft));
}

sub TIEHANDLE {
    my $class = shift;
    my $self = $class->SUPER::TIEHANDLE(@_);
    $self->_get_sock();
    return $self;
}

sub PRINT {
    my MogileFS::Client::Async::HTTPFile $self = shift;

    # get data to send to server
    my $data = shift;
    my $newlen = length $data;
    $self->{pos} += $newlen;

    $self->{length} += $newlen;
    $self->{data} .= $data;

    if ($self->{hdl}) {
        $self->{hdl}->push_write($self->_get_data_chunk);
    }
}
*print = *PRINT;

sub _get_data_chunk {
    my ($self) = @_;
    return undef unless length $self->{data};
    my $data = $self->{data};
    $self->{data} = '';
    $self->{length} = 0;
    if ($self->{content_length}) {
        return $data;
    }
    $self->format_chunk($data);
}

sub _get_sock {
    my ($self) = @_;
    warn("Get sock");

    my $host = $self->{host};

    # setup
    my ($ip, $port) = $host =~ /^(.*):(\d+)$/;

    my $g; $g = tcp_connect $ip, $port, sub {
        my ($fh, $host, $port) = @_;
        my $error = $!;
        if (!$fh) {
            die("Did not get socket: $error");
        }
        $self->{sock} = $fh;
        undef $g;
        warn("Got a socket to $host");
        $self->_get_hdl;
    }, sub { 10 };
}

sub _get_hdl {
    my ($self) = @_;
    warn("Get handle " . $self->{hdl});
    my $hdl = $self->{hdl} = AnyEvent::Handle->new(
          fh => $self->{sock},
          on_error => sub {
             my ($hdl, $fatal, $msg) = @_;
             $hdl->destroy;
             undef $self->{hdl};
             die "got error $msg\n";
          },
    );
    warn("Got handle " . $self->{hdl} . " " . ref($self->{hdl}));
    if ($self->{content_length}) {
        $hdl->push_write("PUT $self->{uri} HTTP/1.0\r\nContent-length: $self->{content_length}\r\n\r\n");
    }
    else {
        $hdl->push_write("PUT $self->{uri} HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n");
    }
    warn("End of get_hdl Handle is " . $self->{hdl});
    my $data = $self->_get_data_chunk;
    if (defined $data) {
        warn("Pushed " . length($data) . " bytes in get_hdl");
        $hdl->push_write($data);
    }
    $self->{on_hdl_cb}->() if $self->{on_hdl_cb};
}

sub CLOSE {
    my ($self) = @_;
    my $cv = AnyEvent->condvar;
    warn("Started with CV $cv");
    my $cb = sub {
        my $data = $self->_get_data_chunk;
        if (defined $data) {
            warn("Pushed " . length($data) . " bytes in close");
            $self->{hdl}->push_write($data);
        }
        $self->{hdl}->push_write($self->format_chunk_eof)
            unless $self->{content_length};
        $self->{hdl}->on_drain( sub { $self->_CLOSE($cv) });
    };
    if ($self->{hdl}) {
        $cb->();
    }
    else {
        $self->{on_hdl_cb} = $cb;
    }
    $cv->recv;
}

sub _CLOSE {
    my ($self, $cv) = @_;

    # set a message in $! and $@
    my $err = sub {
        $@ = "$_[0]\n";
        return undef;
    };

    # get response from put
    if ($self->{sock}) {
        my $line = $self->_getline(6);  # wait up to 6 seconds for response to PUT.

        return $err->("Unable to read response line from server ($self->{sock}) after PUT of $self->{length} to $self->{uri}.  _getline says: $@")
            unless defined $line;

        if ($line =~ m!^HTTP/\d+\.\d+\s+(\d+)!) {
            # all 2xx responses are success
            unless ($1 >= 200 && $1 <= 299) {
                my $errcode = $1;
                # read through to the body
                my ($found_header, $body);
                while (defined (my $l = $self->_getline)) {
                    # remove trailing stuff
                    $l =~ s/[\r\n\s]+$//g;
                    $found_header = 1 unless $l;
                    next unless $found_header;

                    # add line to the body, with a space for readability
                    $body .= " $l";
                }
                $body = substr($body, 0, 512) if length $body > 512;
                return $err->("HTTP response $errcode from upload of $self->{uri} to $self->{sock}: $body");
            }
        } else {
            return $err->("Response line not understood from $self->{sock}: $line");
        }
        $self->{sock}->close;
    }

    my MogileFS $mg = $self->{mg};
    my $domain = $mg->{domain};

    my $fid   = $self->{fid};
    my $devid = $self->{devid};
    my $path  = $self->{path};

    my $create_close_args = $self->{create_close_args};

    my $key = shift || $self->{key};

    my $rv = $mg->{backend}->do_request_async("create_close", {
            %$create_close_args,
            fid    => $fid,
            devid  => $devid,
            domain => $domain,
            size   => $self->{content_length} ? $self->{content_length} : $self->{length},
            key    => $key,
            path   => $path,
    }, sub { my $cv = shift; warn("In create closed cb $cv"); $cv->send }, $cv);
#    unless ($rv) {
#        # set $@, as our callers expect $@ to contain the error message that
#        # failed during a close.  since we failed in the backend, we have to
#        # do this manually.
#        return $err->("$mg->{backend}->{lasterr}: $mg->{backend}->{lasterrstr}");
#    }

    return 1;
}
*close = *CLOSE;


sub READ {
    my MogileFS::Client::Async::HTTPFile $self = shift;
    Carp::confess("READ NOT IMPLEMENTED");
}
*read = *READ;


################################################################################
# MogileFS::Client::Async::HTTPFile class methods
#

sub _fail {
    croak "MogileFS::Client::Async::HTTPFile: $_[0]";
}

sub _debug {
    MogileFS::Client::_debug(@_);
}

sub _getset {
    my MogileFS::Client::Async::HTTPFile $self = shift;
    my $what = shift;

    if (@_) {
        # note: we're a TIEHANDLE interface, so we're not QUITE like a
        # normal class... our parameters tend to come in via an arrayref
        my $val = shift;
        $val = shift(@$val) if ref $val eq 'ARRAY';
        return $self->{$what} = $val;
    } else {
        return $self->{$what};
    }
}

sub _fid {
    my MogileFS::Client::Async::HTTPFile $self = shift;
    return $self->{fid};
}

1;
