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
use Net::HTTP ();

use base qw/ MogileFS::NewHTTPFile /;

use fields qw/
    hdl
    on_hdl_cb
    closing_cb
    closed_cb
    error_cb
/;

BEGIN {
    no strict 'refs';
    foreach my $name (qw/ format_chunk format_chunk_eof /) {
        *{$name} = Net::HTTP->can($name);
    }
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


sub TIEHANDLE {
    my $self = shift;
    $self = fields::new($self) unless ref $self;
    $self = $self->SUPER::TIEHANDLE(@_);
    my %args = @_;
    $self->{$_} = $args{$_} for qw/ closing_cb closed_cb /;
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

    warn("PRINT $data");

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
        warn("Pushed " . length($data) . " bytes in get_hdl " . $data);
        $hdl->push_write($data);
    }
    $self->{on_hdl_cb}->() if $self->{on_hdl_cb};
}

sub CLOSE {
    my ($self) = @_;
    my $cb = sub {
        my $data = $self->_get_data_chunk;
        if (defined $data) {
            warn("Pushed " . length($data) . " bytes in close " . $data);
            $self->{hdl}->push_write($data);
        }
        $self->{hdl}->push_write($self->format_chunk_eof)
            unless $self->{content_length};
        $self->{hdl}->on_drain( sub { $self->_CLOSE() });
    };
    if ($self->{hdl}) {
        $cb->();
    }
    else {
        $self->{on_hdl_cb} = $cb;
    }
    $self->{closing_cb}->();
}

sub _CLOSE {
    my ($self) = @_;

    # set a message in $! and $@
    my $err = sub {
        $@ = "$_[0]\n";
        return undef;
    };

     # FIXME - Cheat here, the response should be small, so we assume it'll allways all be
     #         readable at once, THIS MAY NOT BE TRUE!!!
     my $error;
     my $w; $w = AnyEvent->io( fh => $self->{sock}, poll => 'r', cb => sub {
         undef $w;
         my $buf;
         do {
             if ($self->{sock}->eof) {
                 $error = "Connection closed unexpectedly without response";
                 return;
             }
             my $res; $self->{sock}->read($res, 4096); $buf .= $res;
         } while (!length($buf));
         my ($top, @headers) = split /\r?\n/, $buf;
         if ($top =~ m{HTTP/1.[01]\s+2\d\d}) {
             # Woo, 200!
             undef $error;
             my MogileFS $mg = $self->{mg};
             my $domain = $mg->{domain};

             my $fid   = $self->{fid};
             my $devid = $self->{devid};
             my $path  = $self->{path};

             my $create_close_args = $self->{create_close_args};

             my $key = $self->{key};

             my $rv = $mg->{backend}->do_request_async("create_close",
                {
                     %$create_close_args,
                     fid    => $fid,
                     devid  => $devid,
                     domain => $domain,
                     size   => $self->{content_length} ? $self->{content_length} : $self->{length},
                     key    => $key,
                     path   => $path,
                },
                sub { $self->{closed_cb}->(@_) },
                sub { $self->{error_cb}->(@_) },
            );
         }
         else {
             $error = "Got non-200 from remote server $top";
         }
     });

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
