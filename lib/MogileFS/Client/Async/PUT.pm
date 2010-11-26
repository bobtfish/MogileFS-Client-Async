package MogileFS::Client::Async::PUT;
use Moose;
use Method::Signatures::Simple;
use Errno qw(EIO EINVAL EPERM);
use namespace::autoclean;

has [qw/mg
            fid
            devid
            class
            key
            path
            length
            pos
            ua
            eof
            readonly
            readLineChunkSize
/] => (is => 'rw');

method BUILD {
    while ($self->{path}) {
        my $req;
        # overwrite needs changing to create if not exists?
        if ($args{overwrite}) {
            $req = HTTP::Request->new( PUT => $self->{path} ); # Ensure file overwritten/created, even if they don't print anything
        } else {
            $req = HTTP::Request->new( HEAD => $self->{path} );
        }

        my $res = $ua->request( $req );

        if ($res->is_success) {
            if ($args{overwrite}) {
                $self->{length} = 0;
            } else {
                $self->{length} = $res->header( 'Content-Length' ) || 0;
            }

            last;
        } else {
            my $dest = shift @{$args{backup_dests}};

            if ($dest) {
                $self->{devid} = $dest->[0];
                $self->{path}  = $dest->[1];
            } else {
                $self->{devid} = undef;
                $self->{path}  = undef;
            }
        }
    }

    return unless $self->{path};

    $self->{pos}      = 0;
    $self->{ua}       = $ua;
    $self->{eof}      = 0;

    $self->{mg}       = $args{mg};
    $self->{fid}      = $args{fid};
    $self->{key}      = $args{key};
    $self->{readonly} = $args{readonly} || 0;

    return $self;
}
__PACKAGE__->meta->add_method('TIEHANDLE' => __PACKAGE__->meta->fine_method_by_name('new'));

method READ ($bufish, $len, $offset) {
    my $buf = \$bufish;

    defined( $$buf ) or $$buf = '';
    defined( $offset ) or $offset = 0;

    if ($len == 0) {
        $$buf = '';
        return 0;
    }

    die "Negative len [$len] passed" if $len < 0;

    die "Negative offset [$offset] not supported" if $offset < 0;

    return 0 if ($self->EOF);

    my $start = $self->{pos};
    my $end   = $self->{pos} + $len - 1;

    my $req = HTTP::Request->new(GET => $self->{path}, [
        Range => "bytes=$start-$end",
    ], );

    my $res = $self->{ua}->request( $req );

    if ($res->is_error) {
        if ($res->code eq RC_REQUEST_RANGE_NOT_SATISFIABLE) {
            $self->{eof} = 1;
            return 0;
        }

        $! = EIO;
        return;
    }

    my $length = length( $res->content );

    $self->{pos} += $length;

    # Behaviour is not correct with offsets < length of existing buffer
    if ($offset) {
        $$buf = substr($$buf, 0, $offset) . $res->content;
    } else {
        $$buf = $res->content;
    }

    return $length;
}
*read = *READ;

method WRITE ($buf, $len, $offset) {
    if ($self->{readonly}) {
        $! = EPERM;
        return;
    }

    if (defined $len || defined $offset) {
        $offset = 0 if ! defined $offset;

        $buf = substr($buf, $offset, $len);
    }

    $len = length($buf);

    my $start = $self->{pos};
    my $end   = $self->{pos} + $len - 1;

    my $req = HTTP::Request->new(PUT => $self->{path}, [
        'Content-Range' => "bytes $start-$end/*",
    ], );

    $req->add_content($buf);

    my $res = $self->{ua}->request( $req );

    if ($res->is_error) {
        $! = EIO;
        return;
    }

    if ($self->{pos} + $len > $self->{length}) {
        $self->{length} = $self->{pos} + $len;
    }

    $self->{pos} += $len;

    $self->{eof} = ($self->{pos} == $self->{length} ? 1 :0);

    return $len;
}
*write = *WRITE;

method EOF {
    return 1 if $self->{eof};

    return unless $self->{length};

    return $self->{pos} >= $self->{length};
}
*eof = *EOF;

method TELL {
    return $self->{pos};
}
*tell = *TELL;

method SEEK ($offset, $whence) {
    if ($whence == 1) {
        $offset += $self->{pos};
    } elsif ($whence == 2) {
        $offset += $self->{length};
    }

    if ($offset > $self->{length}) {
        $! = EINVAL;
        return 0;
    }

    $self->{pos} = $offset;
    $self->{eof} = ($self->{pos} == $self->{length} ? 1 :0);

    return 1;
}
*seek = *SEEK;

method GETC {
    $self->READ( my $buf, 1 );

    return $buf;
}
*getc = *GETC;

method PRINT {
    my $buf = join(defined $, ? $, : "", @_);

    $buf .= $\ if defined $\;

    $self->WRITE($buf, length($buf), 0);
}
*print = *PRINT;

method PRINTF (@args){
    my $buf = sprintf(@args);

    $self->WRITE($buf,length($buf),0);
}
*printf = *PRINTF;

method CLOSE {
    if ($self->{devid}) {
       my $mg = $self->{mg};

        my $rv = $mg->{backend}->do_request
            ("create_close", {
                fid    => $self->{fid},
                devid  => $self->{devid},
                domain => $mg->{domain},
                size   => $self->{length},
                key    => $self->{key},
                path   => $self->{path},
            });

        unless ($rv) {
            $@ = "$mg->{backend}->{lasterr}: $mg->{backend}->{lasterrstr}";
            return undef;
        }
    }

    return 1;
}
*close = *CLOSE;

method BINMODE {
    return 1;
}
*binmode = *BINMODE;

method FILENO {
    # Wanted by perl debugger
    return -1;
}
*fileno = *FILENO;

# Must return undef (not just '') on EOF
method READLINE {
    my $retBuff;
    my $startPos = $self->{pos};
    my $foundEol;
READ:
    while (!$self->EOF) {
        my $readBuff;
        my $rc = $self->read($readBuff, $self->{readLineChunkSize});
        # Undef $/ => we will only exit on EOF (which should be right)
        $foundEol = index($readBuff, $/) if defined $/;
        if (defined($foundEol) && $foundEol >= 0) {
            $foundEol += length($/);
            $retBuff ||= '';
            $retBuff .= substr($readBuff, 0, $foundEol);
            # We have over-read, so go back
            $self->seek($startPos + length($retBuff) , 0);
            last READ;
        }
        else {
            # Go round again
            $retBuff .= $readBuff;
        }
    }
    return $retBuff;
}
*readline = *READLINE;

method path {
    return $self->{path};
}

__PACKAGE__->meta->make_immutable;
1;
