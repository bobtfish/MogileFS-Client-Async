package MogileFS::Client::Async;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::HTTP;
use AnyEvent::Socket;
use URI;
use MogileFS::Client::Async::Tracker;
use Carp qw/confess/;
use POSIX qw( EAGAIN );
use Try::Tiny qw/ try catch /;
use base qw/ MogileFS::Client /;

our $VERSION = '0.010';

BEGIN {
    my @steal_symbols = qw/ fadvise FADV_SEQUENTIAL /;
    if (eval {require IO::AIO; 1}) {
        foreach my $sym (@steal_symbols) {
            no strict 'refs';
            *{$sym} = \&{"IO::AIO::$sym"};
        }
    }
    else {
        foreach my $sym (@steal_symbols) {
            *{$sym} = sub {};
        }
    }
}

use namespace::clean;

sub _default_callback { shift->send(@_) }

sub _init {
    my $self = shift;
    my %args = @_;
    $self->{backend} = MogileFS::Client::Async::Tracker->new(
        hosts => $args{hosts},
        timeout => $args{timeout},
    );
    $self->SUPER::_init(@_);
}

sub new_file { confess("new_file is unsupported in " . __PACKAGE__) }
sub edit_file { confess("edit_file is unsupported in " . __PACKAGE__) }
sub read_file { confess("read_file is unsupported in " . __PACKAGE__) }

sub store_file {
    my $self = shift;
    return undef if $self->{readonly};

    my ($key, $class, $file, $opts) = @_;
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
    use Data::Dumper;
    warn Dumper($res);

    my $dests = [];  # [ [devid,path], [devid,path], ... ]

    # determine old vs. new format to populate destinations
    unless (exists $res->{dev_count}) {
        push @$dests, [ $res->{devid}, $res->{path} ];
    } else {
        for my $i (1..$res->{dev_count}) {
            push @$dests, [ $res->{"devid_$i"}, $res->{"path_$i"} ];
        }
    }

    my ($length, $error, $devid, $path);
    my @dests = (@$dests, @$dests, @$dests); # 2 retries
    my $try = 0;
    foreach my $dest (@dests) {
        $try++;
        ($devid, $path) = @$dest;
        my $uri = URI->new($path);
        my $cv = AnyEvent->condvar;
        my ($socket_guard, $socket_fh);
        $socket_guard = tcp_connect $uri->host, $uri->port, sub {
            my ($fh, $host, $port) = @_;
            $error = $!;
            if (!$fh) {
                $cv->send;
                return;
            }
            $socket_fh = $fh;
            $cv->send;
        }, sub { 10 };
        $cv->recv;
        if (! $socket_fh) {
            $error ||= 'unknown error';
            warn("Connection error: $error to $path");
            next;
        }
        undef $error;
        # We are connected!
        open my $fh_from, "<", $file or confess("Could not open $file");

        # Hint to Linux that doubling readahead will probably pay off.
        fadvise($fh_from, 0, 0, FADV_SEQUENTIAL());

        $length = -s $file;
        my $buf = 'PUT ' . $uri->path . " HTTP/1.0\r\nConnection: close\r\nContent-Length: $length\r\n\r\n";
        $cv = AnyEvent->condvar;
        my $w;
        my $timeout;
        my $reset_timer = sub {
            my ($type, $time) = @_;
            $type ||= 'unknown';
            $time ||= 60;
            my $start = time();
            $timeout = AnyEvent->timer(
                after => $time,
                cb => sub {
                    undef $w;
                    my $took = time() - $start;
                    $error = "Connection timed out duing data transfer of type $type (after $took seconds)";
                    $cv->send;
                },
            );
        };
        $w = AnyEvent->io( fh => $socket_fh, poll => 'w', cb => sub {
            $reset_timer->('read');
            if (!length($buf)) {
                my $bytes = sysread $fh_from, $buf, '4096';
                $reset_timer->('write');
                if (!defined $bytes) { # Error, read FH blocking, no need to check EAGAIN
                    $error = $!;
                    $cv->send;
                    return;
                }
                if (0 == $bytes) { # EOF reading, and we already wrote everything
                    $cv->send;
                    return;
                }
            }
            my $len = syswrite $socket_fh, $buf;
            $reset_timer->('loop');
            if ($len && $len > 0) {
                $buf = substr $buf, $len;
            }
            if (!defined $len && $! != EAGAIN) { # Error, we could get EAGAIN as write sock non-blocking
                $error = $!;
                $cv->send;
                return;
            }
        });
        $reset_timer->('start PUT');
        $cv->recv;
        $cv = AnyEvent->condvar;
        # FIXME - Cheat here, the response should be small, so we assume it'll allways all be
        #         readable at once, THIS MAY NOT BE TRUE!!!
        $w = AnyEvent->io( fh => $socket_fh, poll => 'r', cb => sub {
            undef $timeout;
            undef $w;
            $cv->send;
            my $buf;
            do {
                if ($socket_fh->eof) {
                    $error = "Connection closed unexpectedly without response";
                    return;
                }
                my $res; $socket_fh->read($res, 4096); $buf .= $res;
            } while (!length($buf));
            my ($top, @headers) = split /\r?\n/, $buf;
            if ($top =~ m{HTTP/1.[01]\s+2\d\d}) {
                # Woo, 200!
                undef $error;
            }
            else {
                $error = "Got non-200 from remote server $top";
            }
        });
        $reset_timer->('response', 1200); # Wait up to 20m, as lighty
                                          # may have to copy the file between
                                          # disks. EWWWW
        $cv->recv;
        undef $timeout;
        if ($error) {
            warn("Error sending data (try $try) to $uri: $error");
            next; # Retry
        }
        last; # Success
    }
    die("Could not write to any mogile hosts, should have tried " . scalar(@$dests) . " did try $try")
        if $error;

    $self->run_hook('new_file_end', $self, $key, $class, $opts);

    my $rv = $self->{backend}->do_request
            ("create_close", {
                fid    => $res->{fid},
                devid  => $devid,
                domain => $self->{domain},
                size   => $length,
                key    => $key,
                path   => $path,
            });

    unless ($rv) {
        die "$self->{backend}->{lasterr}: $self->{backend}->{lasterrstr}";
        return undef;
    }

    $self->run_hook('store_file_end', $self, $key, $class, $opts);

    return $length;
}

sub store_content {
    die("BORK");
    my MogileFS::Client $self = shift;
    return undef if $self->{readonly};

    my($key, $class, $content, $opts) = @_;

    $self->run_hook('store_content_start', $self, $key, $class, $opts);

    my $fh = $self->new_file($key, $class, undef, $opts) or return;
    $content = ref($content) eq 'SCALAR' ? $$content : $content;
    $fh->print($content);

    $self->run_hook('store_content_end', $self, $key, $class, $opts);

    $fh->close or return;
    length($content);
}

sub get_paths {
    my ($self, $key, $opts, $cb, $cv) = @_;
    $cv = $self->get_paths_async($key, $opts, $cb, $cv);
    $cv->recv;
}

sub get_paths_async {
    my ($self, $key, $opts, $cb, $cv) = @_;

    # handle parameters, if any
    my ($noverify, $zone);
    unless (ref $opts) {
        $opts = { noverify => $opts };
    }
    my %extra_args;

    $noverify = 1 if $opts->{noverify};
    $zone = $opts->{zone};

    $cv ||= AnyEvent->condvar;

    $cb ||= \&_default_callback;

    my $my_cb = sub {
        my ($cv, $res) = @_;
        my @paths = map { $res->{"path$_"} } (1..$res->{paths});

        $self->run_hook('get_paths_end', $self, $key, $opts);
        $cb->($cv, @paths);
    };

    if (my $pathcount = delete $opts->{pathcount}) {
        $extra_args{pathcount} = $pathcount;
    }

    $self->run_hook('get_paths_start', $self, $key, $opts);

    warn("Get_paths");
    $self->{backend}->do_request
        ("get_paths", {
            domain => $self->{domain},
            key    => $key,
            noverify => $noverify ? 1 : 0,
            zone   => $zone,
	    %extra_args,
        }, $my_cb, $cv) or return ();

    return $cv;
}

sub read_to_file {
    my ($self, $key, $fn, $opts, $cb, $cv) = @_;
    $self->read_to_file_async($key, $fn, $opts, $cb, $cv)->recv;
}

sub read_to_file_async {
    my ($self, $key, $fn, $opts, $cb, $cv) = @_;

    warn("Get paths");
    $opts ||= {};
    $cv ||= AnyEvent->condvar;
    $cb ||= \&_default_callback;

    $self->get_paths_async($key, $opts, sub {
        my ($cv, @paths) = @_;
        warn("In read_to_file_async cb for get_paths_async");
        unless (@paths) {
            $cv->croak("No paths for $key");
        }
        $self->_read_http_to_file_async([ @paths ], $fn, $cb, $cv);
    }, $cv);
}

# FIXME - Should be possible without a temp file..
use File::Temp qw/ tempfile /;
sub get_file_data {
    my ($self, $key, $timeout, $cb, $cv) = @_;
    $timeout ||= 10;
    my @paths = $self->get_paths($key, 1);
    return undef unless @paths;
    $cv ||= AnyEvent->condvar;
    my $timer = AnyEvent->timer( after => $timeout, cb => sub { $cv->send(undef) });
    $cb ||= \&_default_callback;
    my (undef, $filename) = tempfile();
    $self->_read_http_to_file_async([@paths], $filename, $cb, $cv)->recv;
    my $data = do { local $/; open my $fh, '<', $filename or die; <$fh> };
    unlink $filename;
    return \$data;
}

sub delete {
    my ($self, $key, $cb, $cv) = @_;
    my $res;
    try { $res = $self->delete_async($key, $cb, $cv)->recv }
    catch { die $_ unless $_ =~ '^unknown_key' }; # FIXME - exception should eq unknown_key
    return $res;
}

sub delete_async {
    my ($self, $key, $cb, $cv) = @_;
    $cv ||= AnyEvent->condvar;
    $cb ||= \&_default_callback;
    if ($self->{readonly}) {
        $cb->($cv, undef);
        return undef;
    }
    $self->{backend}->do_request_async(
        "delete", {
            domain => $self->{domain},
            key    => $key,
        }, $cb, $cv,
    );
}

#sub rename {}

#sub list_keys

#sub foreach_key

#sub update_class

sub _read_http_to_file_async {
    my ($self, $paths, $fn, $cb, $cv) = @_;

    Carp::confess("No paths") unless $fn;
    Carp::confess("No callback") unless $cb;
    my @possible_paths = @$paths;
    my $try; $try = sub {
        warn("HTTP Try");
        my $path = shift(@possible_paths);

        unless ($path) {
            $cv->croak("Could not read from mogile (no working paths)");
            $cb->($cv);
        }

        my ($bytes) = (0, undef);

        my $h;
        open my $write, '>', $fn or confess("Could not open $fn to write");
        warn("Starting http req $path");
        http_request
            GET => $path,
            timeout => 120, # 2m
            on_header => sub {
                my ($headers) = @_;
                warn("Have headers");
                return 0 if ($headers->{Status} != 200);
                $h = $headers;
                1;
            },
            on_body => sub {
                warn("Have body chunk");
                syswrite($write, $_[0]) or return 0;
                $bytes += length($_[0]);
                1;
            },
            sub { # On complete!
                my ($err, $headers) = @_;
                close $write;
                $err = 1 if !defined($err); # '' on ok, undef on fail
                if ($err) {
                    warn("HTTP error getting mogile $path: " . $headers->{Reason} . "\n");
                    unlink $fn;
                    return $try->();
                }
                $h = $headers;
                close($write);
                undef $write;
                warn("Got complete file, sending to callback");
                $cb->($cv, $bytes);
                1;
            };
    };
    $try->();
    return $cv;
}

1;

