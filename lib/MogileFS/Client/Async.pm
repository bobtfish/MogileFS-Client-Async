package MogileFS::Client::Async;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::HTTP;
use AnyEvent::Socket;
use URI;
use Carp qw/confess/;
use POSIX qw( EAGAIN );

use base qw/ MogileFS::Client /;

BEGIN {
    eval "use IO::AIO;";
}

our $VERSION = '0.007';

sub new_file { confess("new_file is unsupported in " . __PACKAGE__) }
sub edit_file { confess("edit_file is unsupported in " . __PACKAGE__) }
sub read_file { confess("read_file is unsupported in " . __PACKAGE__) }

sub read_to_file {
    my $self = shift;
    my $key = shift;
    my $fn = shift;

    my @paths = $self->get_paths($key);

    die("No paths for $key") unless @paths;

    for (1..2) {
        foreach my $path (@paths) {
            my ($bytes, $write) = (0, undef);
            open $write, '>', $fn or confess("Could not open $fn to write");

            my $cv = AnyEvent->condvar;
            my $h;
            my $guard = http_request
                GET => $path,
                timeout => 120, # 2m
                on_header => sub {
                    my ($headers) = @_;
                    return 0 if ($headers->{Status} != 200);
                    $h = $headers;
                    1;
                },
                on_body => sub {
                    syswrite $write, $_[0] or return 0;
                    $bytes += length($_[0]);
                    1;
                },
                sub { # On complete!
                    my (undef, $headers) = @_;
                    $h = $headers;
                    close($write);
                    undef $write;
                    $cv->send;
                    1;
                };
            $cv->recv;
            return $bytes if ($bytes && !$write);
            # Error..
            $h->{Code} = 590;
            $h->{Reason} = "Unknown error";
            warn("HTTP error getting mogile $key: " . $h->{Reason} . "\n");
            close $write;
            unlink $fn;
        }
    }
    confess("Could not read $key from mogile");
}

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
        my $timeout = AnyEvent->timer( after => 10, cb => sub { undef $socket_guard; $cv->send; } );
        $socket_guard = tcp_connect $uri->host, $uri->port, sub {
            my ($fh, $host, $port) = @_;
            $error = $!;
            undef $timeout; # Note that removing the guard clears $!
            if (!$fh) {
                $cv->send;
                return;
            }
            $socket_fh = $fh;
            $cv->send;
        };
        $cv->recv;
        if (! $socket_fh) {
            $error ||= 'unknown error';
            warn("Connection error: $error to $path");
            next;
        }
        undef $error;
        # We are connected!
        open my $fh_from, "<", $file or confess("Could not open $file");
        $length = -s $file;
        my $buf = 'PUT ' . $uri->path . " HTTP/1.0\r\nConnection: close\r\nContent-Length: $length\r\n\r\n";
        $cv = AnyEvent->condvar;
        my $w;
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
                my $res; $socket_fh->read($res, 4096); $buff .= $res;
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

sub get_file_data {
    # given a key, load some paths and get data
    my MogileFS::Client $self = $_[0];
    my ($key, $timeout) = ($_[1], $_[2]);

    my @paths = $self->get_paths($key, 1);
    return undef unless @paths;

    # iterate over each
    foreach my $path (@paths) {
        next unless defined $path;
        if ($path =~ m!^http://!) {
            # try via HTTP
            my $ua = new LWP::UserAgent;
            $ua->timeout($timeout || 10);

            my $res = $ua->get($path);
            if ($res->is_success) {
                my $contents = $res->content;
                return \$contents;
            }

        } else {
            # open the file from disk and just grab it all
            open FILE, "<$path" or next;
            my $contents;
            { local $/ = undef; $contents = <FILE>; }
            close FILE;
            return \$contents if $contents;
        }
    }
    return undef;
}

1;

