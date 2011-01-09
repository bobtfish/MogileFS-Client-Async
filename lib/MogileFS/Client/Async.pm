package MogileFS::Client::Async;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::HTTP;
use AnyEvent::Socket;
use URI;
use MogileFS::Client::Async::Backend;
use MogileFS::Client::Async::HTTPFile;
use Carp qw/confess/;
use POSIX qw( EAGAIN );
use Try::Tiny qw/ try catch /;
use IO::WrapTie ();
use base qw/ MogileFS::Client /;

our $VERSION = '0.010';

BEGIN {
    my $AIO = try {require IO::AIO; 1};
    foreach my $sym (qw/ fadvise FADV_SEQUENTIAL /) {
        no strict 'refs';
        *{$sym} = $AIO ? \&{"IO::AIO::$sym"} : sub {};
    }
}

use namespace::clean;

sub _backend_class_name { 'MogileFS::Client::Async::Backend' }

sub new_file {
    my ($self, $key, $class, $bytes, $opts) = @_;
    my $cv_end = AnyEvent->condvar;
    my $fh_cv = AnyEvent->condvar;
    $self->new_file_async($key, $class, $bytes, $opts,
        sub { $fh_cv->send(@_) },
        sub { $cv_end->send(1) }, # Closed cb - i.e. after ->CLOSE is called, and all done with tracker
        sub { $cv_end->recv }, # Closing cb - ->CLOSE has just been called and is about to return
                               # this determines the return of the CLOSE method
                               # Used here in the sync interface to make close($fh) block as expected
                               # till everything is done..
        sub { $self->{backend}->{lasterr} = shift; $fh_cv->send(undef); },
    );
    $fh_cv->recv;
}

sub new_file_async {
    my $self = shift;
    return undef if $self->{readonly};

    my ($key, $class, $bytes, $opts, $fh_cb, $closed_cb, $closing_cb, $on_error) = @_;
    $bytes += 0;
    $opts ||= {};
    die("No fh_cb") unless $fh_cb; # \&_default_callback
    die("No closed_cb") unless $closed_cb;
    $closing_cb ||= sub { 1 }; # By default close($fh) returns true, even though fh not fully closed

    # Extra args to be passed along with the create_open and create_close commands.
    # Any internally generated args of the same name will overwrite supplied ones in
    # these hashes.
    my $create_open_args =  $opts->{create_open_args} || {};
    my $create_close_args = $opts->{create_close_args} || {};

    $self->run_hook('new_file_start', $self, $key, $class, $opts);

    $self->{backend}->do_request_async("create_open",
        {  # Args
            %$create_open_args,
            domain => $self->{domain},
            class  => $class,
            key    => $key,
            fid    => $opts->{fid} || 0, # fid should be specified, or pass 0 meaning to auto-generate one
            multi_dest => 1,
        },
        sub { # on_success
            my $res = shift;
            my $dests = [];  # [ [devid,path], [devid,path], ... ]

            for my $i (1..$res->{dev_count}) {
                push @$dests, [ $res->{"devid_$i"}, $res->{"path_$i"} ];
            }

            my $main_dest = shift @$dests;
            my ($main_devid, $main_path) = ($main_dest->[0], $main_dest->[1]);

            $self->run_hook('new_file_end', $self, $key, $class, $opts);

            $fh_cb->(IO::WrapTie::wraptie(
                'MogileFS::Client::Async::HTTPFile',
                    mg    => $self,
                    fid   => $res->{fid},
                    path  => $main_path,
                    devid => $main_devid,
                    backup_dests => $dests,
                    class => $class,
                    key   => $key,
                    content_length => $bytes+0,
                    create_close_args => $create_close_args,
                    overwrite => 1,
                    closing_cb => $closing_cb,
                    closed_cb => $closed_cb,
            ));
        },
        $on_error,
    );
}

sub edit_file { confess("edit_file is unsupported in " . __PACKAGE__) }

sub read_file { # FIXME - This is insane
    my ($self, $key) = @_;
    my $timeout ||= 10;
    my @paths = $self->get_paths($key, 1);
    return undef unless @paths;
    my $cv ||= AnyEvent->condvar;
    my $timer = AnyEvent->timer( after => $timeout, cb => sub { $cv->send(undef) });
    my $cb ||= \&_default_callback;
    my (undef, $filename) = tempfile();
    $self->_read_http_to_file_async([@paths], $filename, $cb, $cv)->recv;
     open my $fh, '<', $filename or die;;
    return $fh;
}

sub store_file {
    my $self = shift;
    return undef if $self->{readonly};

    my ($key, $class, $file, $opts) = @_;
    $opts ||= {};

    $self->run_hook('store_file_start', $self, $key, $class, $opts);

    my $length = -s $file;
    open my $fh_from, "<", $file or confess("Could not open $file");

    my $fh = $self->new_file($key, $class, $length, $opts);

    # Hint to Linux that doubling readahead will probably pay off.
    fadvise($fh_from, 0, 0, FADV_SEQUENTIAL());

    my $buf;
    while (sysread($fh_from, $buf, 4096)) {
        $fh->print($buf);
    }
    $fh->close;

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

sub get_paths {
    my ($self, $key, $opts) = @_;
    my $cv = AnyEvent->condvar;
    $self->get_paths_async($key, $opts, sub { $cv->send(@_) }, sub { $cv->throw(@_) });
    $cv->recv;
}

sub get_paths_async {
    my ($self, $key, $opts, $on_success, $on_error) = @_;

    # handle parameters, if any
    my ($noverify, $zone);
    unless (ref $opts) {
        $opts = { noverify => $opts };
    }
    my %extra_args;

    $noverify = 1 if $opts->{noverify};
    $zone = $opts->{zone};

    my $my_on_success = sub {
        my ($res) = @_;
        my @paths = map { $res->{"path$_"} } (1..$res->{paths});

        $self->run_hook('get_paths_end', $self, $key, $opts);
        $on_success->(@paths);
    };

    if (my $pathcount = delete $opts->{pathcount}) {
        $extra_args{pathcount} = $pathcount;
    }

    $self->run_hook('get_paths_start', $self, $key, $opts);

    $self->{backend}->do_request_async("get_paths", {
            domain => $self->{domain},
            key    => $key,
            noverify => $noverify ? 1 : 0,
            zone   => $zone,
	        %extra_args,
        },
        $my_on_success,
        $on_error,
    );
}

sub read_to_file {
    my ($self, $key, $fn, $opts, $cb, $cv) = @_;
    $self->read_to_file_async($key, $fn, $opts, $cb, $cv)->recv;
}

sub read_to_file_async {
    my ($self, $key, $fn, $opts, $on_success, $on_error) = @_;

    $opts ||= {};
    $self->get_paths_async($key, $opts,
        sub {
            my ($cv, @paths) = @_;
            unless (@paths) {
                $on_error->("No paths for $key");
                return;
            }
            $self->_read_http_to_file_async([ @paths ], $fn, $on_success, $on_error);
        },
        $on_error,
    );
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
    my ($self, $key) = @_;
    my $cv = AnyEvent->condvar;
    $self->delete_async($key, sub { $cv->send(@_) }, sub { $cv->send(@_ ) });
    $cv->recv;
}

sub delete_async {
    my ($self, $key, $on_success, $on_error) = @_;
    if ($self->{readonly}) {
        $on_error->(undef);
        return undef;
    }
    $self->{backend}->do_request_async(
        "delete",
        {
            domain => $self->{domain},
            key    => $key,
        },
        $on_success,
        $on_error,
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
        my $path = shift(@possible_paths);

        unless ($path) {
            $cv->croak("Could not read from mogile (no working paths)");
            $cb->($cv);
        }

        my ($bytes) = (0, undef);

        my $h;
        open my $write, '>', $fn or confess("Could not open $fn to write");
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

