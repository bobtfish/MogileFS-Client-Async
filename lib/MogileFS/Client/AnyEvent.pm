package MogileFS::Client::AnyEvent;
use strict;
use warnings;
use AnyEvent;
use AnyEvent::HTTP;
use Carp qw/confess/;

use base qw/ MogileFS::Client /;

sub new_file { confess("new_file is unsupported in " . __PACKAGE__) }
sub edit_file { confess("edit_file is unsupported in " . __PACKAGE__) }
sub read_to_file { confess("read_file is unsupported in " . __PACKAGE__) }

sub read_to_file {
    my $self = shift;
    my $key = shift;
    my $fn = shift;

    my @paths = $self->get_paths($key);

    for (1..2) {
        foreach my $path (@paths) {
            my ($bytes, $write) = (0, undef);
            open my $write, '>', $fn or confess("Could not open $fn to write");

            my $cv = AnyEvent->condvar;
            my $h;
            my $guard = http_request
                GET => $path,
                timeout => 120, # 2m
                on_header => sub {
                    my ($headers) = @_;
                    return 0 if ($headers->{Status} != 200);
                    warn Dumper $_[0];
                    $h = $headers;
                    1;
                }
                on_body => sub {
                    print $write, $_[0] or return 0;
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

    my $main_dest = shift @$dests;
    my ($main_devid, $main_path) = ($main_dest->[0], $main_dest->[1]);

    $self->run_hook('new_file_end', $self, $key, $class, $opts);

    my $fh = $self->new_file($key, $class, undef, $opts) or return;
    open my $fh_from, $file or confess("Could not open $file");

    my $length = -s $file;

    # FIXME
    while (my $len = read $fh_from, my($chunk), $chunk_size) {
        $fh->print($chunk);
        $bytes += $len;
    }

    $self->run_hook('store_file_end', $self, $key, $class, $opts);

    close $fh_from;
    $fh->close or return;
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


