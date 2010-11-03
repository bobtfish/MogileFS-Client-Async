package MogileFS::Client::Stupid;
use strict;
use warnings;
use URI;
use Carp qw/confess/;
use Try::Tiny;
use IO::AIO;

use base qw/ MogileFS::Client::Async /;

use namespace::clean;

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
    my $worked = 0;
    foreach my $dest (@dests) {
        $try++;
        ($devid, $path) = @$dest;
        my $uri = URI->new($path);

        try {
            # Allow Linux to get some readahead while we fiddle around with sockets.
            open my $fh_from, "<", $file or confess("Could not open $file");
            IO::AIO::fadvise($fh_from, 0, 0, IO::AIO::FADV_SEQUENTIAL());

            $length = -s $file;
            my $buf = 'PUT ' . $uri->path . " HTTP/1.0\r\nConnection: close\r\nContent-Length: $length\r\n\r\n";

            my $socket = IO::Socket::INET->new(
                PeerAddr => $uri->host,
                PeerPort => $uri->port,
                Proto => 'tcp',
                Timeout => 30,
            ) or die $!;

            my $c = syswrite($socket, $buf);
            if ($c != length($buf)) {
                die "syswrite failed, attempted ".length($buf)." only wrote $c: $!";
            }

            $c = IO::AIO::sendfile($socket, $fh_from, 0, $length);
            if ($c != $length) {
                die "sendfile failed, attempted $length only wrote $c: $!";
            }
            close $fh_from;

            my $buf = "";
            # slurp in what comes back
            do {
                my $res;
                sysread($socket, $res, 4096);
                $buf .= $res;
            } while (!$socket->eof);
            my ($top, @headers) = split /\r?\n/, $buf;
            if ($top =~ m{HTTP/1.[01]\s+2\d\d}) {
                $worked = 1;
            }
            else {
                die "Got non-200 from remote server $top";
            }
            close $socket;
        }
        catch {
            warn "write failed: $_";
        };
    }
     
    die("Could not write to any mogile hosts, should have tried " . scalar(@$dests) . " did try $try")
        unless $worked;

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

1;

