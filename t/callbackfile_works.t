use strict;
use warnings;
use Test::More;
use Test::Exception;
use MogileFS::Client::CallbackFile;
use Digest::SHA1;
use File::Temp qw/ tempfile /;
use Data::Dumper;

sub sha1 {
    open(FH, '<', shift) or die;
    my $sha = Digest::SHA1->new;
    $sha->addfile(*FH);
    close(FH);
    $sha->hexdigest;
}

my $exp_sha = sha1($0);

my $mogc = MogileFS::Client::CallbackFile->new(
    domain => "state51",
    hosts => [qw/
        tracker0.cissme.com:7001
        tracker1.cissme.com:7001
        tracker2.cissme.com:7001
    /],
);
ok $mogc, 'Have client';

my $key = 'test-t0m-foobar';



open(my $read_fh, "<", $0) or die "failed to open $0: $!";

isa_ok($read_fh, 'GLOB');

my $exp_len = -s $read_fh;
my $callback = $mogc->store_file_from_fh($key, 'rip', $read_fh, $exp_len, {});

isa_ok($callback, 'CODE');

$callback->(0, 0);
$callback->(1, 0);
$callback->(2, 0);
$callback->($exp_len, 0);
$callback->($exp_len, 1);

lives_ok {
    my ($fh, $fn) = tempfile;
    $mogc->read_to_file($key, $fn);
    is( -s $fn, $exp_len, 'Read file back with correct length' )
        or system("diff -u $0 $fn");
    is sha1($fn), $exp_sha, 'Read file back with correct SHA1';
    unlink $fn;
};

done_testing;

