use strict;
use warnings;
use Test::More;
use Test::Exception;
use MogileFS::Client::Async;
use Digest::SHA1;
use File::Temp qw/ tempfile /;

sub sha1 {
    open(FH, '<', shift) or die;
    my $sha = Digest::SHA1->new;
    $sha->addfile(*FH);
    close(FH);
    $sha->hexdigest;
}

my $exp_sha = sha1($0);

my $mogc = MogileFS::Client::Async->new(
    domain => "state51",
    hosts => [qw/
        tracker0.cissme.com:7001
        tracker1.cissme.com:7001
        tracker2.cissme.com:7001
    /],
);
ok $mogc, 'Have client';

my $key = 'test-t0m-foobar';

eval { $mogc->delete($key); };

my $exp_len = -s $0;
lives_ok {
    is $mogc->store_file($key, 'rip', $0), $exp_len,
        'Stored file of expected length';
} 'lives ok';

my $cv = $mogc->get_paths_async($key);
lives_ok {
    my @paths = $cv->recv;
    ok scalar(@paths), 'Have some paths';
} 'Lived ok';

lives_ok {
    my ($fh, $fn) = tempfile;
    my $cv = $mogc->read_to_file_async($key, $fn);
    $cv->recv;
    is( -s $fn, $exp_len, 'Read file back with correct length' )
        or system("diff -u $0 $fn");
    is sha1($fn), $exp_sha, 'Read file back with correct SHA1';
    unlink $fn;
};

my $contents = do { local $/; open my $fh, '<', $0 or die; <$fh> };
my $contents_from_mogile = ${ $mogc->get_file_data($key) };

is $contents_from_mogile, $contents;

done_testing;

