use strict;
use warnings;
use Test::More;
use Test::Exception;
use MogileFS::Client::Async;
use Digest::SHA1;
use File::Temp qw/ tempfile /;
use Test::Fatal;

sub sha1 {
    open(FH, '<', shift) or die;
    my $sha = Digest::SHA1->new;
    $sha->addfile(*FH);
    close(FH);
    $sha->hexdigest;
}

my $exp_sha = sha1($0);

my $mogc = MogileFS::Client::Async->new(
    domain => "test",
    hosts => [qw/
        localhost:7001
    /],
);
ok $mogc, 'Have client';

my $key = 'test-t0m-foobar';

eval { $mogc->delete($key); };

my $exp_len = -s $0;
is exception {
    is $mogc->store_file($key, 'test', $0), $exp_len,
        'Stored file of expected length';
}, undef, 'lives ok';

my $cv = $mogc->get_paths_async($key);
is exception {
    my @paths = $cv->recv;
    ok scalar(@paths), 'Have some paths';
}, undef, 'Lived ok';

is exception {
    my ($fh, $fn) = tempfile;
    $mogc->read_to_file($key, $fn);
    is( -s $fn, $exp_len, 'Read file back with correct length' )
        or system("diff -u $0 $fn");
    is sha1($fn), $exp_sha, 'Read file back with correct SHA1';
    unlink $fn;
}, undef;

my $contents = do { local $/; open my $fh, '<', $0 or die; <$fh> };
my $contents_from_mogile = ${ $mogc->get_file_data($key) };

is $contents_from_mogile, $contents;

is exception { ok $mogc->delete($key), 'deleted'; }, undef, 'Delete no exception';
is exception { ok !$mogc->delete($key), 'Nothing to delete'; }, undef,
    'No exception on delete again';

{
    my $key = 'test-t0m-foobaz';
    my $fh;
    is exception {
        ok $fh = $mogc->new_file($key, 'test');
    }, undef;
    $fh->print($contents);
    $fh->close;

    my $cv = $mogc->get_paths_async($key);
    is exception {
        my @paths = $cv->recv;
        ok scalar(@paths), 'Have some paths';
    }, undef, 'get paths lived ok';
}

{
    my $key = 'test-t0m-fooquux';
    my $size = length($contents);
    my $fh;
    is exception {
        ok $fh = $mogc->new_file($key, 'test', $size);
    }, undef;

#    my $cv = $mogc->get_paths_async($key);
#    is exception {
#        my @paths = $cv->recv;
#        ok scalar(@paths), 'Have some paths';
#    }, undef, 'get paths lived ok';
}

done_testing;

