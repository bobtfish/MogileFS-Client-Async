use strict;
use warnings;
use Test::More;
use Test::Exception;
use MogileFS::Client::Callback;
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

my $mogc = MogileFS::Client::Callback->new(
    domain => "state51",
    hosts => [qw/
        tracker0.cissme.com:7001
        tracker1.cissme.com:7001
        tracker2.cissme.com:7001
    /],
);
ok $mogc, 'Have client';

my $key = 'test-t0m-foobar';

my $exp_len = -s $0;
my $callback = $mogc->store_file_from_callback($key, 'rip', $exp_len, {
    on_failure => sub { ok 0, "upload failed for some reason!"; diag $_[0]; },
    on_success => sub {
        my ($args) = @_;
        diag Dumper($args);
        ok length($args->{url}) > 1;
        is($args->{total_bytes}, $exp_len);
        is($args->{client}, 'callback');
        ok $args->{time_elapsed} > 0;
        ok $args->{time_elapsed} < 60;
    },
});
isa_ok($callback, "CODE");

open(my $fh, "<", $0) or die $!;
my $line = <$fh>;
my $rest = join("", <$fh>);

ok length($line);
ok length($rest);

$callback->(\$line, 0);
$callback->($rest, 0);
close $fh;
$callback->("", 1);

lives_ok {
    my ($fh, $fn) = tempfile;
    $mogc->read_to_file($key, $fn);
    is( -s $fn, $exp_len, 'Read file back with correct length' )
        or system("diff -u $0 $fn");
    is sha1($fn), $exp_sha, 'Read file back with correct SHA1';
    unlink $fn;
};

done_testing;

