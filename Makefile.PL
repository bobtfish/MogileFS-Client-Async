use strict;
use warnings;
use inc::Module::Install 0.92;

all_from 'lib/MogileFS/Client/Async.pm';

requires 'MogileFS::Client';
requires 'AnyEvent';
requires 'AnyEvent::HTTP';
requires 'namespace::clean';

# Perhaps these should be merely recommended, they are only needed by
# Callback.pm.
requires 'IO::Socket::INET';
requires 'Try::Tiny';
requires 'File::Slurp';

recommends 'IO::AIO' => 3.25;

test_requires 'Test::More' => 0.88;

repository 'git://github.com/davel/MogileFS-Client-Async.git';

WriteAll;

