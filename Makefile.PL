use strict;
use warnings;
use inc::Module::Install 0.99;

all_from 'lib/MogileFS/Client/Async.pm';

requires 'MogileFS::Client';
requires 'AnyEvent';
requires 'AnyEvent::HTTP';
requires 'namespace::clean';

recommends 'IO::AIO' => 3.25;

test_requires 'Test::More' => 0.88;

WriteAll;

