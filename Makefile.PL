use strict;
use warnings;
use inc::Module::Install 0.99;

all_from 'lib/MogileFS/Client/AnyEvent.pm';

requires 'MogileFS::Client';
requires 'AnyEvent';
requires 'AnyEvent::HTTP';

test_requires 'Test::More' => 0.88;

WriteAll;

