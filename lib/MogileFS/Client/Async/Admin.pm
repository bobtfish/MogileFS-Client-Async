package MogileFS::Client::Async::Admin;
use strict;
use warnings;
use MogileFS::Client::Async::Backend;

use base qw/ MogileFS::Admin /;

sub _backend_class_name { 'MogileFS::Client::Async::Backend' }

1;
