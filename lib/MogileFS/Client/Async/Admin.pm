package MogileFS::Client::Async::Admin;
use strict;
use warnings;

use base qw/ MogileFS::Client::Backend /;

sub new {
    my $self = shift;
    $self = fields::new($self) unless ref $self;

    my %args = @_;

    $self->{readonly} = $args{readonly} ? 1 : 0;
    my %backend_args = (  hosts => $args{hosts} );
    $backend_args{timeout} = $args{timeout} if $args{timeout};
    $self->{backend} = MogileFS::Async::Tracker->new( %backend_args )
        or _fail("couldn't instantiate MogileFS::Async::Tracker");

    return $self;
}

1;
