package GRNOC::TSDS::Aggregate;

use strict;
use warnings;

use Moo;
use Types::Standard qw( Str Bool );

use GRNOC::Log;
use GRNOC::Config;

use Data::Dumper;

### required attributes ###

has config_file => ( is => 'ro',
                     isa => Str,
                     required => 1 );


### optional attributes ###

has daemonize => ( is => 'ro',
                   isa => Bool,
                   default => 1 );

### private attributes ###

has config => ( is => 'rwp' );


sub BUILD {

    my ( $self ) = @_;

    # create and store config object
    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 0 );

    $self->_set_config( $config );   

    # setup signal handlers
    $SIG{'TERM'} = sub {
        log_info( 'Received SIG TERM.' );
        $self->stop();
    };

    $SIG{'HUP'} = sub {
        log_info( 'Received SIG HUP.' );
    };


    return $self;
}

sub start {
    die "Must be implemented by subclass";
}

sub stop(){
    my ( $self ) = @_;

    log_info( 'Stopping.' );

    exit(0);
}

1;
