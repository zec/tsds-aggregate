package GRNOC::TSDS::Aggregate::Aggregator::Worker;

use Moo;

use GRNOC::WebService::Client;
use GRNOC::TSDS::Aggregate::Aggregator::Message;

use Net::AMQP::RabbitMQ;
use JSON::XS;
use Math::Round qw( nlowmult nhimult );
use Try::Tiny;

use Data::Dumper;

### constants ###

use constant QUEUE_PREFETCH_COUNT => 20;
use constant QUEUE_FETCH_TIMEOUT => 10 * 1000;
use constant RECONNECT_TIMEOUT => 10;
use constant PENDING_QUEUE_CHANNEL => 1;
use constant FINISHED_QUEUE_CHANNEL => 2;
use constant SERVICE_CACHE_FILE => '/etc/grnoc/name-service-cacher/name-service.xml';
use constant COOKIES_FILE => '/var/lib/grnoc/tsds/aggregate/cookies.dat';

### required attributes ###

has config => ( is => 'ro',
                required => 1 );

has logger => ( is => 'ro',
                required => 1 );

### internal attributes ###

has is_running => ( is => 'rwp',
                    default => 0 );

has rabbit => ( is => 'rwp' );

has json => ( is => 'rwp' );

has websvc => ( is => 'rwp' );

### public methods ###

sub start {

    my ( $self ) = @_;

    $self->logger->debug( "Starting." );

    # flag that we're running
    $self->_set_is_running( 1 );

    # change our process name
    $0 = "tsds_aggregator [worker]";

    # setup signal handlers
    $SIG{'TERM'} = sub {

        $self->logger->info( "Received SIG TERM." );
        $self->stop();
    };

    $SIG{'HUP'} = sub {

        $self->logger->info( "Received SIG HUP." );
    };

    # create JSON object
    my $json = JSON::XS->new();

    $self->_set_json( $json );

    # create websvc object
    my $websvc = GRNOC::WebService::Client->new( uid => $self->config->get( '/config/tsds/username' ),
						 passwd => $self->config->get( '/config/tsds/password' ),
						 realm => $self->config->get( '/config/tsds/realm' ),
						 service_cache_file => SERVICE_CACHE_FILE,
						 cookieJar => COOKIES_FILE,
						 usePost => 1 );

    $websvc->set_service_identifier( 'urn:publicid:IDN+grnoc.iu.edu:' . $self->config->get( '/config/tsds/cloud' ) . ':TSDS:1:Query' );

    $self->_set_websvc( $websvc );

    # connect to rabbit queues
    $self->_rabbit_connect();

    # continually consume messages from rabbit queue, making sure we have to acknowledge them
    $self->logger->debug( 'Starting RabbitMQ consume loop.' );

    return $self->_consume_loop();
}

sub stop {

    my ( $self ) = @_;

    $self->logger->debug( 'Stopping.' );

    # this will cause the consume loop to exit
    $self->_set_is_running( 0 );
}

### private methods ###

sub _consume_loop {

    my ( $self ) = @_;

    while ( 1 ) {

        # have we been told to stop?
        if ( !$self->is_running ) {

            $self->logger->debug( 'Exiting consume loop.' );
            return 0;
        }

        # receive the next rabbit message
        my $rabbit_message;

        try {

            $rabbit_message = $self->rabbit->recv( QUEUE_FETCH_TIMEOUT );
        }

        catch {

            $self->logger->error( "Error receiving rabbit message: $_" );

            # reconnect to rabbit since we had a failure
            $self->_rabbit_connect();
        };

        # didn't get a message?
        if ( !$rabbit_message ) {

            $self->logger->debug( 'No message received.' );

            # re-enter loop to retrieve the next message
            next;
        }

        # try to JSON decode the messages
        my $messages;

        try {

            $messages = $self->json->decode( $rabbit_message->{'body'} );
        }

        catch {

            $self->logger->error( "Unable to JSON decode message: $_" );
        };

        if ( !$messages ) {

            try {

                # reject the message and do NOT requeue it since its malformed JSON
                $self->rabbit->reject( PENDING_QUEUE_CHANNEL, $rabbit_message->{'delivery_tag'}, 0 );
            }

            catch {

                $self->logger->error( "Unable to reject rabbit message: $_" );

                # reconnect to rabbit since we had a failure
                $self->_rabbit_connect();
            };
        }

        # retrieve the next message from rabbit if we couldn't decode this one
        next if ( !$messages );

        # make sure its an array (ref) of messages
        if ( ref( $messages ) ne 'ARRAY' ) {

            $self->logger->error( "Message body must be an array." );

            try {

                # reject the message and do NOT requeue since its not properly formed
                $self->rabbit->reject( PENDING_QUEUE_CHANNEL, $rabbit_message->{'delivery_tag'}, 0 );
            }

            catch {

                $self->logger->error( "Unable to reject rabbit message: $_" );

                # reconnect to rabbit since we had a failure
                $self->_rabbit_connect();
            };

            next;
        }

        my $num_messages = @$messages;
        $self->logger->debug( "Processing message containing $num_messages aggregations to perform." );

        my $t1 = time();

        my $success = $self->_consume_messages( $messages );

        my $t2 = time();
        my $delta = $t2 - $t1;

        $self->logger->debug( "Processed $num_messages updates in $delta seconds." );

        # didn't successfully consume the messages, so reject but requeue the entire message to try again
        if ( !$success ) {

            $self->logger->debug( "Rejecting rabbit message, requeueing." );

            try {

                $self->rabbit->reject( 1, $rabbit_message->{'delivery_tag'}, 1 );
            }

            catch {

                $self->logger->error( "Unable to reject rabbit message: $_" );

                # reconnect to rabbit since we had a failure
                $self->_rabbit_connect();
            };
        }

        # successfully consumed message, acknowledge it to rabbit
        else {

            $self->logger->debug( "Acknowledging successful message." );

            try {

                $self->rabbit->ack( 1, $rabbit_message->{'delivery_tag'} );
            }

            catch {

                $self->logger->error( "Unable to acknowledge rabbit message: $_" );

                # reconnect to rabbit since we had a failure
                $self->_rabbit_connect();
            };
        }
    }
}

sub _consume_messages {

    my ( $self, $messages ) = @_;

    # gather all messages to process
    my $aggregates_to_process = [];

    # handle every TSDS message that came within the rabbit message
    foreach my $message ( @$messages ) {

        # make sure message is an object/hash (ref)
        if ( ref( $message ) ne 'HASH' ) {

            $self->logger->error( "Messages must be an object/hash of data, skipping." );
            next;
        }

        my $type = $message->{'type'};
        my $interval_from = $message->{'interval_from'};
        my $interval_to = $message->{'interval_to'};
        my $start = $message->{'start'};
        my $end = $message->{'end'};
        my $meta = $message->{'meta'};
	my $values = $message->{'values'};
	my $required_meta = $message->{'required_meta'};

	my $aggregate_message;

	try {
	    
	    $aggregate_message = GRNOC::TSDS::Aggregate::Aggregator::Message->new( type => $type,
										   interval_from => $interval_from,
										   interval_to => $interval_to,
										   start => $start,
										   end => $end,
										   meta => $meta,
										   values => $values,
										   required_meta => $required_meta );
	}

	catch {

	    $self->logger->error( $_ );
	};

	# include this to our list of messages to process if it was valid
	push( @$aggregates_to_process, $aggregate_message ) if $aggregate_message;
    }

    # aggregate all of the data across all messages
    my $success = 1;

    try {

        $self->_aggregate_messages( $aggregates_to_process ) if ( @$aggregates_to_process > 0 );
    }

    catch {

        $self->logger->error( "Error aggregating messages: $_" );
        $success = 0;
    };
    
    return $success;
}

sub _aggregate_messages {

    my ( $self, $messages ) = @_;

    foreach my $message ( @$messages ) {

	my $type = $message->type;
	my $from = $message->interval_from;
	my $to = $message->interval_to;
	my $start = $message->start;
	my $end = $message->end;
	my $meta = $message->meta;
	my $values = $message->values;
	my $required_meta = $message->required_meta;

	# craft the query needed to fetch the data from the necessary interval
	my $from_clause = "from $type";
	my $values_clause = $self->_get_values_clause( from => $from, values => $values );
	my $between_clause = $self->_get_between_clause( start => $start, end => $end, from => $from );
	my $where_clause = $self->_get_where_clause( $meta );
	my $by_clause = $self->_get_by_clause( $required_meta );

	my $query = "$values_clause $between_clause $by_clause $from_clause $where_clause";

	warn $query;

	# issue the query to the webservice to retrieve the data we need to aggregate
	my $results = $self->websvc->query( query => $query );

	# handle any errors attempting to query the webservice
	if ( !$results ) {

	    die( "Error querying TSDS web service: " . $self->websvc->get_error() );
	}

	if ( $results->{'error'} ) {

	    die( "Error retrieving data from TSDS: " . $results->{'error_text'} );
	}

	$results = $results->{'results'};

	foreach my $result ( @$results ) {

	    warn Dumper $result;
	}
    }
}

sub _get_values_clause {

    my ( $self, %args ) = @_;

    my $from = $args{'from'};
    my $values = $args{'values'};

    # convert each value to proper aggregation based upon the interval we are fetching the data from
    my @values = map { "aggregate(values.$_, $from, average) as $_" } @$values;

    # comma separate each
    my $values_clause = "get " . join( ', ', @values );
    
    return $values_clause;
}

sub _get_between_clause {

    my ( $self, %args ) = @_;

    my $start = $args{'start'};
    my $end = $args{'end'};
    my $from = $args{'from'};

    # make sure we fetch all data within the from interval
    $start = nlowmult( $from, $start );
    $end = nhimult( $from, $end );

    return "between ($start, $end)";
}

sub _get_by_clause {

    my ( $self, $required_meta ) = @_;

    my $by_clause = "by " . join( ',', @$required_meta );

    return $by_clause;
}

sub _get_where_clause {

    my ( $self, $measurements ) = @_;

    my @or_clauses;

    foreach my $measurement ( @$measurements ) {

	my @clause;

	while ( my ( $key, $value ) = each( %$measurement ) ) {

	    push( @clause, "$key = \"$value\"" );
	}
	
	my $clause = '( ' . join( ' and ', @clause ) . ' )';

	push( @or_clauses, $clause );
    }
   
    my $where_clause = "where " . join( ' or ', @or_clauses );

    return $where_clause;    
}

sub _rabbit_connect {

    my ( $self ) = @_;

    my $rabbit_host = $self->config->get( '/config/rabbit/host' );
    my $rabbit_port = $self->config->get( '/config/rabbit/port' );
    my $rabbit_pending_queue = $self->config->get( '/config/rabbit/pending-queue' );
    my $rabbit_finished_queue = $self->config->get( '/config/rabbit/finished-queue' );

    while ( 1 ) {

        $self->logger->info( "Connecting to RabbitMQ $rabbit_host:$rabbit_port." );

        my $connected = 0;

        try {

            my $rabbit = Net::AMQP::RabbitMQ->new();

            $rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );

	    # open channel to the pending queue we'll read from
            $rabbit->channel_open( PENDING_QUEUE_CHANNEL );
            $rabbit->queue_declare( PENDING_QUEUE_CHANNEL, $rabbit_pending_queue, {'auto_delete' => 0} );
            $rabbit->basic_qos( PENDING_QUEUE_CHANNEL, { prefetch_count => QUEUE_PREFETCH_COUNT } );
            $rabbit->consume( PENDING_QUEUE_CHANNEL, $rabbit_pending_queue, {'no_ack' => 0} );

	    # open channel to the finished queue we'll send to
            $rabbit->channel_open( FINISHED_QUEUE_CHANNEL );
            $rabbit->queue_declare( FINISHED_QUEUE_CHANNEL, $rabbit_finished_queue, {'auto_delete' => 0} );

            $self->_set_rabbit( $rabbit );

            $connected = 1;
        }

        catch {

            $self->logger->error( "Error connecting to RabbitMQ: $_" );
        };

        last if $connected;

        $self->logger->info( "Reconnecting after " . RECONNECT_TIMEOUT . " seconds..." );
        sleep( RECONNECT_TIMEOUT );
    }
}

1;
