package GRNOC::TSDS::Aggregate::Daemon;

use strict;
use warnings;

use Moo;
use Types::Standard qw( Str Bool );

use GRNOC::Config;
use GRNOC::Log;

use Proc::Daemon;

use Data::Dumper;
use MongoDB;

use Net::AMQP::RabbitMQ;
use JSON::XS;

use POSIX;

extends 'GRNOC::TSDS::Aggregate';

### private attributes ###

has required_fields => ( is => 'rwp', 
			 default => sub { {} });

has identifiers => ( is => 'rwp',
		     default => sub { {} } );

has now => ( is => 'rwp',
	     default => 0 );

has mongo => ( is => 'rwp' );

has rabbit => ( is => 'rwp' );

has rabbit_queue => ( is => 'rwp' );

### public methods ###

sub start {

    my ( $self ) = @_;

    log_info( 'Starting TSDS Aggregate daemon.' );

    if (! $self->config ){
        die "Unable to load config file";
    }

    log_debug( 'Setting up signal handlers.' );

    # need to daemonize
    if ( $self->daemonize ) {

        log_debug( 'Daemonizing.' );

        my $daemon = Proc::Daemon->new( pid_file => $self->config->get( '/config/pid-file' ) );

        my $pid = $daemon->Init();

        # in child/daemon process
        if ( $pid ){
            log_debug(" Forked child $pid, exiting process ");
            return;
        }

        log_debug( 'Created daemon process.' );
    }

    # dont need to daemonize
    else {

        log_debug( 'Running in foreground.' );
    }

    $self->_mongo_connect() or return;

    $self->_rabbit_connect() or return;

    log_debug("Entering main work loop");

    $self->_work_loop();

    return 1;
}

sub _work_loop {
    my ( $self ) = @_;

    while (1){

        my $next_wake_time;
    
        $self->_set_now(time());

        # Find the databases that need workings
        my $dbs = $self->_get_aggregate_policies();

        if ((keys %$dbs) == 0){
            log_info("No aggregate policies to work on, sleeping for 60s...");
            sleep(60);
            next;
        }

        # For each of those databases, determine whether
        # it's time to do work yet or not
        foreach my $db_name (keys %$dbs){
            my $policies = $dbs->{$db_name};

	    my $next_run = $self->_evaluate_policies($db_name, $policies) or next;

	    next if (! defined $next_run);

	    log_debug("Next run is $next_run for $db_name");

	    # Figure out when the next time we need to look at this is.
	    # If it's closer than anything else, update our next wake 
	    # up time to that
	    $next_wake_time = $next_run if (! defined $next_wake_time || $next_run < $next_wake_time);	    
	}
       
        log_debug("Next wake time is $next_wake_time");

        # Sleep until the next time we've determined we need to do something
        my $delta = $next_wake_time - time;
        if ($delta > 0){
            log_info("Sleeping $delta seconds until next work");
            sleep($delta);
        }
        else {
            log_debug("Not sleeping since delta <= 0");
        }
    }
}

# Operate on all the aggregation policies for a given database
# and create any messages needed.
sub _evaluate_policies {
    my ( $self, $db_name, $policies ) = @_;

    # Make sure we get them in the ascending interval order
    # but descending eval position order so that in the event
    # of a time for interval we use the heaviest evaluated one
    @$policies = sort {$a->{'interval'} <=> $b->{'interval'}
		       ||
		       $b->{'eval_position'} <=> $a->{'eval_position'}} @$policies;
    
    # Keep track of the earliest we need to run next for this db
    my $lowest_next_run;

    # Make sure we know the required fields for this database
    if (! $self->required_fields->{$db_name}){
	my $required_fields = $self->_get_required_fields($db_name);
	return if (! defined $required_fields);
	$self->required_fields->{$db_name} = $required_fields;
    }

    # Iterate over each policy to figure out what needs doing if anything
    foreach my $policy (@$policies){	
	my $interval = $policy->{'interval'};
	my $name     = $policy->{'name'};
	my $last_run = $policy->{'last_run'} || 0;

	my $next_run = $last_run + $interval;
	
	# If there's work to do, let's craft a work
	# order out to some worker and send it
	if ($next_run <= $self->now()){

	    # Get the set of measurements that apply to this policy
	    my $measurements = $self->_get_measurements($db_name, $policy);
	    next if (! $measurements);

	    # Find the previous policy that applied to this measurement so we know
	    # what dirty docs to check
	    # This will also omit any measurements that were already applied to a same
	    # interval but heavier weighted policy to avoid the redundancy. Later on we
	    # only care about what interval was picked, not why it was picked.
	    my $work_buckets = $self->_find_previous_policies(db           => $db_name,
							      current      => $policy,
							      policies     => $policies,
							      measurements => $measurements);

	    # Figure out from those measurements which have data that needs
	    # aggregation into this policy
	    foreach my $prev_interval (keys %$work_buckets){
		my $interval_measurements = $work_buckets->{$prev_interval};

		my $dirty_docs = $self->_get_dirty_data($db_name, $prev_interval, $last_run, $interval_measurements);
		return if (! $dirty_docs);
		
		# Create and send out rabbit messages describing work that
		# needs doing
		my $result = $self->_generate_work(db            => $db_name,
						   interval_from => $prev_interval,
						   interval_to   => $interval,
						   docs          => $dirty_docs,
						   measurements  => $interval_measurements);
		if (! defined $result){
		    log_warn("Error generating work for $db_name policy $name, skipping");
		    return;
		}		
	    }
	    
	    # Update the aggregate to show the last time we successfully 
	    # generated work for this
	    $self->mongo->get_database($db_name)
		->get_collection("aggregate")
		->update({"name" => $name}, {'$set' => {"last_run" => $next_run}});


	    # If we ran, our next run is actually one step ahead
	    $next_run += $interval;
	}

	# Figure out the nearest next run time
	$lowest_next_run = $next_run if (! defined $lowest_next_run || $next_run < $lowest_next_run);
    }
    
    return $lowest_next_run;
}

# Returns an array of just the required field names for a given
# TSDS database
sub _get_required_fields {
    my ( $self, $db_name ) = @_;

    my $metadata;

    eval {
	$metadata = $self->mongo->get_database($db_name)->get_collection('metadata')->find_one();
    };
    if ($@){
	log_warn("Error getting metadata from mongo for $db_name: $@");
	return;
    }

    my @required;

    my $meta_fields = $metadata->{'meta_fields'};

    foreach my $field (keys %$meta_fields){
	next unless ($meta_fields->{$field}->{'required'});
	push(@required, $field);
    }

    # Don't think this should ever be hit, but as a fail safe let's
    # make sure to check we found at least something because otherwise
    # sadness will ensue
    if (@required == 0){
	log_warn("Unable to determine required meta fields for $db_name");
	return;
    }

    return \@required;
}

sub _get_measurements {
    my ( $self, $db_name, $policy ) = @_;

    my $meta     = $policy->{'meta'};
    my $interval = $policy->{'interval'};
    my $name     = $policy->{'name'};

    my $obj;
    eval {
	$obj = JSON::XS::decode_json($meta);
    };
    if ($@){
	log_warn("Unable to decode \"$meta\" as JSON in $db_name policy $name: $@");
	return;
    }

    # Build up the query we need to aggregate
    my @agg;

    # First we need to match on the meta fields specified in this policy
    push(@agg, {'$match' => $obj});

    # Then we need to build up the fields we're going to be grouping together
    # on. Identifier will always be there then the required fields
    # This will get us the unique set of identifiers along with the required
    # fields that went into them
    my $group = {'identifier' => '$identifier'};
    foreach my $field (@{$self->required_fields->{$db_name}}){
	$group->{$field} = '$field';
    }
    push(@agg, {'$group' => {'_id' => $group}});

    log_debug("Aggregate clause is: " . Dumper(\@agg));

    my $results;
    eval {
	$results = $self->mongo
	    ->get_database($db_name)
	    ->get_collection("measurements")
	    ->aggregate(\@agg);
    };
    if ($@){
	log_warn("Error getting distinct identifiers: $@");
	return;
    }

    # Convert to a hash for easier lookup later
    my %lookup;
    foreach my $res (@$results){
	die Dumper($res);
	$lookup{$res->{'identifier'}} = $res;
    }

    # Remember these identifiers so that we can reference them later
    # when figuring out the nearest policy
    $self->identifiers->{$db_name . $name} = \%lookup;

    log_debug("Found " . scalar(keys %lookup) . " measurements for $db_name policy $name");

    return \%lookup;
}

# Given the set of measurements and all the policies,
# find the previous policy that applies to each measurement
sub _find_previous_policies {
    my ( $self, %args ) = @_;

    my $db_name        = $args{'db'};
    my $current_policy = $args{'current'};
    my $policies       = $args{'policies'};
    my $measurements   = $args{'measurements'};

    my %buckets;

    # Make sure we get in descending order, we want to find the highest
    # possible previous one
    my @sorted = sort {$b->{'interval'} <=> $a->{'interval'}
		       ||
		       $b->{'eval_position'} <=> $a->{'eval_position'}} @$policies;

    my $current_interval = $current_policy->{'interval'};

    my @possible_matches;

    # We're looking for a prior policy so the interval has to be smaller
    # or can be the same, in which case if we've already seen
    # the identifier in another policy with the same interval we can skip it
    # here since we have already aggregated it   
    foreach my $policy (@sorted){
	my $interval = $policy->{'interval'};	
	next if ($interval > $current_interval);	
	push(@possible_matches, $policy);
    }


    foreach my $identifier (keys %$measurements){

	# Keep track of what we're ultimately choosing for this
	# identifier
	my $chosen;

	# These will still be sorted by interval and eval position appropriately
	# so we want to see if any have matched this before
	my $already_done = 0;
	foreach my $match (@possible_matches){
	    # We can't use this as the previous policy if that policy didn't include this identifier
	    next unless (exists $self->identifiers->{$db_name . $match->{'name'}}->{$identifier});

	    # If this policy DID include the identifier, we have to see if it was the same interval
	    # or not. If it's the same interval, we don't need to re-aggregate this at all at this
	    # level since it would be redundant work.
	    if ($match->{'interval'} eq $current_interval){
		$already_done = 1;
	    }	   

	    $chosen = $match;
	    last;
	}

	# If we decided that we had already aggregated this measurement at this
	# interval, we don't need to do anything else here.
	next if ($already_done);

	# Use the chosen's interval if we can use a prior aggregation policy. If we can't
	# then default to interval of 1, or hi-res
	my $chosen_interval = defined $chosen ? $chosen->{'interval'} : 1;

	$buckets{$chosen_interval}{$identifier} = $measurements->{$identifier};
    }

    return \%buckets;
}

# Given a database name, an interval, and a timestamp it this figures out
# what data documents for that interval have been updated since the timestamp.
# This returns all those documents
sub _get_dirty_data {
    my ( $self, $db_name, $interval, $last_run, $measurements ) = @_;

    my @ids = keys %$measurements;

    my $query = {
	'updated'    => {'$gte' => $last_run},
	'identifier' => {'$in' => \@ids}
    };

    my $col_name = "data";
    if ($interval && $interval > 1){
	$col_name = "data_$interval";
    }

    my $collection = $self->mongo->get_database($db_name)->get_collection($col_name);

    my $cursor;
    eval {
	$cursor = $collection->find($query)->fields({"updated_start" => 1,
						     "updated_end"   => 1,
						     "identifier"    => 1});
    };

    if ($@){
	log_warn("Unable to find dirty documents in $db_name at interval $interval: $@");
	return;
    }

    my @docs;

    while (my $doc = $cursor->next() ){
	push(@docs, $doc);	
    }

    return \@docs;
}

# Formulate and send a message out to a worker
sub _generate_work {
    my ( $self, %args ) = @_;

    my $db             = $args{'db'};
    my $interval_from  = $args{'interval_from'};
    my $interval_to    = $args{'interval_to'};
    my $docs           = $args{'docs'};
    my $measurements   = $args{'measurements'};

    my @messages;

    # Go through each data document and create a message describing
    # the work needed for the doc.

    # We want to group all the interfaces with the same ceil/floor together
    # so that we can condense them into the same message and have them be
    # serviceable in the same query

    my %grouped;

    foreach my $doc (@$docs){
	my $updated_start = $doc->{'updated_start'};
	my $updated_end   = $doc->{'updated_end'};
	my $identifier    = $doc->{'identifier'};

	# We want to floor/ceil to find the actual timerange affected in the
	# interval in this doc. ie we might have only touched data within one
	# hour but it's going to impact that whole day. Any other measurements
	# impacted during that day can be grouped into the same query
	my $floor = int($updated_start / $interval_to) * $interval_to;
	my $ceil  = int(ceil($updated_end / $interval_to)) * $interval_to;

	push(@{$grouped{$floor}{$ceil}}, $doc);
    }

    # Now that we have grouped the messages based on their timeframes
    # we can actually ship them out to rabbit
    foreach my $start (keys %grouped){
	foreach my $end (keys %{$grouped{$start}}){

	    my $grouped_docs = $grouped{$start}{$end};

	    log_debug("Sending messages for $start - $end, interval_from = $interval_from interval_to = $interval_to, total grouped measurements = " . scalar(@$grouped_docs));

	    my $message = {            
		type           => $db,
		interval_from  => $interval_from,
		interval_to    => $interval_to,
		start          => $start,
		end            => $end,	    
		meta           => [],
		values         => $self->values->{$db}
	    };

	    # Add the meta fields to our message identifying
	    # this measurement
	    foreach my $doc (@$grouped_docs){

		my $measurement = $measurements->{$doc->{'identifier'}};
		my $meta = {};
		foreach my $field (keys %$measurement){
		    $meta->{$field} = $measurement->{$field};
		    push(@{$message->{'meta'}}, $meta);
		}

		# Avoid making messages too big, chunk them up
		if (@{$message->{'meta'}} >= 50){
		    $self->rabbit->publish(1, $self->rabbit_queue, encode_json($message), {'exchange' => ''});
		    $message->{'meta'} = [];
		}
	    }

	    # Any leftover tasks, send that too
	    if (@{$message->{'meta'}} >= 1){
		$self->rabbit->publish(1, $self->rabbit_queue, encode_json($message), {'exchange' => ''});
	    }
	}    
    }

    return 1;
}


sub _get_aggregate_policies {
    my ( $self ) = @_;

    my @db_names = $self->mongo->database_names;

    my %policies;

    foreach my $db_name (@db_names){

	# TEMPORARY HACK FOR TESTING
	next unless ($db_name eq 'interface');

        my $cursor;
	my @docs;
        eval {
            $cursor = $self->mongo->get_database($db_name)->get_collection("aggregate")->find();
	    while (my $doc = $cursor->next()){

		if (! defined $doc->{'eval_position'} || ! defined $doc->{'interval'}){
		    log_warn("Skipping " . $doc->{'name'} . " due to missing eval position and/or interval");
		    next;
		}

		push(@docs, $doc);
	    }
        };
        if ($@){
            if ($@ !~ /not authorized/){
                log_warn("Error querying mongo: $@");
            }
            next;
        }

	if (! @docs){
	    log_debug("No aggregate policies found for database $db_name");
	    next;
	}

	$policies{$db_name} = \@docs;
    }

    log_debug("Found aggregate policies: " . Dumper(\%policies));

    return \%policies;
}


sub _mongo_connect {
    my ( $self ) = @_;

    my $mongo_host = $self->config->get( '/config/master/mongo/host' );
    my $mongo_port = $self->config->get( '/config/master/mongo/port' );
    my $user       = $self->config->get( '/config/master/mongo/username' );
    my $pass       = $self->config->get( '/config/master/mongo/password' );

    log_debug( "Connecting to MongoDB as $user:$pass on $mongo_host:$mongo_port." );

    my $mongo;
    eval {
        $mongo = MongoDB::MongoClient->new(
            host => "$mongo_host:$mongo_port",
            query_timeout => -1,
            username => $user,
            password => $pass
            );
    };
    if($@){
        log_warn("Could not connect to Mongo: $@");
        return;
    }

    log_debug("Connected");

    $self->_set_mongo( $mongo );
}


sub _rabbit_connect {
    my ( $self ) = @_;

    my $rabbit = Net::AMQP::RabbitMQ->new();   

    my $rabbit_host = $self->config->get( '/config/rabbit/host' );
    my $rabbit_port = $self->config->get( '/config/rabbit/port' );
    my $rabbit_queue = $self->config->get( '/config/rabbit/pending-queue' );

    log_debug("Connecting to RabbitMQ on $rabbit_host:$rabbit_port with queue $rabbit_queue");

    my $rabbit_args = {'port' => $rabbit_port};

    eval {
        $rabbit->connect( $rabbit_host, $rabbit_args );
        $rabbit->channel_open( 1 );
        $rabbit->queue_declare( 1, $rabbit_queue, {'auto_delete' => 0} );
    };
    if ($@){
        log_warn("Unable to connect to RabbitMQ: $@");
        return;
    }

    $self->_set_rabbit_queue($rabbit_queue);
    $self->_set_rabbit($rabbit);

    log_debug("Connected");

    return 1;
}

1;
