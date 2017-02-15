package GRNOC::TSDS::Aggregate::Daemon;

use strict;
use warnings;

use Moo;
use Types::Standard qw( Str Bool );
use Try::Tiny;

use GRNOC::Config;
use GRNOC::Log;

use Proc::Daemon;
use Parallel::ForkManager;

use POSIX;
use Data::Dumper;
use List::MoreUtils qw(natatime);

use MongoDB;

use Net::AMQP::RabbitMQ;
use JSON::XS;

use Redis;
use Redis::DistLock;

use Time::HiRes qw(tv_interval gettimeofday);

### public attributes ###

has config_file => ( is       => 'ro',		     
                     isa      => Str,
                     required => 1 );

has daemonize => ( is       => 'ro',
                   isa      => Bool,
		   required => 1,
                   default  => 1 );

has chunk_size => ( is       => 'rw',
		    required => 1,
		    default  => 100 );

has message_size => ( is       => 'rw',
		      required => 1,
		      default  => 50 );

has lock_timeout => ( is      => 'rw',
		      default => 120 );

has max_docs_chunk => ( is      => 'rw',
		        default => 100 );

has force_database => ( is => 'rw',
			default => sub { undef } );

has force_policy => ( is => 'rw',
		      default => sub { undef } );

has force_start => ( is => 'rw',
		     default => sub { undef },
		     coerce => sub { defined $_[0] ? $_[0] + 0 : undef });

has force_end => ( is => 'rw',
		   default => sub { undef },
		   coerce => sub { defined $_[0] ? $_[0] + 0 : undef });

has force_query => ( is => 'rw',
		     default => sub { undef },
		     coerce => sub { defined $_[0] ? JSON::XS::decode_json($_[0]) : undef });

### private attributes ###

has required_fields => ( is => 'rwp', 
			 default => sub { {} });

has value_fields => ( is => 'rwp', 
		     default => sub { {} });

has identifiers => ( is => 'rwp',
		     default => sub { {} } );

has now => ( is => 'rwp',
	     default => 0 );

has mongo => ( is => 'rwp' );

has rabbit => ( is => 'rwp' );

has rabbit_queue => ( is => 'rwp' );

has locker => ( is => 'rwp' );

has locks => ( is => 'rwp',
	       default => sub { [] } );

has config => ( is => 'rwp' );

### public methods ###

sub BUILD {

    my ( $self ) = @_;

    # create and store config object
    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 0 );

    $self->_set_config( $config );   

    my $chunk_size = $config->get('/config/master/num_concurrent_measurements');
    $self->chunk_size($chunk_size) if ($chunk_size);

    my $message_size = $config->get('/config/master/num_messages');
    $self->message_size($message_size) if ($message_size);

    my $lock_timeout = $config->get('/config/master/lock_timeout');
    $self->lock_timeout($lock_timeout) if ($lock_timeout);

    my $max_docs_chunk = $config->get('/config/master/max_docs_per_block');
    $self->max_docs_chunk($max_docs_chunk) if ($max_docs_chunk);

    log_info("Starting with chunk size = " . $self->chunk_size() . ", message size = " . $self->message_size() . ", lock timeout = " . $self->lock_timeout() . ", max docs chunk = " . $self->max_docs_chunk());

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

    my ( $self ) = @_;

    log_info( 'Starting TSDS Aggregate daemon.' );

    if (! $self->config ){
        die "Unable to load config file";
    }

    log_debug( 'Setting up signal handlers.' );

    # need to daemonize
    if ( $self->daemonize ) {

        log_debug( 'Daemonizing.' );

        my $daemon = Proc::Daemon->new( pid_file => $self->config->get( '/config/master/pid-file' ) );

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
    
    $self->_work_loop();

    return 1;
}


sub stop(){
    my ( $self ) = @_;

    log_info( 'Stopping.' );

    exit(0);
}


sub _work_loop {
    my ( $self ) = @_;

    log_debug("Entering main work loop");

    while (1){

	# reconncet
	$self->_connect();		

        my $next_wake_time;
    
        $self->_set_now(time());

        # Find the databases that need workings
	my $dbs;
	try {
	    $dbs = $self->_get_aggregate_policies();
	}
	catch {
	    log_warn("Unable to get aggregate policies: $_");
	};

        if (! defined $dbs || (keys %$dbs) == 0){
            log_info("No aggregate policies to work on, sleeping for 60s...");
            sleep(60);
            next;
        }

	# Forget any previous identifiers, need to re-learn every run in case
	# they change
	$self->_set_identifiers({});

	# We need to fork here because perl doesn't do a very good job of releasing
	# memory back to the system, and the next block can be expensive if there
	# is a lot to do at some iteration, so this is how we get around that as a 
	# long lived process. Yeah...
	my $fm = Parallel::ForkManager->new(1);

	# the fork is going to return the calculated next wake up time
	# back to us so we can sleep in the parent
	$fm->run_on_finish(
	    sub {
		my ($pid,$exit_code,$ident,$exit_signal,$core_dump,$data)=@_;

		log_debug("Fork $pid exited with status = $exit_code, args = " . Dumper($data));

		if ($data && ref($data) eq 'ARRAY' && @$data > 0){
		    $next_wake_time = $data->[0];
		}
	    }
	    );
	
	# in the child
	if (! $fm->start){

	    # reconnect in fork
	    $self->_connect();		

	    # For each of those databases, determine whether
	    # it's time to do work yet or not
	    foreach my $db_name (keys %$dbs){
		my $policies = $dbs->{$db_name};
		
		my $next_run;
		my $failed = 0;
		try {
		    # Make sure we know the required fields for this database
		    my $success = $self->_get_metadata($db_name);
		    return if (! defined $success);		
		    
		    $next_run = $self->_evaluate_policies($db_name, $policies);
		    return if (! defined $next_run);
		    
		    log_info("Next run is $next_run for $db_name (" . localtime($next_run) . ")");
		}
		catch {
		    log_warn("Caught exception while processing $db_name: $_");
		    $failed = 1;
		};
		
		# if we failed for whatever reason, reconnect to everything as a safety
		if ($failed){
		    log_info("Reconnecting to everything due to failure");
		    $self->_connect();		
		}
		
		# Possibly redundant release in case an exception happened above, want
		# to make sure we're not hanging on to things
		$self->_release_locks();

		next if (! defined $next_run);
		
		# Figure out when the next time we need to look at this is.
		# If it's closer than anything else, update our next wake 
		# up time to that
		$next_wake_time = $next_run if (! defined $next_wake_time || $next_run < $next_wake_time);	    
	    }

	    $fm->finish(0, [$next_wake_time]);
	}

	$fm->wait_all_children();

	# If a specific timeframe was given, we're all done now
	if ($self->force_start){
	    log_info("Ending run due to specific start time given");
	    exit(0);
	}

	if (! defined $next_wake_time){
	    log_info("Unable to determine a next wake time, did exceptions happen? Sleeping for 60s and trying again");
	    $next_wake_time = time() + 60;
	}
       
        log_debug("Next wake time is $next_wake_time (" . localtime($next_wake_time) . ")");

        # Sleep until the next time we've determined we need to do something
        my $delta = $next_wake_time - time;
        if ($delta > 0){
            log_info("Sleeping $delta seconds until next work");
            sleep($delta);
        }
	# This shouldn't happen, means we're falling behind
        else {
            log_warn("Not sleeping since delta <= 0, was $delta");
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

    # Iterate over each policy to figure out what needs doing if anything
    foreach my $policy (@$policies){	

	my $start = [gettimeofday];

	my $interval = $policy->{'interval'};
	my $name     = $policy->{'name'};
	my $last_run = $policy->{'last_run'} || 0;

	log_debug("Last run for $db_name $name was $last_run (" . localtime($last_run) . ")" );

	my $next_run = $last_run + $interval;
	
	# If there's work to do, let's craft a work
	# order out to some worker and send it
	if ($next_run <= $self->now() || $self->force_start){

	    # If any chunks fail, we have to NOT update the last_run time so that
	    # we can retry it later. Because we still clear the flags on the ones that
	    # did succeed we don't have to worry about duplicate data being sent
	    my $any_failed = 0;

	    # Always need to get the set of measurements that apply to this policy
	    # since later on we're assuming a policy higher up will be able to 
	    # see what this policy will have touched
	    my $measure_start = [gettimeofday];
	    my $all_measurements = $self->_get_measurements($db_name, $policy);
	    log_debug("Took " . tv_interval($measure_start, [gettimeofday]) . " to get all measurements");
	    next if (! $all_measurements);

	    # After getting the measurements we might not actually need to do work
	    # for this if there was a specific policy given
	    next if ($self->force_policy && $name ne $self->force_policy);

	    # Chunk doing the measurements so that we're not blocking everything
	    my $iterator = natatime($self->chunk_size(), keys %$all_measurements);

	    my $total_chunks  = ceil(scalar(keys %$all_measurements) / $self->chunk_size());
	    my $current_chunk = 0;
	    while (my @identifiers = $iterator->() ){

		$current_chunk++;
		log_info("Chunk $current_chunk / $total_chunks");

		# build up our chunk of measurements from all measurements
		my $measurements = {};
		foreach my $identifier (@identifiers){
		    $measurements->{$identifier} = $all_measurements->{$identifier};
		}

		# Find the previous policy that applied to this measurement so we know
		# what dirty docs to check
		# This will also omit any measurements that were already applied to a same
		# interval but heavier weighted policy to avoid the redundancy. Later on we
		# only care about what interval was picked, not why it was picked.
		my $prev_start = [gettimeofday];
		my $work_buckets = $self->_find_previous_policies(db           => $db_name,
								  current      => $policy,
								  policies     => $policies,
								  measurements => $measurements);
		log_debug("Took " . tv_interval($prev_start, [gettimeofday]) . " to get prev policies for bucket");
		
		# Figure out from those measurements which have data that needs
		# aggregation into this policy
		foreach my $prev_interval (keys %$work_buckets){
		    log_debug("Processing data from $prev_interval");

		    my $block = 0;

		    my $interval_measurements = $work_buckets->{$prev_interval};

		    my @done_ids;

		    # keep getting data until we're finished with this chunk
		    while (1){

			log_debug("Fetching data block = $block");
			log_debug("sizeof ignore_doc_ids = " . scalar(@done_ids));

			my $dirty_start = [gettimeofday];
			my $dirty_docs;
			try {
			    $dirty_docs = $self->_get_data(db             => $db_name,
							   interval       => $prev_interval,
							   last_run       => $last_run,
							   measurements   => $interval_measurements,
							   limit          => $self->max_docs_chunk(),
							   ignore_doc_ids => \@done_ids);
			     log_debug("Took " . tv_interval($dirty_start, [gettimeofday]) . " to get dirty docs for $prev_interval bucket");
			}
			catch {
			    $any_failed = 1;
			    log_warn("Unable to get docs for current measurements, continuing on: $_");
			};

			# if we had an error or we were finished finding docs in this block, we're done here
			last if (! $dirty_docs || @$dirty_docs == 0);

			# keep track of the _ids of docs we have already looked at, this will help prevent us
			# from getting stuck where a doc is live updating and we would otherwise find it
			# multiple times and miss other things
			foreach my $dirty_doc (@$dirty_docs){
			    push(@done_ids, $dirty_doc->{'_id'});
			}

			# Create and send out rabbit messages describing work that
			# needs doing
			my $work_start = [gettimeofday];
			my $result = $self->_generate_work(policy        => $policy,
							   db            => $db_name,
							   interval_from => $prev_interval,
							   interval_to   => $interval,
							   docs          => $dirty_docs,
							   measurements  => $interval_measurements);
			log_debug("Took " . tv_interval($work_start, [gettimeofday]) . " to get generate work for $prev_interval bucket");
		  
			if (! defined $result){
			    log_warn("Error generating work for $db_name policy $name, skipping");
			    return;
			}		

			# Increment to our next block
			$block++;
		    }
		}
	    }

	    # Update the aggregate to show the last time we successfully 
	    # generated work for this
	    
	    # Floor the "now" to the interval to make a restart run pick a pretty
	    # last run time. This is still accurate since floored must be <= $now
	    my $floored = int($self->now() / $interval) * $interval;
	    if (! $any_failed && ! defined $self->force_start){
		$self->mongo->get_database($db_name)
		    ->get_collection("aggregate")
		    ->update_one({"name" => $name}, {'$set' => {"last_run" => $floored}});		
	    }
	    # Since we ran, the next time we need to look is the next time this
	    # the next time its interval is coming around in the future
	    $next_run = $floored + $interval;
	}

	# Figure out the nearest next run time
	$lowest_next_run = $next_run if (! defined $lowest_next_run || $next_run < $lowest_next_run);

	log_info("Policy $db_name $name evaluated in " . tv_interval($start, [gettimeofday]) . " seconds");
    }
    
    return $lowest_next_run;
}

# Returns an array of just the required field names for a given
# TSDS database
sub _get_metadata { 
    my ( $self, $db_name ) = @_;

    my $metadata;

    try {
	$metadata = $self->mongo->get_database($db_name)->get_collection('metadata')->find_one([]);
    }
    catch {
	log_warn("Error getting metadata from mongo for $db_name: $_");
    };

    return if (! $metadata);

    my @required;

    my $meta_fields = $metadata->{'meta_fields'};

    foreach my $field (keys %$meta_fields){
	next unless ($meta_fields->{$field}->{'required'});
	push(@required, $field);
    }

    my @values;

    foreach my $field (keys %{$metadata->{'values'}}){
	push(@values, $field);
    }

    # Remember these
    $self->required_fields->{$db_name} = \@required;
    $self->value_fields->{$db_name} = \@values;

    log_debug("Required fields for $db_name = " . Dumper(\@required));
    log_debug("Values for $db_name = " . Dumper(\@values));

    # Don't think this should ever be hit, but as a fail safe let's
    # make sure to check we found at least something because otherwise
    # sadness will ensue
    if (@required == 0 || @values == 0){
	log_warn("Unable to determine required meta fields and/or value field names for $db_name");
	return;
    }

    return 1;
}

# Finds all measurements that apply to a given aggregation policy
sub _get_measurements {
    my ( $self, $db_name, $policy ) = @_;

    my $meta     = $policy->{'meta'};
    my $interval = $policy->{'interval'};
    my $name     = $policy->{'name'};

    my $start = [gettimeofday];

    my $obj;
    try {
	$obj = JSON::XS::decode_json($meta);  
    }
    catch {
	log_warn("Unable to decode \"$meta\" as JSON in $db_name policy $name: $_");
    };

    return if (! $obj);

    # If there was a query given, make sure we merge it into the 
    # existing policy definition for this
    if (defined $self->force_query){
	if (defined $obj->{'$and'}){
	    push(@{$obj->{'$and'}}, $self->force_query);
	}
	else {
	    $obj = {'$and' => [$obj, $self->force_query]};
	}
    }

    log_debug("Fetching from $db_name.measurements where " . Dumper($obj));

    # Step 1 is to get a distinct set of identifiers that this query
    # matches so that we can iterate over them and get their most recent
    # entry
    my $distinct;
    try {
	$distinct = $self->mongo
	    ->get_database($db_name)
	    ->run_command([distinct => "measurements",
			   key      => "identifier",
			   query    => $obj]);
    }
    catch {
	log_warn("Unable to find distint identifiers for $db_name policy $name: $_");
    };

    return if (! $distinct);

    $distinct = $distinct->{'values'};

    log_debug("Found " . scalar(@$distinct) . " distinct identifiers for $db_name policy $name");

    my %lookup;

    # Optimization here - if we've already looked this measurement up on this run
    # because of another policy we can skip doing some of the work below
    my @missing;
    foreach my $identifier (@$distinct){
	my $found = 0;
	foreach my $cache_name (keys %{$self->identifiers()}){
	    next if ($cache_name eq ($db_name . $name)); # don't look at ourselves for a cache
	    next unless ($cache_name =~ /^$db_name/); # make sure it's the same type
	    my $cache = $self->identifiers()->{$cache_name};

	    

	    # Aha, we found it. Store the same reference
	    if (exists $cache->{$identifier}){
		$lookup{$identifier} = $cache->{$identifier};
		$found = 1;
		last;
	    }
	}
	push(@missing, $identifier) if (! $found);
    }

    log_debug("Found previous cache hits for " .  (scalar(@$distinct) - scalar(@missing)) . " of " . scalar(@$distinct) . " identifiers");

    # For anything else that we hadn't already seen we're going to look it up
    # below
    my $iterator = natatime($self->chunk_size(), @missing);

    while (my @identifiers = $iterator->() ){
	
	# Build up the query we need to aggregate
	my @agg;
	
	# Hm this isn't technically accurate, we might need to do multiple passes
	# once we find the data to figure out if the metadata at the time matched
	# For now this finds the most recent version of the identifier
	push(@agg, {'$match' => {'identifier' => {'$in' => \@identifiers}}});
	push(@agg, {'$group' => {'_id'       => '$identifier', 
				 'max_start' => {'$max' => '$start'}}});
	

	my $cursor;
	try {
	    $cursor = $self->mongo
		->get_database($db_name)
		->get_collection('measurements')
		->aggregate(\@agg);
	}
	catch {
	    log_warn("Unable to fetch latest measurement entries for $db_name policy $name: $_");
	};

	return if (! $cursor);
		
	my @identifiers;
	my @starts;
	my $count = 0;
	while (my $doc = $cursor->next()){
	    push(@identifiers, $doc->{'_id'});
	    push(@starts, $doc->{'max_start'});
	}

	log_debug("Got " . scalar(@identifiers) . " back from agg clause");
	
	# Make sure we get the right set of fields to query
	my $fields = {'identifier' => 1, 'values' => 1, 'start' => 1};
	foreach my $req_field (@{$self->required_fields->{$db_name}}){
	    $fields->{$req_field} = 1;
	}
	
	# Okay, ready to grab the entries now. This is pretty annoying as a 3rd pass
	# but we needed more fields than we could grab in the aggregation pipeline
	undef $cursor;
	try {
	    $cursor = $self->mongo
		->get_database($db_name)
		->get_collection('measurements')
		->find(['identifier' => {'$in' => \@identifiers},
			'start'      => {'$in' => \@starts}])->fields($fields);
	}
	catch {
	    log_warn("Unable to execute latest measurement entries query with fields for $db_name policy $name: $_");
	};

	return if (! $cursor);

    	
	# Store these results
	while (my $doc = $cursor->next()){
	    $lookup{$doc->{'identifier'}} = $doc;
	}
    }

    # Remember these identifiers so that we can reference them later
    $self->identifiers->{$db_name . $name} = \%lookup;

    my $duration = tv_interval($start, [gettimeofday]);

    log_debug("Found " . scalar(keys %lookup) . " measurements for $db_name policy $name in $duration seconds");

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
	# skip ourselves
	next if ($current_policy->{'name'} eq $policy->{'name'});
	next if ($policy->{'interval'} > $current_interval);
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

    foreach my $interval (keys %buckets){
	log_info("Building " . scalar(keys %{$buckets{$interval}}) . " measurements for interval $current_interval from interval $interval");

	# Should never hit this but an easy sanity check. We can never build say a 1 hour
	# aggregate from a 1 day so if something above fails here's our last check
	if ($current_interval < $interval){
	    log_warn("Internal error - calculated bad prior interval. Aborting.");
	    return;
	}
    }

    return \%buckets;
}

# Given a database name, an interval, and a timestamp it this figures out
# what data documents for that interval have been updated since the timestamp.
# This returns all those documents
sub _get_data {
    my ( $self, %args ) = @_;

    my $db_name          = $args{'db'};
    my $interval         = $args{'interval'};
    my $last_run         = $args{'last_run'};
    my $measurements     = $args{'measurements'};
    my $limit            = $args{'limit'};
    my $ignore_doc_ids   = $args{'ignore_doc_ids'};

    my @identifiers = keys %$measurements;

    # our base query, always scope to just these identifiers
    my $query = Tie::IxHash->new(
	'identifier' => {'$in'  => \@identifiers},
	'_id' => {'$not' => {'$in' => $ignore_doc_ids}}, # skip any data docs we saw in a previous run.
	                                                 # this prevents the issue where you get stuck with a doc
	                                                 # that is being updated and you keep re-finding it
    );

    # If we were given a start/end time, use that
    my $hint;
    if (defined $self->force_start){
        $query->Push('start' => {'$lte' => $self->force_end});
	$query->Push('end'   => {'$gte' => $self->force_start});
	$hint = "identifier_1_start_1_end_1";
    }
    # Otherwise detect anything that needs doing
    else {
	$query->Push('updated' => {'$gt'  => $last_run});
	$hint = "updated_1_identifier_1";
    }

    my $col_name = "data";
    if ($interval && $interval > 1){
	$col_name = "data_$interval";
    }

    # Make sure the data is indexed or this could kill the system
    $self->_verify_indexes($db_name, $col_name) or return;

    my $collection = $self->mongo->get_database($db_name)->get_collection($col_name);

    my $fields = {
	"start"         => 1,
	"end"           => 1,
	"identifier"    => 1,
	"_id"           => 1
    };

    my $fetch_1_start = [gettimeofday];
    my $cursor;
    try {
	$cursor = $collection->find($query)->hint($hint)->fields($fields)
	    ->limit($limit)
    }
    catch {
	log_warn("Unable to find dirty documents in $db_name at interval $interval: $_");
    };

    return if (! $cursor);

    log_debug("Query executed in " . tv_interval($fetch_1_start, [gettimeofday]) . " seconds");

    my @docs;
    while (my $doc = $cursor->next() ){
	push(@docs, $doc);	
    }

    log_debug("Found " . scalar(@docs) . " dirty docs in " . tv_interval($fetch_1_start, [gettimeofday]) ." seconds, attempting to get locks");

    # This part is a bit strange. We have to do a first fetch to figure out
    # what all docs we're going to need to touch. Then we need to lock them
    # all through Redis so that another process doesn't touch them while
    # we're doing our thing. Then we need to fetch them again to ensure that
    # the version in memory is the same as the one on disk
    # We're also fetching them by _id the second time around to make sure
    # we're only getting exactly the ones we have already locked. Any others
    # will be picked up in a later run.
    my @internal_ids;
    my $lock_start = [gettimeofday];
    foreach my $doc (@docs){

	my $key  = $self->_get_cache_key($db_name, $col_name, $doc);
	my $lock = $self->locker->lock($key, $self->lock_timeout());

	if (! $lock){
	    log_warn("Could not get a lock on key $key, skipping it for this run");
	    next;
	}

	push(@internal_ids, $doc->{'_id'});
	push(@{$self->locks()}, $lock);
    }

    log_debug("Got locks in " . tv_interval($lock_start, [gettimeofday]) . " seconds");
    log_debug("Internal IDs size is " . scalar(@internal_ids));

    my $fetch_2_start = [gettimeofday];

    # We need these additional fields now
    $fields->{'updated_start'} = 1;
    $fields->{'updated_end'}   = 1;

    # Now that they're all locked, fetch them again
    undef $cursor;
    try {
	$cursor = $collection->find([_id => {'$in' => \@internal_ids}])->fields($fields);		
    }
    catch {
	log_warn("Unable to find dirty documents on fetch 2: $_");
	return;
    };
    
    return if (! $cursor);

    my @final_docs;
    while (my $doc = $cursor->next()){
	push(@final_docs, $doc);
    }   
    
    log_debug("Found " . scalar(@final_docs) . " final dirty docs to work on in " . tv_interval($fetch_2_start, [gettimeofday]) . " seconds");
    
    return \@final_docs;
}

# Formulate and send a message out to a worker
sub _generate_work {
    my ( $self, %args ) = @_;

    my $db             = $args{'db'};
    my $interval_from  = $args{'interval_from'};
    my $interval_to    = $args{'interval_to'};
    my $docs           = $args{'docs'};
    my $measurements   = $args{'measurements'};
    my $policy         = $args{'policy'};

    my @messages;

    # Go through each data document and create a message describing
    # the work needed for the doc.

    # We want to group all the interfaces with the same ceil/floor together
    # so that we can condense them into the same message and have them be
    # serviceable in the same query
    my %grouped;

    my @clear_doc_ids;

    my $start = [gettimeofday];

    foreach my $doc (@$docs){
	my $doc_start     = $doc->{'start'};
	my $doc_end       = $doc->{'end'};
	my $updated_start = $doc->{'updated_start'};
	my $updated_end   = $doc->{'updated_end'};

	# Handle docs that might have existed prior to setting these fields
	if (! defined $updated_end || ! defined $updated_start){
	    log_debug("Doc is missing updated_end and/or updated_start, using full doc length");
	    $updated_start = $doc_start if (! defined $updated_start);
	    $updated_end = $doc_end if (! defined $updated_end);
	}

	# Whether or not we should clear the flags on this document
	my $should_clear = 1;

	# If the user provided a start/end time we need to use that instead
	# of what's marked on the document
	if ($self->force_start){

	    # If the document is dirty outside of the specified timeframe we shouldn't
	    # clear the flags
	    if ($updated_start < $self->force_start || $updated_end > $self->force_end){
		log_debug("Not clearing flag on doc due to updated flags outside of force_start/end range");
		$should_clear = 0;
	    }

	    $updated_start = $self->force_start;
	    $updated_end   = $self->force_end;
	}

	my $identifier    = $doc->{'identifier'};

	# This shouldn't be possible during auto detect but is 
	# possible during forced timeframes
	if ($updated_end > $doc_end){
	    log_debug("Doc had an updated_end > doc_end ($updated_end vs $doc_end), using doc_end");
	    $updated_end = $doc_end;
	}
	if ($updated_start < $doc_start){
	    log_debug("Doc had an updated_start < doc_start ($updated_start vs $doc_start), using doc_start");
	    $updated_start = $doc_start;
	}

	# We want to floor/ceil to find the actual timerange affected in the
	# interval in this doc. ie we might have only touched data within one
	# hour but it's going to impact that whole day. Any other measurements
	# impacted during that day can be grouped into the same query
	my $floor = int($updated_start / $interval_to) * $interval_to;
	my $ceil  = int(ceil($updated_end / $interval_to)) * $interval_to;

	# There's no point aggregating stuff in the future, so if the ceil is
	# ahead of where we are set it to $now
	if ($ceil > $self->now){
	    log_debug("Ceil of " . localtime($ceil) . " greater than now " . localtime($self->now));
	    $ceil = int($self->now / $interval_to) * $interval_to;
	    log_debug("Setting ceil to " . localtime($ceil));
	}

	# If we've determined that there is no complete section for this update,
	# don't bother
	if ($ceil <= $floor){
	    log_debug("Skipping partially done section");
	    next;
	}

	push(@{$grouped{$floor}{$ceil}}, $doc);

	# If we're running in a user specified timeframe, we need to ensure we only
	# clear out the flag on docs where the user specified time covers the
	# updated times on the doc itself
	push(@clear_doc_ids, $doc->{'_id'}) if ($should_clear);
    }

    my @final_values;
    foreach my $value (@{$self->value_fields->{$db}}){
	my $attributes = {
	    name           => $value,
	    hist_res       => undef,
	    hist_min_width => undef
	};
	if (exists $policy->{'values'}{$value}){
	    $attributes->{'hist_res'}       = $policy->{'values'}{$value}{'hist_res'};
	    $attributes->{'hist_min_width'} = $policy->{'values'}{$value}{'hist_min_width'};
	}
	push(@final_values, $attributes);
    }

    # Now that we have grouped the messages based on their timeframes
    # we can actually ship them out to rabbit
    foreach my $start (keys %grouped){
	foreach my $end (keys %{$grouped{$start}}){

	    my $grouped_docs = $grouped{$start}{$end};

	    log_info("Sending messages for $start (" . localtime($start) . ") - $end (" . localtime($end) . " ), interval_from = $interval_from interval_to = $interval_to, total grouped measurements = " . scalar(@$grouped_docs));

	    my $message = {            
		type           => $db,
		interval_from  => $interval_from,
		interval_to    => $interval_to,
		start          => $start,
		end            => $end,
		meta           => [],
		required_meta  => $self->required_fields->{$db},
		values         => \@final_values		
	    };

	    # Add the meta fields to our message identifying
	    # these measurements
	    foreach my $doc (@$grouped_docs){

		my $measurement = $measurements->{$doc->{'identifier'}};
		my @meas_values;
		my %meas_fields;

		# Add the min/max for values if present
		if (exists $measurement->{'values'}){
		    foreach my $value_name (keys %{$measurement->{'values'}}){
			push(@meas_values, {
			    'name' => $value_name,
			    'min'  => $measurement->{'values'}{$value_name}{'min'},
			    'max'  => $measurement->{'values'}{$value_name}{'max'}
			});
		    }
		}

		# Add the meta required fields
		foreach my $req_field (@{$self->required_fields->{$db}}){
		    $meas_fields{$req_field} = $measurement->{$req_field};
		}

		my $meta = {
		    values => \@meas_values,
		    fields => \%meas_fields
		};

		push(@{$message->{'meta'}}, $meta);

		# Avoid making messages too big, chunk them up
		if (@{$message->{'meta'}} >= $self->message_size()){
		    $self->rabbit->publish(1, $self->rabbit_queue, encode_json([$message]), {'exchange' => ''});
		    $message->{'meta'} = [];
		}
	    }

	    # Any leftover tasks, send that too
	    if (@{$message->{'meta'}} > 0){
		$self->rabbit->publish(1, $self->rabbit_queue, encode_json([$message]), {'exchange' => ''});
	    }
	}    
    }

    my $duration = tv_interval($start, [gettimeofday]);

    log_debug("All work messages sent to rabbit in $duration seconds");

    $start = [gettimeofday];

    # Now that we have generated all of the messages, we can go through and clear the flags 
    # on each of these data documents
    my $col_name = "data";
    if ($interval_from > 1){
	$col_name = "data_$interval_from";
    }   
    my $collection = $self->mongo->get_database($db)->get_collection($col_name);

    log_debug("Clearing updated flags for impacted docs in $db $col_name");

    my $result;
    try {
	$result = $collection->update_many({_id => {'$in' => \@clear_doc_ids}},
                                           {'$unset' => {'updated'       => 1,
                                                         'updated_start' => 1,
                                                         'updated_end'   => 1}});
    }
    catch {
	log_warn("Unable to clear updated flags on data docs: $_");	
    };

    return if (! $result);

    $duration = tv_interval($start, [gettimeofday]);
    log_debug("Cleared updated flags in $duration seconds");

    # We can go ahead and let go of all of our locks now
    $self->_release_locks();

    return 1;
}


sub _get_aggregate_policies {
    my ( $self ) = @_;

    my @db_names = $self->mongo->database_names;

    my %policies;

    foreach my $db_name (@db_names){

	# If we were given a specific database to work on, ignore other databases
	if (defined $self->force_database && $self->force_database ne $db_name){
	    log_debug("Skipping $db_name because we have a specific database of " . $self->force_database);
	    next;
	}

        my $cursor;
	my @docs;

	my $authorized = 1;
	try {
            $cursor = $self->mongo->get_database($db_name)->get_collection("aggregate")->find([]);
	    while (my $doc = $cursor->next()){

		if (! defined $doc->{'eval_position'} || ! defined $doc->{'interval'}){
		    log_warn("Skipping " . $doc->{'name'} . " due to missing eval position and/or interval");
		    next;
		}

		push(@docs, $doc);
	    }
        }
	catch {
	    if ($_ =~ /not authorized/){ 
		$authorized = 0;
	    }
	    else {
		log_warn("Error querying mongo: $_");		
	    }
        };

	# If we're unauthorized just skip ahead to the next database,
	# not really an error
	next if (! $authorized);

	# If we weren't able to get a cursor at all and it wasn't because
	# of unauthorized, we have a real error so abort
	return if (! $cursor);

	if (! @docs){
	    log_debug("No aggregate policies found for database $db_name");
	    next;
	}

 	$policies{$db_name} = \@docs;
    }

    log_debug("Found aggregate policies: " . Dumper(\%policies));

    return \%policies;
}

sub _connect {
    my ( $self) = @_;

    $self->_mongo_connect() or return;
    $self->_rabbit_connect() or return;
    $self->_redis_connect() or return;

    return 1;
}

sub _mongo_connect {
    my ( $self ) = @_;

    my $mongo_host = $self->config->get( '/config/master/mongo/host' );
    my $mongo_port = $self->config->get( '/config/master/mongo/port' );
    my $user       = $self->config->get( '/config/master/mongo/username' );
    my $pass       = $self->config->get( '/config/master/mongo/password' );

    log_debug( "Connecting to MongoDB as $user:$pass on $mongo_host:$mongo_port." );

    my $mongo;
    try {
        $mongo = MongoDB::MongoClient->new(
            host => "$mongo_host:$mongo_port",
            socket_timeout_ms => -1,
            username => $user,
            password => $pass
            );
    }
    catch {
        log_warn("Could not connect to Mongo: $_");
    };

    return if (! $mongo);

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

    my $success = 0;
    try {
        $rabbit->connect( $rabbit_host, $rabbit_args );
        $rabbit->channel_open( 1 );
        $rabbit->queue_declare( 1, $rabbit_queue, {'auto_delete' => 0} );
	$success = 1;
    }
    catch {
        log_warn("Unable to connect to RabbitMQ: $_");
    };

    return if (! $success);

    $self->_set_rabbit_queue($rabbit_queue);
    $self->_set_rabbit($rabbit);

    log_debug("Connected");

    return 1;
}

sub _redis_connect {
    my ( $self ) = @_;

    my $redis_host = $self->config->get( '/config/master/redis/host' );
    my $redis_port = $self->config->get( '/config/master/redis/port' );

    log_debug("Connecting to redis on $redis_host:$redis_port");

    my $redis = Redis->new( server => "$redis_host:$redis_port" );

    my $locker = Redis::DistLock->new( servers => [$redis],
                                       retry_count => 20,
				       retry_delay => 0.5);

    $self->_set_locker( $locker );

    log_debug("Connected");

    return 1;
}

# Given a database name, collection name, and a data document
# generates the Redis lock cache key in the same manner as the writer
# process to help them coordinate
sub _get_cache_key {
    my ( $self, $db, $col, $doc ) = @_;

    my $key = "lock__" . $db . "__" . $col;
    $key .=  "__" . $doc->{'identifier'};
    $key .=  "__" . $doc->{'start'};
    $key .=  "__" . $doc->{'end'};

    return $key;
}

sub _release_locks {
    my ( $self ) = @_;

    log_debug("Releasing " . scalar(@{$self->locks}) . " locks");

    foreach my $lock (@{$self->locks}){
	$self->locker->release($lock);
    }

    $self->_set_locks([]);

    return 1;
}

# Ensure that the "updated" index exists on the
# given db/col. We can't do a full table scan
# on huge data, that would cause much sadness
sub _verify_indexes {
    my ( $self, $db_name, $col_name ) = @_;

    my @indexes = $self->mongo()->get_database($db_name)->get_collection($col_name)->indexes()->list->all;

    my $found = 0;
    foreach my $index (@indexes){

	if ($index->{'key'}{'updated'} && $index->{'key'}{'identifier'}){
	    log_debug("Verified 'updated+identifier' index in $db_name $col_name");
	    $found++;
	}
	if ($index->{'key'}{'identifier'} && $index->{'key'}{'start'} && $index->{'key'}{'end'}){
	    log_debug("Verified 'identifier+start+end' index in $db_name $col_name");
	    $found++;
	}
    }

    if ($found != 2){
	log_warn("No 'updated+identifier' or 'identifier+start+end' index found in $db_name $col_name, skipping. This is an error!");
    }

    return $found == 2;
}


1;
