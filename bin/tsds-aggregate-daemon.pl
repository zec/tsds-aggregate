#!/usr/bin/perl

use strict;
use warnings;

use GRNOC::TSDS::Aggregate::Daemon;
use JSON::XS;

use GRNOC::Log;
use Getopt::Long;
use Data::Dumper;

### constants ###

use constant DEFAULT_CONFIG_FILE => '/etc/grnoc/tsds-aggregate/config.xml';
use constant DEFAULT_LOGGING_FILE => '/etc/grnoc/tsds-aggregate/logging.conf';
use constant DEFAULT_LOCK_DIR => '/var/run/grnoc/tsds-aggregate/';

### command line options ###

my $help;
my $config = DEFAULT_CONFIG_FILE;
my $logging = DEFAULT_LOGGING_FILE;
my $lock_dir = DEFAULT_LOCK_DIR;
my $nofork;
my $database;
my $start;
my $end;
my $policy;
my $query;

# parse options from command line
GetOptions( "help|h|?"    => \$help,
            "config=s"    => \$config,
            "logging=s"   => \$logging,
            "lock-dir=s"  => \$lock_dir,
            "nofork"      => \$nofork,
	    "database=s"  => \$database,
	    "start=s"     => \$start,
	    "end=s"       => \$end,
	    "policy=s"    => \$policy,
	    "query=s"     => \$query,
           ) or die;

# did they ask for help?
usage() if $help;

GRNOC::Log->new(config => $logging);

if (defined $start && ! defined $end){
    my $now = time();
    log_info("Setting end to $now because start given but not end");
    $end = $now; 
}

if (defined $end && ! defined $start){
    print "end requires start, aborting\n";
    usage();
}

if (defined $policy && ! defined $database){
    print "policy requires database, aborting\n";
    usage();
}

if (defined $query && ! defined $database){
    print "query requires database, aborting\n";
    usage();
}

if (defined $start){
    log_info("Not daemonizing due to given start and/or end");
    $nofork = 1;
}

my $aggregator = GRNOC::TSDS::Aggregate::Daemon->new(config_file     => $config,
						     lock_dir        => $lock_dir,
						     force_database  => $database,
						     force_policy    => $policy,
						     force_query     => $query,
						     force_start     => $start,
						     force_end       => $end,
						     daemonize       => !$nofork);

$aggregator->start();                                    

sub usage {
    print "Usage: $0 
                     [--config <path>] 
                     [--logging <path>]
                     [--lock-dir <path>]
                     [--start <epoch timestamp>] - implies --nofork, 
                     [--end <epoch timestamp>] - implies --nofork, requires --start, implied \"now\" if --start given but not --end                
                     [--database <only run on this database name>]
                     [--policy <only run on this policy name>] - requires --database
                     [--query <stringified JSON, gets AND'd into the base policy query>] - requires --database
                     [--nofork]
                     [--help]\n";
    exit(1);
}
