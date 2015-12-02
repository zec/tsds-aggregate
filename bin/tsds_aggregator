#!/usr/bin/perl

use strict;
use warnings;

use GRNOC::TSDS::Aggregate::Aggregator;

use Getopt::Long;
use Data::Dumper;

### constants ###

use constant DEFAULT_CONFIG_FILE => '/etc/grnoc/tsds/aggregate/config.xml';
use constant DEFAULT_LOGGING_FILE => '/etc/grnoc/tsds/aggregate/logging.conf';

### command line options ###

my $config = DEFAULT_CONFIG_FILE;
my $logging = DEFAULT_LOGGING_FILE;
my $nofork;
my $help;

GetOptions( 'config=s' => \$config,
            'logging=s' => \$logging,
            'nofork' => \$nofork,
            'help|h|?' => \$help );

# did they ask for help?
usage() if $help;

# start/daemonize writer
my $aggregator = GRNOC::TSDS::Aggregate::Aggregator->new( config_file => $config,
                                                          logging_file => $logging,
                                                          daemonize => !$nofork );

$aggregator->start();

### helpers ###

sub usage {

    print "Usage: $0 [--config <file path>] [--logging <file path>] [--nofork]\n";

    exit( 1 );
}
