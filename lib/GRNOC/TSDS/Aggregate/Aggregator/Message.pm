package GRNOC::TSDS::Aggregate::Aggregator::Message;

use Moo;

use Types::Standard qw( Str ArrayRef );
use Types::XSD::Lite qw( PositiveInteger NonNegativeInteger );

### required attributres ###

has type => ( is => 'ro',
	      isa => Str,
	      required => 1 );

has interval_from => ( is => 'ro',
		       isa => PositiveInteger,
		       required => 1 );

has interval_to => ( is => 'ro',
		     isa => PositiveInteger,
		     required => 1 );

has start => ( is => 'ro',
	       isa => NonNegativeInteger,
	       required => 1 );

has end => ( is => 'ro',
	     isa => NonNegativeInteger,
	     required => 1 );

has meta => ( is => 'ro',
	      isa => ArrayRef[ Map[ Str, Str ] ],
	      required => 1 );

has values => ( is => 'ro',
		isa => ArrayRef[Str],
		required => 1 );

has required_fields => ( is => 'ro',
			 isa => ArrayRef[Str],
			 required => 1 );

1;
