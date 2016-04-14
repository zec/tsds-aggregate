package GRNOC::TSDS::Aggregate::Aggregator::Message;

use Moo;

use Types::Standard qw( Str Num ArrayRef Map Dict Maybe );
use Types::Common::Numeric qw( PositiveInt PositiveOrZeroInt PositiveNum );

### required attributes ###

has type => ( is => 'ro',
              isa => Str,
              required => 1 );

has interval_from => ( is => 'ro',
                       isa => PositiveInt,
                       required => 1 );

has interval_to => ( is => 'ro',
                     isa => PositiveInt,
                     required => 1 );

has start => ( is => 'ro',
               isa => PositiveOrZeroInt,
               required => 1 );

has end => ( is => 'ro',
             isa => PositiveOrZeroInt,
             required => 1 );

has meta => ( is => 'ro',
              isa => ArrayRef[ Dict[ 'fields' => Map[ Str, Str ],
				     'values' => ArrayRef[ Dict[ 'name' => Str,
								 'min' => Maybe[Num],
								 'max' => Maybe[Num] ] ] ] ],

              required => 1 );

has values => ( is => 'ro',
                isa => ArrayRef[ Dict[ 'name' => Str,
                                       'hist_res' => Maybe[PositiveNum],
                                       'hist_min_width' => Maybe[PositiveNum] ] ],
                required => 1 );

has required_meta => ( is => 'ro',
                       isa => ArrayRef[Str],
                       required => 1 );

1;
