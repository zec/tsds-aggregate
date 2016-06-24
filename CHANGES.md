## GRNOC TSDS Aggregate 1.0.3 -- Fri Jun 24 2016

### Features:
* ISSUE=530 Added knob to control maximum amount of data fetched in a single
block. This allows better control over server behavior and should help with situations
like data backfill from choking up the server.


### Fixes:
* ISSUE=821 Slight change in architecture to fork a new temporary process that does
the actual work to work around perl memory management issues. Impact should be that
while idling until next run considerably less memory should be held onto.


## GRNOC TSDS Aggregate 1.0.2 -- Tues Apr 19 2016

### Features:
* ISSUE=708 Fixed edge case where writer would complain about aggregate messages with a histogram width
with a floating point value. This was only seen in cases where data variances were very small, such
as in optical readings.


## GRNOC TSDS Aggregate 1.0.1 -- Fri Mar 04 2016

### Features:
 * ISSUE=13132 Fixed issue where upon first install the daemon would make the determination that everything needed aggregation.


## GRNOC TSDS Aggregate 1.0.0 -- Fri Feb 12 2016

### Features:
 * ISSUE=12464 Overhaul of TSDS data aggregation process. Instead of being bundled with the services library, this is now a standalone package to better enable horizontal distribution. Data retrieval now happens through the Query interface to enable aggregates from aggregates, and detection of what needs to be aggregated is more intelligent.