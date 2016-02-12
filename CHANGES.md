## GRNOC TSDS Aggregate 1.0.0 -- Fri Feb 12 2016

### Features:
 * ISSUE=12464 Overhaul of TSDS data aggregation process. Instead of being bundled with the services library, this is now a standalone package to better enable horizontal distribution. Data retrieval now happens through the Query interface to enable aggregates from aggregates, and detection of what needs to be aggregated is more intelligent.