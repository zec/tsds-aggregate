# TSDS Aggregate Install Guide

This document covers installing the TSDS aggregation process on a new machine. Steps should be followed below in order unless you know for sure what you are doing. This document assumes a RedHat Linux environment or one of its derivatives.

## Installation

Installing these packages is just a yum command away. Nothing will automatically start after installation as we need to move on to configuration.

```
[root@tsds ~]# yum install grnoc-tsds-aggregate
```


## tsds-aggregate-daemon

This is the process that determines what needs to be aggregated at any given time. It is a daemon that polls the various TSDS databases, examines their aggregation policies, detects what data needs to be aggregated, and disperses "work orders" via rabitmq to a pool of workers that handle actually crunching the data.

IMPORTANT - only one of these must be running per TSDS cluster. If you are installing additional workers to an existing cluster, skip ahead to the tsds-aggregate-workers section.

The configuration file and logging file are listed below:

```
/etc/grnoc/tsds/aggregate/config.xml
/etc/grnoc/tsds/aggregate/logging.conf
```

Leaving the logging at the defaults is fine for most installations. Change this if you need more information from the system. Modifying the configuration file is required.

Update the "<master>" section so that it has the correct MongoDB connection information. If installing directly on the same system where mongo is running, you will likely need to only edit the username and password.

Update the "<redis>" section so that it has the correct Redis connection information for the Redis installation in the grnoc-tsds-services. If installing directly on the same system where mongo is running, you may not need to change anything here.

Update the "<rabbit>" section so that it has the correct RabbitMQ connection information. If installing directly on the same system where RabbitMQ is running, you may not need to change anything here. Please note that the RabbitMQ pending-queue and finished-queue values must agree between the daemon and any workers.

Start the daemon with:

```
/etc/init.d/tsds-aggregate-daemon start
```

and verify that it is running correctly by looking at the output of `ps`. If there are errors, they should appear in /var/log/messages. Alternatively, try running it manually in the foreground by:

```
[root@tsds ~]# /usr/bin/tsds-aggregate --nofork
```

Assuming things are running correctly, you should be able to see information about aggregate data work orders being sent out in /var/log/messages for various timeframes. Depending on when this is started it may take some time - for example an hourly aggregation policy is only examined once an hour.


## tsds-aggregate-workers

The workers are processes that read "work order" messages from the tsds-aggregate-daemon process off the configured RabbitMQ queue. It will query data from TSDS, crunch it into the aggregate calculation, and then send the final result off as another rabbit message to the tsds-writer process for actual writing into MongoDB.

Since all interaction happens with RabbitMQ and webservices, these workers may be turned up on any host that has access in order to spread around the CPU load. The logging and the configuration files are shared with the tsds-aggregate-daemon described above.

```
/etc/grnoc/tsds/aggregate/config.xml
/etc/grnoc/tsds/aggregate/logging.conf
```

The "<worker>" and "<rabbit>" sections will need to updated to reflect real values. For "<worker>" the num processes can be tuned to the needs and capabilities of the system, and the credentials to talk to the TSDS webservices must be configured properly.