MyHoard [![Build Status](https://travis-ci.org/aiven/myhoard.png?branch=master)](https://travis-ci.org/aiven/myhoard)
=====================================================================================================================

MyHoard is a daemon for creating, managing and restoring MySQL backups.
The backup data can be stored in any of the supported cloud object storages.
It is functionally similar to [pghoard](https://github.com/aiven/pghoard)
backup daemon for PostgreSQL.

Features
========

* Automatic periodic full backup
* Automatic binary log backup in near real-time
* Cloud object storage support (AWS S3, Google Cloud Storage, Azure)
* Encryption and compression
* Backup restoration from object storage
* Point-in-time-recovery (PITR)
* Automatic backup history cleanup based on number of backups and/or backup age
* Purging local binary logs once they're backed up and not needed by other
  MySQL servers (requires external system to provide executed GTID info for the
  standby servers)
* Almost no extra local disk space requirements for creating and restoring
  backups

Fault-resilience and monitoring:

* Handles temporary object storage connectivity issues by retrying all
  operations
* Metrics via statsd using [Telegraf tag extensions](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd)
* Unexpected exception reporting via Sentry
* State reporting via HTTP API
* Full internal state stored on local file system to cope with process and
  server restarts

Overview
========

There are a number existing tools and scripts for managing MySQL backups so
why have yet another tool? As far as taking a full (or incremental) snapshot of
MySQL goes, [Percona XtraBackup](https://www.percona.com) does a very
good job and is in fact what MyHoard is using internally as well. Where things usually get
more complicated is when you want to back up and restore binary logs so that
you can do point-in-time recovery and reduce data loss window. Also, as good
as Percona XtraBackup is for taking and restoring the backup you still need
all sorts of scripts and timers added around it to actually execute it and if
anything goes wrong, e.g. because of network issues, it's up to you to retry.

Often binary log backup is based on just uploading the binary log files using
some simple scheduled file copying mechanism and restoring them is left as an
afterthought, usually just comprising of "download all the binlogs and then
use mysqlbinlog to replay them". In addition to not having proper automation
to do this to ensure it is repeatable and safe this approach also does not work
in some cases: In order for binary log restoration with mysqlbinlog to be safe
you need to have all binary logs on local disk. For change heavy environments
this may be much more than the size of the actual database and if server disk
is adjusted based on the database size the binary logs may simply not fit on
the disk.

MyHoard uses an alternative approach for binary log restoration, which is based
on presenting the backed up binary logs as relay logs in batches via direct
relay log index manipulation and having the regular SQL slave thread apply them
as if they were replicated from another MySQL server. This allows applying them
in batches so there's very little extra disk space required during restoration
and this would also allow applying them in parallel (though that requires more
work, currently there are known issues with using ``slave-parallel-workers``
value other than 0, i.e. multithreading must currently be disabled).

Existing tooling also doesn't pay much attention to real life HA environments
and failovers where the backup responsibilities need to be switched from one
server to another and getting uninterrupted sequence of backed up transactions
that can be restored to any point in time, including the time around the
failover. This requires something much more sophisticated than just blindly
uploading all local binary logs.

MyHoard aims to provide a single solution daemon that takes care of all of your
MySQL backup and restore needs. It handles creating, managing and restoring
backups in multi-node setups where master nodes may frequently be going away
(either because of rolling forward updates or actual server failure). You just
need to create a fairly simple configuration file, start the systemd service on
the master and any standby servers and make one or two HTTP requests to get the
daemon into correct state and it will start automatically doing the right
things.

Basic usage
===========

On the very first master after you've initialized MySQL database and started up
MyHoard you'd do this:

```
curl -XPUT -H "Content-Type: application/json" -d '{"mode": "active"}' \
  http://localhost:16001/status
```

This tells MyHoard to switch to active mode where it starts backing up data on
this server. If there are no existing backups it will immediately create the
first one.

On a new standby server you'd first install MySQL and MyHoard but not start or
initialize MySQL (i.e. don't do ``mysqld --initialize``). After starting the
MyHoard service you'd do this:

```
curl http://localhost:16001/backup  # lists all available backups
curl -XPUT -H "Content-Type: application/json" \
  -d '{"mode": "restore", "site": "mybackups", "stream_id": "backup_id", "target_time": null}' \
  http://localhost:16001/status
```

This tells MyHoard to fetch the given backup, restore it, start the MySQL
server once finished, and switch to observe mode where it keeps on observing
what backups are available and what transactions have been backed up but
doesn't do any backups itself. Because binary logging is expected to be
enabled also on the standby server MyHoard does take care of purging any local
binary logs that contain only transactions that have been backed up. If you
wanted to restore to a specific point in time you'd just give a timestamp like
`"2019-05-22T11:19:02Z"` and restoration will be performed up until the last
transaction before the target time.

If the master server fails for any reason you'd do this on one of the standby
servers:

```
curl -XPUT -H "Content-Type: application/json" -d '{"mode": "promote"}' \
  http://localhost:16001/status
```

This updates the object storage to indicate this server is now the master and
any updates from the old master should be ignored by any other MyHoard
instances. (The old master could still be alive at this point but e.g.
responding so slowly that it is considered to be unavailable yet it might be
able to accept writes and back those up before going totally away and those
transactions must be ignored when restoring backups in the future because they
have not been replicated to the new master server.) After the initial object
storage state update is complete MyHoard switches itself to ``active`` mode and
resumes uploading binary logs to the currently active backup stream starting
from the first binary log that contains transactions that have not yet been
backed up.

Requirements
============

MyHoard requires Python 3.6 or later and some additional components to operate:

* [percona-xtrabackup](https://github.com/percona/percona-xtrabackup)
* [pghoard](https://github.com/aiven/pghoard)
* [python3-PyMySQL](https://github.com/PyMySQL/PyMySQL)
* [MySQL server 8.x+](https://www.oracle.com/mysql/)

Currently MyHoard only works on Linux and expects MySQL service to be managed
via systemd.

MyHoard requires MySQL to be used and configured in a specific manner in order
for it to work properly:

* Single writable master, N read only standbys
* Binary logging enabled both on master and on standbys
* ``binlog_format`` set to ``ROW``
* Global transaction identifiers (GTIDs) enabled
* Use of only InnoDB databases

Configuration options
=====================

``myhoard.json`` has an example configuration that shows the structure of the
config file and has reasonable default values for many of the settings. Below
is full list the settings and the effect of each.

**backup_settings.backup_age_days_max**

Maximum age of backups. Any backup that has been closed (marked as final with
no more binary logs being uploaded to it) more than this number of days ago
will be deleted from storage, unless total number of backups is below the
minimum number of backups.

**backup_settings.backup_count_max**

Maximum number of backups to keep. Because new backups can be requested
manually it is possible to end up with a large number backups. If the total
number goes above this backups will be deleted even if they are not older than
``backup_age_days_max`` days.

**backup_settings.backup_count_min**

Minimum number of backups to keep. If for example the server is powered off and
then back on a month later, all existing backups would be very old. However,
in that case it is usually not desirable to immediately delete all old backups.
This setting allows specifying a minimum number of backups that should always
be preserved regardless of their age.

**backup_hour**

The hour of day at which to take new full backup. If backup interval is less
than 24 hours this is used as base for calculating the backup times. E.g. if
backup interval was 6 hours and backup hour was 4, backups would be taken at
hours 4, 10, 16 and 22.

**backup_interval_minutes**

The interval in minutes at which to take new backups. Individual binary logs
are backed up as soon as they're created so there's usually no need to have
very frequent full backups.

**backup_minute**

The minute of hour at which to take new full backup.

**forced_binlog_rotation_interval**

How frequently, in seconds, to force creation of new binary log if one hasn't
been created otherwise. This setting ensures that environments with low rate of
changes so that new binary logs are not created because the size limit is
exceeded also get all data backed up frequently enough.

**upload_site**

Name of the backup site to which new backups should be created to. See
``backup_sites`` for more information. Only needs to be defined if multiple
non-recovery backup sites are present.

**backup_sites**

Object storage configurations and encryption keys. This is an object with
``"site_name": {<site_config>}`` entries. Typically there is only a single
backup site but in cases where new server needs to fetch a backup from a
different location than where it should start writing its own backups there
could be two. The backup site name has no relevance for MyHoard and you can
pick whatever names you like.

Each site has the following configuration options:

**backup_sites.{name}.compression**

The compression method and option to use for binary logs uploaded to this
site:

**backup_sites.{name}.compression.algorithm**

One of the supported compression algorithms: ``lzma``, ``snappy``, ``zstd``.
Defaults to ``snappy``.

**backup_sites.{name}.compression.level**

Compression level for ``lzma`` or ``zstd``.

**backup_sites.{name}.encryption_keys**

This is an object containing two keys, ``public`` and ``private``. These define
the RSA master key used for encrypting/decrypting individual encryption keys
used for actual data encryption/decryption. The values must be in PEM format.

**backup_sites.{name}.object_storage**

The object storage configuration for this backup site. Please refer to the
[PGHoard readme](https://github.com/aiven/pghoard) for details regarding these
settings.

**backup_sites.{name}.recovery_only**

This site is not considered for new backups, it can only be used for recovering
an existing backup.

**binlog_purge_settings.enabled**

If ``true`` MyHoard purges binary logs that are no longer required. The
recommended configuration is to have MySQL keep binary logs around for a
longish period of time (several days) but have this setting enabled so that
MyHoard removes the binary logs as soon as they aren't needed anymore.

Note that in order to consider replication to other MySQL servers that are
part of the cluster the ``PUT /replication_state`` API must be used to
periodically tell MyHoard what transactions other cluster nodes have applied
to avoid MyHoard purging binary logs that haven't been replicated. This state
update is not strictly required since MySQL will not allow purging binary logs
that connected standbys don't yet have but in case standbys get temporarily
disconnected relevant binlogs could get purged. Also, in case a standby is
promoted as new master it should still have any binary log that any other
standby hasn't yet received so that when the other standbys start to replicate
from the standby that gets promoted as master they are able to get in sync.
This requires MyHoard on the standby to know what transactions other standbys
have applied.

**binlog_purge_settings.min_binlog_age_before_purge**

Minimum age of a binary log before purging it is considered. It is advisable to
set this to some minutes to avoid any race conditions where e.g. new standby
joins the cluster but MyHoard hasn't yet been informed of its replication
position and could end up purging binary log that the new standby could end up
needing.

**binlog_purge_settings.purge_interval**

Number of seconds between checks for binary logs that could be removed.

**binlog_purge_settings.purge_when_observe_no_streams**

Allow purging binary logs when no active backups exist. This setting is mostly
relevant for detached read-only replicas that are not configured to take
backups of their own. In this case the read replica would see no active backups
because the backup site used by the source service has been specified as
recovery only site for the replica. For any other nodes than detached read-only
replicas this setting can be set to ``false``, for the replicas this should be
``true`` or else MyHoard cannot do any purging at all.

**http_address**

The IP address to which MyHoard binds its HTTP server. It is highly recommended
to use local loopback address. MyHoard provides no authentication or TLS
support for the HTTP requests and they should only be made from localhost
unless you use something like HAProxy to add authentication and encryption.

**http_port**

The TCP port to which MyHoard binds its HTTP server.

**mysql.binlog_prefix**

The full path and file name prefix of binary logs. This must be the same as the
corresponding MySQL configuration option except full path is always required
here.

**mysql.client_params**

The parameters MyHoard uses to connect to MySQL. Because MyHoard needs to
perform certain low level operations like manually patch GTID executed value in
``mysql.gtid_executed`` table while restoring data the user account must have
high level of privileges.

**mysql.config_file_name**

Full path of the MySQL server configuration file. This is passed to Percona
XtraBackup while creating or restoring full database snapshot.

**mysql.data_directory**

Full path of the MySQL data directory. This is currently only used for getting
file sizes for reporting and progress tracking purposes.

**mysql.relay_log_index_file**

Full path of the MySQL relay log index file. This must be the same as the
corresponding MySQL configuration option except full path is always required
here.

**mysql.relay_log_prefix**

The full path and file name prefix of relay logs. This must be the same as the
corresponding MySQL configuration option except full path is always required
here.

**restore_max_binlog_bytes**

Maximum amount of disk space to use for binary logs including pre-fetched logs
that are not yet being applied and the binary logs that MySQL is currently
applying. When Percona XtraBackup is restoring the full database snapshot
MyHoard starts prefetching binary logs that are needed on top of the full
snapshot. Up to this number of bytes are fetched. When the snapshot has been
restored MyHoard tells MySQL to start applying the binary logs it has managed
to pre-fetch thus far and keep on pre-fetching files as long as the total size
of pre-fetched and files being applied is below the limit (files are deleted as
soon as they have been fully applied).

This should be set to something like 1% of all disk space or at least a few
hundred MiBs (depending on individual binary log max size] so that sufficient
number of binary logs can be fetched.

**sentry_dsn**

Set this value to Sentry Data Source Name (DSN) string to have any unexpected
exceptions sent to Sentry.

**server_id**

Server identifier of this node (integer in the range 1 - 0x7FFFFFFF). This must
match the corresponding MySQL configuration option for this node.

**start_command**

Command used for starting MySQL. This is mostly used for tests. In any
production setups using systemd to manage MySQL server daemon is highly
recommended.

**state_directory**

Directory where to store MyHoard state files. MyHoard stores its full state
into a number of separate JSON files.

**statsd**

Statsd configuration used for sending metrics data points. The implementation
will send tags along the data points so Telegraf Statsd or other similar
implementation that can handle the tags is expected.

The tags specified here are also reused for Sentry.

**systemctl_command**

The ``systemctl`` base command to invoke when MyHoard needs to start or stop
the MySQL server. This is only used when restoring a backup where MySQL needs
to be started after full database snapshot has been recovered and restarted
a couple of times with slightly different settings to allow patching GTID
executed information appropriately.

**systemd_env_update_command**

A command to invoke before ``systemctl`` to configure MySQL server to use the
desired configuration options. This is typically just the built-in
``myhoard_mysql_env_update`` command that writes to MySQL systemd environment
file. Separate command is needed to allow running the update as root user.

**systemd_service**

Name of the MySQL systemd service.

**temporary_directory**

Temporary directory to use for backup and restore operations. This is currently
not used directly by MyHoard but instead passed on to Percona XtraBackup. It is
recommended not to use ``/tmp`` for this because that is an in-memory file
system on many distributions and the exact space requirements for this
directory are not well defined.

HTTP API
========

MyHoard provides an HTTP API for managing the service. The various entry points
are described here.

All APIs return a response like this on error:

```
{
  "message": "details regarding what went wrong"
}
```


GET /backup
-----------

Lists all available backups. This call takes no request parameters. Response
format is as follows:

```
{
  "backups": [
    {
      "basebackup_info": {
        ...
      },
      "closed_at": "2019-05-23T06:29:10.041489Z",
      "completed_at": "2019-05-22T06:29:20.582302Z",
      "recovery_site": false,
      "resumable": true,
      "site": "{backup_site_name}",
      "stream_id": "{backup_identifier}"
    }
  ]
}
```

In case MyHoard has not yet finished fetching the list of backups the root
level ``"backups"`` value will be ``null``.

**basebackup_info**

This contains various details regarding the full snapshot that is the basis of
this backup.

**closed_at**

The time at which last binary log to this stream was uploaded and no more
uploads are expected. The backup can resume to any point in time between
``closed_at`` and ``completed_at``. If ``closed_at`` is ``null`` the backup is
active and new binary logs are still being uploaded to it.

**recovery_site**

Tells whether the backup site this backup is stored into is recovery only.

**resumable**

Tells whether the backup is in a state that another server can continue backing
up data to it. When current master server starts a new backup it first needs to
upload the initial full snapshot and some associated metadata. Before that is
done no other server could possibly do anything useful with that backup. Once
these steps are completed then if the master fails for any reason and one of
the standbys are promoted as new master the newly promoted master can continue
uploading binary logs to the existing active backup. If the backup is not
resumable the new master needs to discard it and start new backup from scratch.

**site**

Name of the backup site this backup is stored into.

**stream_id**

Identifier of this backup.

POST /backup
------------

Create new full backup or force binary log rotation and back up the latest
binary log file. Request body must be like this:

```
{
  "backup_type": "{basebackup|binlog}",
  "wait_for_upload": 3.0
}
```

**backup_type**

This specifies the kind of backup to perform. If set to ``basebackup`` a new
full backup is created and old backup is closed once that is complete. If set
to ``binlog`` this rotates currently active binary log so that a finished
binary log file with all current transactions is created and that file is then
backed up.

**wait_for_upload**

This is only valid in case ``backup_type`` is ``binlog``. In that case the
operation will block for up to as many seconds as specified by this parameter
for the binary log upload to complete before returning.

Response on success looks like this:

```
{
  "success": true
}
```

PUT /replication_state
----------------------

This call can be used to inform MyHoard of the executed GTIDs on other servers
in the cluster to allow MyHoard to only purge binlogs that have been fully
applied on all cluster nodes. Request body must be like this:

```
{
  "server1": {
    "206375d9-ec5a-46b7-bb26-b621812e7471": [[1, 100]],
    "131a1f4d-fb7a-44fe-94b9-5508445aa126": [[1, 5858]],
  },
  "server2": {
    "206375d9-ec5a-46b7-bb26-b621812e7471": [[1, 100]],
    "131a1f4d-fb7a-44fe-94b9-5508445aa126": [[1, 5710]],
  }
}
```

The main level object must have an entry for each server in the cluster. The
names of the servers are not relevant as long as they are used consistently.
For each of the servers the value is an object that contains server UUID as the
key and list of GNO start end ranges as the value. The server UUID is the
original server from which the transactions originated, not the UUID of the
server reporting the numbers. In the example both servers ``server1`` and
``server2`` have executed transactions 206375d9-ec5a-46b7-bb26-b621812e7471:1-100
and both have executed some part of transactions from server
131a1f4d-fb7a-44fe-94b9-5508445aa126 but ``server1`` is further ahead, having
executed GNOs up until 5858 while ``server2`` is only at 5710.

Note that it is not expected to have multiple masters active at the same time.
Multiple server UUIDs exist when old master servers have been replaced.

Response on success echoes back the same data sent in the request.

GET /status
-----------

Returns current main mode of MyHoard. Response looks like this:

```
{
  "mode": "{active|idle|observe|promote|restore}"
}
```

See the status update API for more information regarding the different modes.

PUT /status
-----------

Updates current main mode of MyHoard. Request must be like this:

```
{
  "force": false,
  "mode": "{active|idle|observe|promote|restore}",
  "site": "{site_name}",
  "stream_id": "{backup_id}",
  "target_time": null,
  "target_time_approximate_ok": false
}
```

**force**

This value can only be passed when switching mode to active.

When binary log restoration is ongoing setting this true will cause mode to
be forcibly switched to promote without waiting for all binary logs to get
applied and the promote phase will skip the step of ensuring all binary logs
are applied. If mode is already promote and binary logs are being applied in
that state, the binary logs sync is considered to be immediately complete.
If the server is not currently applying binary logs passing ``"force": true``
will cause the operation to fail with error 400.

This parameter is only intended for exceptional situations. For example
broken binary logs that cannot be applied and it is preferable to promote the
server in it's current state. Another possible case is binary logs containing
changes to large tables without primary keys with row format in use and the
operation being so slow that it will not complete in reasonable amount of time.
Data loss will incur when using this option!

**mode**

MyHoard initially starts in mode ``idle``. In this state it only fetches the
available backups but doesn't actively do anything else. From ``idle`` state it
is possible to switch to ``restore`` or ``active`` states. Only the very first
server in a new cluster should be switched to ``active`` state directly from
``idle`` state. All other servers must first be switched to ``restore`` and
only after restoration has finished should other state changes be performed.

When restore operation completes MyHoard automatically transitions to mode
``observe``, in which it keeps track of backups managed by other servers in the
cluster but doesn't actively back up anything. If this node should be the new
master (or new separate forked service) then mode must be switched to
``promote`` once MyHoard has changed it from ``restore`` to ``observe``. This
will make MyHoard update metadata in object storage appropriately before
automatically transitioning to state ``active``.

Servers in state ``active`` cannot be transitioned to other states. They are
the active master node and MyHoard on the node must just be deactivated if the
server should stop acting in that role.

**site**

This is only applicable when new mode is ``restore``. Identifies the site
containing the backup to restore. Use ``GET /backup`` to list all available
backups.

**stream_id**

This is only applicable when new mode is ``restore``. Identifies the backup
to restore. Use ``GET /backup`` to list all available backups.

**target_time**

This is only applicable when new mode is ``restore``. If this is omitted or
``null`` the last available transaction for the given backup is restored. When
this is defined restoration is performed up until the last transaction before
this time. Must be ISO 8601 timestamp. If the requested time is not available
in the given timestamp (time is not between the ``completed_at`` and
``closed_at`` timestamps) the request will fail.

**target_time_approximate_ok**

This is only applicable when new mode is ``restore`` and ``target_time`` has
been specified. If this is set to ``true`` then ``target_time`` is only used to
restrict results on individual binary log level. That is, the restore process
is guaranteed not to restore binary logs whose first transaction is later than
the given target time but the last file that is picked for restoration is fully
applied even if that means applying some transactions that are more recent than
the target time.

This mode is useful when restoring potentially large number of binary logs and
the exact target time is not relevant. Enabling this mode avoids having to use
the ``UNTIL SQL_AFTER_GTIDS = x`` parameter for the SQL thread. The ``UNTIL``
modifier forces single threaded apply and on multi-core machines makes the
restoration slower. The single threaded mode only applies for the last batch
but that too can be very large and setting this value can significantly reduce
the restoration time.

GET /status/restore
-------------------

If current mode is ``restore`` this API can be used to get details regarding
restore progress. If mode is something else the request will fail with HTTP
status 400. For successful requests the response body looks like this:

```
{
  "basebackup_compressed_bytes_downloaded": 8489392354,
  "basebackup_compressed_bytes_total": 37458729461,
  "binlogs_being_restored": 0,
  "binlogs_pending": 73,
  "binlogs_restored": 0,
  "phase": "{current_phase}"
}
```

**basebackup_compressed_bytes_downloaded**

Number of compressed bytes of the full snapshot that have been downloaded so
far. Decryption and decompression is performed on the fly so these bytes have
also been processed.

**basebackup_compressed_bytes_total**

Total number of (compressed) bytes in the full snapshot.

**binlogs_being_restored**

Number of binary logs currently passed on to MySQL to restore.

**binlogs_pending**

Binary logs that are pending to be restored. Note that this number may be
going up if there is currently an active master node that is uploading new
binary logs to the backup being restored and there is no recovery target time
given.

**binlogs_restored**

Number of binary logs that have been successfully applied.

**phase**

Current phase of backup restoration. Possible options are these:

* getting_backup_info: Backup metadata is being fetched to determine what
  exactly needs to be restored.
* initiating_binlog_downloads: Binary log prefetch operations are being
  scheduled so that progress with those can be made while the full snapshot is
  being restored.
* restoring_basebackup: The full snapshot is being downloaded and prepared.
* refreshing_binlogs: Refreshing binary log info to see if new binary logs have
  been uploaded to object storage from current master. This and the other
  binlog related phases are typically entered multiple times as the binlogs are
  handled in batches.
* applying_binlogs: Refreshing the list of binary logs MySQL should be
  restoring.
* waiting_for_apply_to_finish: Waiting for MySQL to finish applying current
  subset of binary logs.
* finalizing: Performing final steps to complete backup restoration.
* completed: The operation has completed. This is typically not returned via
  the API because MyHoard will automatically switch to ``observe`` mode when
  restoration completes and the restoration status is not available in that
  mode.
* failed: Restoring the backup failed. The operation will be retried
  automatically but it may fail repeatedly and analyzing logs to get more
  details regarding the failure is advisable.

License
=======

MyHoard is licensed under the Apache License, Version 2.0. Full license text
is available in the ``LICENSE`` file and at
http://www.apache.org/licenses/LICENSE-2.0.txt


Contact
=======

Bug reports and patches are very welcome, please post them as GitHub issues
and pull requests at https://github.com/aiven/myhoard. Any possible
vulnerabilities or other serious issues should be reported directly to the
maintainers <opensource@aiven.io>.

Credits
=======

MyHoard was created by, and is maintained by, [Aiven](https://aiven.io) cloud
data hub developers.

Recent contributors are listed on the GitHub project page,
https://github.com/aiven/myhoard/graphs/contributors

MyHoard uses [Percona Xtrabackup](https://www.percona.com) for creating and
restoring database snapshot excluding binary logs.

Copyright ⓒ 2019 Aiven Ltd.
