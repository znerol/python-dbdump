dbdump.py
=========

A python script simplifying partial backups of MySQL databases.

Introduction
------------
With web applications becoming more and more complex also the number of tables
in their database is increasing. Beside content and configuration, many of the
current web applications also store temporary or aggregated data into the
database. In order to save resources it may be desirable to exclude or separate
derived data. This can lead to dramatically reduced file sizes and faster
restores.

With dbdump.py the database schema and the data is dumped separately. While the
schema always includes all tables, the data-dump can be configured to include /
exclude certain tables.

Also part of the collection is a shell script, which can be run as CGI script
when cron is not available or limited to http requests at the hosting machine.

Usage
-----

```
usage: ./dbdump.py [-h] [-n NAME] [-i INCLUDES] [-e EXCLUDES] [-c] [-p] [-k KEEP] [-d DEFAULTS_FILE] [-v]
                   dumpdir database

positional arguments:
  dumpdir               The destination directory for database dumps
  database              The database to dump

optional arguments:
  -h, --help            show this help message and exit
  -n NAME, --name NAME  Base name of dumpfile. Defaults to name of database.
  -i INCLUDES, --include INCLUDES
                        Add table include pattern. May be specified multiple times.
  -e EXCLUDES, --exclude EXCLUDES
                        Add table exclude pattern. May be specified multiple times.
  -c, --compress        Compress backups using gzip
  -p, --prune           Prune old backups after dumping the database
  -k KEEP, --keep KEEP  Keep this number of backups in prune phase
  -d DEFAULTS_FILE, --defaults-file DEFAULTS_FILE
                        Path to MySQL defaults file
  -v, --verbose         Turn on verbose logging
```

License
-------

* [GPL-3 or later](https://www.gnu.org/licenses/gpl-3.0.en.html)

