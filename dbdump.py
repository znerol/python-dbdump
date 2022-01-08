#!/usr/bin/env python3

import argparse
import contextlib
import fnmatch
import glob
import gzip
import logging
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, Optional, Sequence

log = logging.getLogger("mysqldumpy")


def match_any(subject: str, patterns: Sequence[str]) -> bool:
    """
    Use fnmatch to test all patterns against subject. Return true if any matches.
    """
    for pattern in patterns:
        if fnmatch.fnmatch(subject, pattern):
            return True

    return False


class FilesystemRepository:
    """
    Represents a backup repository on the filesystem.
    """
    name: str
    dumpdir: Path
    compress: bool
    dateformat: str
    part: Optional[str]
    log = logging.getLogger("FilesystemRepository")

    def __init__(
        self,
        name: str,
        dumpdir: str,
        compress: bool,
        dateformat: str = "%Y%m%dT%H%M%S%z",
        part: Optional[str] = None,
    ):
        self.name = name
        self.dumpdir = Path(dumpdir)
        self.compress = compress
        self.dateformat = dateformat
        self.part = part

    def prefix(self) -> str:
        """
        Return path prefix for dumps in this repository.
        """
        return str(self.dumpdir.joinpath(f"{self.name}-"))

    def suffix(self) -> str:
        """
        Return path suffix for dumps in this repository.
        """
        partstr = f"-{self.part}" if self.part else ""
        extension = ".sql.gz" if self.compress else ".sql"
        return f"{partstr}{extension}"

    def index(self) -> Sequence[Path]:
        """
        List existing dumps in this repository.
        """
        pattern = f"{self.prefix()}*{self.suffix()}"
        candidates = sorted(glob.glob(pattern))
        return [Path(p) for p in candidates if Path(p).is_file()]

    def filepath(self, datestamp: Optional[datetime] = None) -> Path:
        """
        Return path to dump for the given datestamp.
        """
        if datestamp is None:
            datestamp = datetime.now()

        datestring = datestamp.strftime(self.dateformat)

        return Path(f"{self.prefix()}{datestring}{self.suffix()}")

    @contextlib.contextmanager
    def open(self, datestamp: Optional[datetime] = None):
        """
        Create new dumpfile and return a writable stream.

        Note: Returned object is a contextmanager. I.e.:

            with repository.open() as outstream:
                # do stuff here
        """
        outpath = self.filepath(datestamp)

        with tempfile.NamedTemporaryFile(
            dir=self.dumpdir,
            delete=False,
            mode="wb",
        ) as outtemp:
            if self.compress:
                with gzip.GzipFile(
                    filename=outpath,
                    fileobj=outtemp,
                    mode="wb"
                ) as outcompress:
                    yield outcompress
            else:
                yield outtemp

        Path(outtemp.name).rename(outpath)
        self.log.info("Dumped %s", outpath)

    def prune(self, keep: int):
        """
        Prune repository, keep specified number of dump files.
        """
        files = self.index()
        numprune = len(files)-keep

        if numprune > 0:
            self.log.debug("Start pruning %i out of %i files in dir %s",
                           numprune, len(files), self.dumpdir)

            for path in files[:-keep]:
                path.unlink()
                self.log.info("Pruned %s", path)

            self.log.debug("Finished pruning %i out of %i files in dir %s",
                           numprune, len(files), self.dumpdir)


class MySQLSource:
    """
    Represents a MySQL / MariaDB database source.
    """
    database: str
    defaults_file: Optional[str]
    mysql_executable: str = "mysql"
    mysqldump_executable: str = "mysqldump"
    log = logging.getLogger("MySQLSource")

    def __init__(self, database: str, defaults_file: Optional[str] = None):
        self.database = database
        self.defaults_file = defaults_file

    def tables(
        self,
        includes: Sequence[str],
        excludes: Sequence[str]
    ) -> Sequence[str]:
        """
        List available tables in given database.
        """
        self.log.debug("Listing tables in database %s", self.database)

        cmd = [self.mysql_executable] + self.args(
            "--execute=SHOW TABLES",
            "--batch",
            "--skip-column-names",
            self.database
        )
        tables = subprocess.check_output(cmd, text=True).splitlines()

        self.log.debug("Found %i tables in database %s",
                       len(tables), self.database)

        selection = [
            table for table in tables
            if match_any(table, includes) and not match_any(table, excludes)
        ]

        self.log.debug("Selected %i tables from database %s",
                       len(selection), self.database)

        return selection

    def dump(
        self,
        outstream: BinaryIO,
        tables: Sequence[str],
        no_data: Optional[bool],
    ):
        """
        Run mysqldump, stream output to binary outstream.
        """
        self.log.debug("Start dumping %i tables from database %s",
                       len(tables), self.database)

        flags = ["--no-data"] if no_data else []
        cmd = [self.mysqldump_executable] + self.args(
            *flags,
            self.database,
            *tables
        )

        with subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=None
        ) as process:
            try:
                shutil.copyfileobj(process.stdout, outstream)
                retcode = process.wait()
            except:
                process.kill()
                raise

            if retcode:
                raise subprocess.CalledProcessError(retcode, cmd[0])

        self.log.debug("Finished dumping %i tables from database %s",
                       len(tables), self.database)

    def args(self, *args):
        """
        Construct list of arguments suitable for subprocess.run
        """
        defaults_args = [] if self.defaults_file is None else [
            f"--defaults-file={self.defaults_file}",
        ]
        return defaults_args + list(args)


def backup(
    repository: FilesystemRepository,
    source: MySQLSource,
    includes: Sequence[str],
    excludes: Sequence[str],
    no_data: bool,
    datestamp: datetime = None,
):
    """
    Run one backup job
    """
    tables = source.tables(includes, excludes)

    with repository.open(datestamp) as outstream:
        source.dump(outstream, tables, no_data)


def main(prog, args):
    """
    mysqldumpy main procedure
    """
    parser = argparse.ArgumentParser(prog=prog)

    parser.add_argument(
        "-n", "--name", type=str, dest="name",
        help="Base name of dumpfile. Defaults to name of database."
    )
    parser.add_argument(
        "-i", "--include", action="append", type=str, dest="includes",
        help="Add table include pattern. May be specified multiple times."
    )
    parser.add_argument(
        "-e", "--exclude", action="append", type=str, dest="excludes",
        help="Add table exclude pattern. May be specified multiple times."
    )
    parser.add_argument(
        "-c", "--compress", action="store_true", dest="compress",
        help="Compress backups using gzip",
    )
    parser.add_argument(
        "-p", "--prune", action="store_true", dest="prune",
        help="Prune old backups after dumping the database",
    )
    parser.add_argument(
        "-k", "--keep", type=int, dest="keep", default=None,
        help="Keep this number of backups in prune phase",
    )
    parser.add_argument(
        "-d", "--defaults-file", type=str, dest="defaults_file",
        help="Path to MySQL defaults file",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose",
        help="Turn on verbose logging"
    )
    parser.add_argument(
        "dumpdir", type=str,
        help="The destination directory for database dumps"
    )
    parser.add_argument(
        "database", type=str,
        help="The database to dump",
    )

    options = parser.parse_args(args)

    level = logging.DEBUG if options.verbose else logging.INFO
    logging.basicConfig(level=level)

    # Keep must be specified if prune is set
    if options.prune and options.keep is None:
        parser.print_usage()
        sys.exit(1)

    database = options.database
    name = options.name or database
    includes = options.includes or ["*"]
    excludes = options.excludes or []

    source = MySQLSource(database, options.defaults_file)

    datestamp = datetime.now()
    # Dump DDL
    ddlrepo = FilesystemRepository(
        name, options.dumpdir, options.compress, part="schema")
    backup(
        repository=ddlrepo,
        source=source,
        includes=["*"],
        excludes=[],
        no_data=True,
        datestamp=datestamp
    )

    # Dump Data
    datarepo = FilesystemRepository(
        name, options.dumpdir, options.compress, part="data")
    backup(
        repository=datarepo,
        source=source,
        includes=includes,
        excludes=excludes,
        no_data=False,
        datestamp=datestamp
    )

    if options.prune:
        ddlrepo.prune(options.keep)
        datarepo.prune(options.keep)


if __name__ == "__main__":
    main(sys.argv[0], sys.argv[1:])
