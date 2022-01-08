#!/bin/sh

set -eu

################################################################################
##### CONFIGURATION ############################################################
################################################################################

# Mandatory: Set DBDUMP to the full path to the dbdump.py script.
# DBDUMP=/some/path/to/dbdump.py
DBDUMP="./dbdump.py"

# Specify a path to the dump directory.
DUMPDIR="/var/backups"

# Specify the database name.
DATABASE="test"

# Specify command line options (see dbdump.py --help)
OPTIONS="--defaults-path /etc/mysql/my.cnf --compress --prune --keep 5 --exclude cache_*"

# Path to python interpreter
PYTHON="/usr/bin/python3"

################################################################################
##### DO NOT CHANGE ANYTHING BEYOND THIS LINE ##################################
################################################################################

echo "Content-Type: text/plain"
echo

# Check if we can execute dbdump.py
if [ ! -x "$DBDUMP" ]; then
    exit 1
fi

# Execute main script in clean environment
exec "${PYTHON}" "${DBDUMP}" ${OPTIONS} "${DUMPDIR}" "${DATABASE}"
