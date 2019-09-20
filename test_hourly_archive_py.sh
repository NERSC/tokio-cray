#!/bin/bash -l
################################################################################
#  Wrapper to call hourly_archive.py and track the history of data that has been
#  already successfully archived.
#
#  Glenn K. Lockwood, July 2019            Lawrence Berkeley National Laboratory
################################################################################

BASE_DIR="$(dirname $(readlink -f ${BASH_SOURCE[0]}))"

source ${BASE_DIR}/siteconfig.sh # Load host-specific parameters

LOGFILE_BASE="${BASE_DIR}/archive_lmtdb.test.log"
LOGFILE="${LOGFILE_BASE}_${RANDOM}"

${BASE_DIR}/hourly_archive.py cscratch | gawk '{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0 }' > $LOGFILE 2>&1

### In the event of an error, e-mail the administrator the log file
if [ $? -ne 0 ]; then
    mailx -s "$(basename $BASH_SOURCE) generated errors" glock@lbl.gov < $LOGFILE
fi

### Append the log file to the global log
cat $LOGFILE >> $LOGFILE_BASE
rm $LOGFILE
