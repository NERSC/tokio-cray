#!/bin/bash -l
#
#  This script wraps the wrapper that is called by cron and generates e-mail
#  alerts when hourly_archive.sh throws errors.  It can get very noisy during
#  major downtime since this will send e-mails every time cron calls it.
#

BASE_DIR="$(dirname $(readlink -f ${BASH_SOURCE[0]}))"
source ${BASE_DIR}/siteconfig.sh # load host-specific parameters

LOGFILE_BASE="${BASE_DIR}/archive_collectdes.log"
LOGFILE="${LOGFILE_BASE}_${RANDOM}"

${BASE_DIR}/hourly_archive_collectdes.sh > $LOGFILE 2>&1

### In the event of an error, e-mail the administrator the log file
if [ $? -ne 0 ]; then
    mailx -s "$(basename $BASH_SOURCE) generated errors" $ADMIN_EMAIL < $LOGFILE
fi

### Append the log file to the global log
cat $LOGFILE >> $LOGFILE_BASE
rm $LOGFILE
