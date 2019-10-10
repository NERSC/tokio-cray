#!/bin/bash -l
################################################################################
#  Wrapper to call hourly_archive.py, track errors, and send e-mail
#  notifications when things are broken.  Intended to be called directly by
#  cron.
#
#  Glenn K. Lockwood, October 2019         Lawrence Berkeley National Laboratory
################################################################################

BASE_DIR="$(dirname $(readlink -f ${BASH_SOURCE[0]}))"
ERRORS=""

source ${BASE_DIR}/siteconfig.sh # Load host-specific parameters

################################################################################
LOGFILE_BASE="${BASE_DIR}/archive_hourly.log"
MAX_ERROR_FREQ=${MAX_ERROR_FREQ:-21600} # 21600 = six hours

# LMT is broken as of October 8, 2019
#${BASE_DIR}/hourly_archive.py cscratch | gawk '{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0 }' > $LOGFILE 2>&1

for fs in projecta projectb project2
do
    LOGFILE="${LOGFILE_BASE}_${fs}_${RANDOM}"

    ${BASE_DIR}/hourly_archive.py -v $fs 2>&1 | gawk '{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0 }' > $LOGFILE
    ret=$?

    ### Append the log file to the global log
    cat $LOGFILE >> $LOGFILE_BASE

    # ret=5 means we just deferred due to next schedule being in the future
    #if [ $ret -ne 0 -a $ret -ne 5 ]; then
    if [ $ret -ne 0 ]; then
        ERRORS="$ERRORS $LOGFILE"
    else
        rm $LOGFILE
    fi
done

### In the event of an error, e-mail the administrator the log file
if [ ! -z "$ERRORS" ]; then
    # check to see when the last error email was sent
    now="$(date '+%s')"
    if [ -f last_error_email ]; then
        last_err=$(cat last_error_email)
    else
        last_err=0
    fi
    error_ago=$((now - last_err))

    # only report error if we haven't reported one in a while.  cuts down on
    # high-frequency cronjobs sending out tons of emails for a service that is
    # down for a few hours
    if [ $error_ago -gt $MAX_ERROR_FREQ ]; then
        # report an error
        for logfile in $LOGFILE
        do
            echo "========================================"
            echo "$logfile"
            echo "========================================"
            cat $logfile
            rm $logfile
        done | mailx -s "$(basename $BASH_SOURCE) generated errors" $ADMIN_EMAIL

        # update the timestamp of last email sent
        echo "$now" > last_error_email
    fi
fi
