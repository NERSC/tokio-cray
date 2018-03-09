# TOKIO for Cray ClusterStor

This repository contains some of the auxiliary scripts used to automatically
archive Lustre system data from a Lustre Monitoring Tool (LMT) database into the
TOKIO Time Series HDF5 format.

## Quick Start

1. Download and install pytokio (https://www.github.com/nersc/pytokio)
2. `cp siteconfig-example.sh siteconfig.sh`
3. Edit `siteconfig.sh` and add your site's specific paths and credentials

You can then test out the hourly archiver script by hand to ensure that your
configurations are correct:

    PYTOKIO_LMT_USER=lwatchclient \
        PYTOKIO_LMT_PASSWORD="" \
        FILE_SYSTEMS="filesystem_snx10001" \
        ./hourly_archive.sh 2018-03-09T00:00:00 2018-03-09T01:00:00

Remember to replace your `PYTOKIO_LMT_USER`, `PYTOKIO_LMT_PASSWORD`, and
`FILE_SYSTEMS` variables with valid values.  You should also ensure that the
date ranges passed to `hourly_archive.sh` correspond to date ranges that
actually contain data in the LMT database.  Specifying today's date is usually
the best way to ensure this.

Once your `siteconfig.sh` is correctly configured, you can automate the archival
process with a cron job.  The following crontab line represents what this may
look like:

    */15  *   *  *   * PYTOKIO_LMT_USER=lmtuser PYTOKIO_LMT_PASSWORD=lmtpasswd FILE_SYSTEMS="filesystem_snx10001 filesystem_snx10002 filesystem_snx10003 filesystem_snx11001" /home/jane/src/git/pytokio-cray/hourly_crontab_wrapper.sh

The `hourly_crontab_wrapper.sh` script (and all its dependencies) are fully
self-contained and can be run from any working directory.  These scripts are
also idempotent, so it is safe to run them multiple times per hour.  In the
above example, the archival process is kicked off four times an hour in case

1. there are temporary connection issues that may cause the archival process
   to randomly fail, and/or
2. the cron job does not run reliably, and it may not have been able to archive
   the previous hour

## Daily Reporting

This repository also contains the `daily_lmt_summary.sh` script which e-mails a
daily report of file system activity and raises alarms if the archival process
is causing errors.

If you want to receive a daily report for the previous day every morning at
9:00 AM, you may want to install a crontab entry like this:

    0  9   *  *   * /home/jane/src/git/pytokio-cray/daily_lmt_summary.sh

Like `hourly_crontab_wrapper.sh`, `daily_lmt_summary.sh` does not care what
`$PWD` is when run.
