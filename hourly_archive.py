#!/usr/bin/env python
"""Executes a TOKIO CLI archiver tool using a stateful schedule"""

import os
import sys
import json
import stat
import argparse
import datetime

import tokio
import tokio.cli.archive_lmtdb

SCHEDULE_FILE = "%s.schedulefile"
ARCHIVE_LMTDB = tokio.cli.archive_lmtdb.main

VERBOSITY = 1

# Intervals that start over MAX_AGE seconds ago are discarded without being run
MAX_AGE = datetime.timedelta(days=1)
MAX_ATTEMPTS = 5
SCHEDULE_STEP = datetime.timedelta(hours=1) # archive this much time in one invocation
FILE_INTERVAL = datetime.timedelta(days=1) # domain of each output file

DATE_FMT = "%Y-%m-%dT%H:%M:%S"

DEFAULT_CONFIG = {
    "admin_emails": [
        "glock@lbl.gov"
    ],
    "report_emails": [
        "glock@lbl.gov",
        "lgerhardt@lbl.gov"
    ],
    "output_dir": "/global/homes/g/glock/lmt/pma/tokio-cray",
    "fsname_to_hdf5": {
        "scratch1": "snx11025.hdf5",
        "scratch2": "snx11035.hdf5",
        "scratch3": "snx11036.hdf5",
        "cscratch": "snx11168.hdf5",
        "gscratch": "snx11169.hdf5",
        "coribb": "coribb.hdf5",
        "esnet": "esnet_nersc.hdf5"
    },
    "archiver_args": {
        "scratch1": "--host edisonxa1.nersc.gov --timestep 5 --database filesystem_snx11025",
        "scratch2": "--host edisonxa2.nersc.gov --timestep 5 --database filesystem_snx11035",
        "scratch3": "--host edisonxa3.nersc.gov --timestep 5 --database filesystem_snx11036",
        "cscratch": "--host corifs01.nersc.gov --timestep 5 --database filesystem_snx11168",
        "gscratch": "--host gertfs01.nersc.gov --timestep 5 --database filesystem_snx11169",
        "coribb": "--num-nodes 288 --ssds-per-node 4 --timestep 10 --timeout 300 --threads 4 --index cori-collectd-*"
    },
}

class PeriodicArchiver(object):
    """Class that periodically runs an archiver process

    Attributes:
        verbosity (int): Level of verbosity when executing
        admin_emails (list of str): Addresses to notify on errors
        report_emails (list of str): Addresses to notify with daily reports
        output_dir (str): Path to base of date-indexed directory where HDF5
            files will be generated
        use_schedule (bool): If False, use an explicitly specified start/end
            rather than rely on the scheduler
        schedule_file (str): Path to a file to be used to record the last
            executed interval
    """
    def __init__(self, config=None, verbose=1):
        self.verbosity = verbose

        # site-specific configuration parameters
        self.output_base_dir = None
        self.archiver_args = None
        self.fsname_to_hdf5 = None

        # runtime parameters
        self.use_schedule = True
        self.schedule_file = None
        self.sched_start = None
        self.attempts = None

        self.load_config(config=config)

    def vprint(self, msg, level=1):
        """Print messages based on verbosity level
        """
        if level >= self.verbosity:
            print(msg)

    def verror(self, msg):
        """Print error messages
        """
        sys.stderr.write("ERROR: " + msg + "\n")

    def load_config(self, config):
        """Loads the site-specific configuration file
        """
        if config:
            with open(config, 'r') as configfp:
                config_blob = json.load(configfp)
        else:
            config_blob = DEFAULT_CONFIG

        self.output_base_dir = config_blob.get('output_dir')
        self.archiver_args = config_blob.get('archiver_args')
        self.fsname_to_hdf5 = config_blob.get('fsname_to_hdf5')
        self.schedule_file = config_blob.get("schedule_file", SCHEDULE_FILE)

    def run_archiver(self, start, end, fsname, output_file):
        """Launches the archiver process.

        Args:
            start (datetime.datetime): Begin retrieving and archiving data
                starting at this time, inclusive
            end (datetime.datetime): Stop retrieving and archiving data after
                this time, exclusive
            fsname (str): file system whose config/schedule should be used
            output_file (str): File to which output should be written
        """
        # TODO: check lock

        argv = self.archiver_args.get(fsname, "").split()
        if not os.path.isfile(output_file):
            argv += [
                "--init-start", start.strftime("%Y-%m-%dT00:00:00"),
                "--init-end", (end + FILE_INTERVAL).strftime("%Y-%m-%dT00:00:00")
            ]

        argv += ["--output", output_file]

        argv.append(start.strftime("%Y-%m-%dT%H:%M:%S"))
        argv.append(end.strftime("%Y-%m-%dT%H:%M:%S"))

        self.vprint("Calling archiver with args: %s" % " ".join(argv))

        ARCHIVE_LMTDB(argv=argv)

        return 0

    def init_schedule(self, fsname):
        """Initializes internal schedule state and schedulefile

        Args:
            fsname (str): file system whose config/schedule should be used
        """
        self.sched_start = datetime.datetime.now() - MAX_AGE
        self.sched_start = datetime.datetime(
            year=self.sched_start.year,
            month=self.sched_start.month,
            day=self.sched_start.day,
            hour=self.sched_start.hour,
            minute=0,
            second=0)
        self.attempts = 0
        self.commit_schedule(fsname)

    def get_schedule(self, fsname):
        """Reads schedulefile and updates internal schedule state

        Args:
            fsname (str): file system whose config/schedule should be used

        Returns:
            tuple of datetime.datetime: Start and end times of the next interval
        """

        if not os.path.isfile(self.schedule_file % fsname):
            self.init_schedule(fsname)

        with open(self.schedule_file % fsname, 'r') as sched_file:
            line = sched_file.readline()

        self.sched_start, self.attempts = line.split()[0:2]
        self.sched_start = datetime.datetime.fromtimestamp(int(self.sched_start))
        self.attempts = int(self.attempts)

        # if schedule is ancient (e.g., because monitoring was paused), reset it
        if (datetime.datetime.now() - self.sched_start) > MAX_AGE:
            self.init_schedule(fsname)

        return self.sched_start, self.sched_start + SCHEDULE_STEP

    def update_schedule(self, fsname, success):
        """Updates the schedulefile with either a new starting interval or the
        same interval but an incremented failure count.

        Args:
            fsname (str): file system whose config/schedule should be used
            success (bool): If the update should reflect a successful execution
                or increment the failure counter
        """
        if not self.use_schedule:
            return

        if self.attempts is None:
            raise RuntimeError("update_schedule called before get/init schedule")

        if success:
            self.sched_start += SCHEDULE_STEP
            self.attempts = 0
        else:
            self.attempts += 1
            # self.sched_start remains unchanged

        self.commit_schedule(fsname)

    def commit_schedule(self, fsname):
        """Write current schedule state into the schedule file

        Args:
            fsname (str): file system whose config/schedule should be used
        """
        with open(self.schedule_file % fsname, 'w') as sched_file:
            sched_file.write("%d %d\n" % (tokio.common.to_epoch(self.sched_start), self.attempts))

    def archive(self, start, end, fsname):
        """Reads the schedule file, executes the archiver, and updates the
        schedule file.

        Args:
            start (datetime.datetime): Begin retrieving and archiving data
                starting at this time, inclusive
            end (datetime.datetime): Stop retrieving and archiving data after
                this time, exclusive
            fsname (str): file system whose config/schedule should be used
            output_file (str): File to which output should be written

        """
        # specify start/stop date/time if we want to bypass the scheduler
        if start is not None:
            self.use_schedule = False
            if start is None or end is None:
                self.verror("Invalid start/end; using schedule instead")
                self.use_schedule = True

        # determine validity of fsname and resolve output filename
        output_file = self.fsname_to_hdf5.get(fsname)
        if output_file is None:
            self.verror("Target file system %s is not in configuration file" % fsname)
            self.verror("Valid file systems are: " " ".join(self.fsname_to_hdf5.keys()))
            return

        # get schedule using fsname
        if self.use_schedule:
            start, end = self.get_schedule(fsname)

        # don't run schedules that include the future
        if start > datetime.datetime.now() or end > datetime.datetime.now():
            self.vprint("Next job for %s (%s to %s) is in the future; deferring"
                        % (fsname, start.strftime(DATE_FMT), end.strftime(DATE_FMT)))
            return
        self.vprint("Executing schedule from %s to %s for %s"
                    % (start.strftime(DATE_FMT), end.strftime(DATE_FMT), fsname))

        # create output directories if necessary
        output_file = os.path.join(
            self.output_base_dir,
            start.strftime("%Y-%m-%d"),
            output_file)
        output_dir = os.path.dirname(output_file)
        if not os.path.isdir(output_dir):
            if not os.path.isdir(self.output_base_dir):
                self.vprint("Creating " + self.output_base_dir)
                os.makedirs(self.output_base_dir)
                chmod(self.output_base_dir, stat.S_IROTH|stat.S_IXOTH|stat.S_IRGRP|stat.S_IXGRP)
            self.vprint("Creating " + output_dir)
            os.mkdir(output_dir)
            chmod(output_dir, stat.S_IROTH|stat.S_IXOTH|stat.S_IRGRP|stat.S_IXGRP)

        # actually create the output file at this point
        self.vprint("Archiving %s to %s" % (fsname, os.path.basename(output_file)))
        ret = self.run_archiver(
            start=start,
            end=end,
            fsname=fsname,
            output_file=output_file)

        # check for a total failure to create an output file
        if not os.path.isfile(output_file):
            self.verror("Failed to create %s" % output_file)
            self.update_schedule(fsname, success=False)
            return

        # fix permissions if file created
        chmod(output_file, stat.S_IROTH|stat.S_IRGRP|stat.S_IWGRP)

        # check for non-fatal errors
        if ret != 0:
            self.update_schedule(fsname, success=False)
            self.verror("Archiver existed with code %d" % ret)
            return

        # if we successfully archived an interval, update the schedule
        self.update_schedule(fsname, success=True)

def chmod(path, add_perms):
    """Wraps os.chmod to provide something equivalent to ``chmod +x``

    Args:
        path (str): Path to file/directory whose permissions should be changed
        add_perms (int): combination of stat mode flags to add to path's
            permissions
    """
    filestat = os.stat(path)
    os.chmod(path, filestat.st_mode | add_perms)

def main(argv=None):
    """CLI interface into the PeriodicArchiver class
    """
    # parse CLI options
    parser = argparse.ArgumentParser()
    parser.add_argument("fsname", type=str, default=None,
                        help="name of file system to archive")
    parser.add_argument("-s", "--start", type=str, default=None,
                        help="start time of query in %s format" % DATE_FMT)
    parser.add_argument("-e", "--end", type=str,
                        help="end time of query in %s format" % DATE_FMT)
    args = parser.parse_args(argv)

    # convert CLI options into datetime
    if args.start and args.end:
        try:
            start = datetime.datetime.strptime(args.start, DATE_FMT)
            end = datetime.datetime.strptime(args.end, DATE_FMT)
        except ValueError:
            sys.stderr.write("Start and end times must be in format %s\n" % DATE_FMT)
            raise
    else:
        start = None
        end = None

    archiver = PeriodicArchiver(config=None)
    archiver.archive(start=start, end=end, fsname=args.fsname)

if __name__ == "__main__":
    main()
