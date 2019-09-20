#!/usr/bin/env python
"""Executes a TOKIO CLI archiver tool using a stateful schedule
"""

import os
import sys
import json
import stat
import errno
import fcntl
import argparse
import datetime

import tokio
import tokio.cli.archive_lmtdb
import tokio.cli.archive_mmperfmon
import tokio.cli.archive_collectdes
import tokio.cli.archive_esnet_snmp

SCHEDULE_FILE = "%s.schedulefile"
ARCHIVERS = {
    "scratch1": tokio.cli.archive_lmtdb.main,
    "scratch2": tokio.cli.archive_lmtdb.main,
    "scratch3": tokio.cli.archive_lmtdb.main,
    "cscratch": tokio.cli.archive_lmtdb.main,
    "coribb": tokio.cli.archive_collectdes.main,
    "project2": tokio.cli.archive_mmperfmon.main,
    "projecta": tokio.cli.archive_mmperfmon.main,
    "projectb": tokio.cli.archive_mmperfmon.main,
#   "esnet": tokio.cli.archive_esnet_snmp.main, # does not currently work; CLI interface doesn't support --init-start/--init-end
}

# Intervals that start over MAX_AGE seconds ago are discarded without being run
MAX_AGE = 30
MAX_ATTEMPTS = 2
SCHEDULE_STEP = datetime.timedelta(hours=1) # archive this much time in one invocation
FILE_INTERVAL = datetime.timedelta(days=1) # domain of each output file

DATE_FMT = "%Y-%m-%dT%H:%M:%S"
DATE_FMT_PRINT = "YYYY-MM-DDTHH:MM:SS"

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
        "esnet": "esnet_nersc.hdf5",
        "project2": "project2.hdf5",
        "projecta": "projecta.hdf5",
        "projectb": "projectb.hdf5"
    },
    "archiver_args": {
        "scratch1": "--host edisonxa1.nersc.gov --timestep 5 --database filesystem_snx11025",
        "scratch2": "--host edisonxa2.nersc.gov --timestep 5 --database filesystem_snx11035",
        "scratch3": "--host edisonxa3.nersc.gov --timestep 5 --database filesystem_snx11036",
        "cscratch": "--host corifs01.nersc.gov --timestep 5 --database filesystem_snx11168",
        "gscratch": "--host gertfs01.nersc.gov --timestep 5 --database filesystem_snx11169",
        "coribb": "--num-nodes 288 --ssds-per-node 4 --timestep 10 --timeout 300 --threads 4 --index cori-collectd-*",
        "project2": "--filesystem project2",
        "projecta": "--filesystem projecta",
        "projectb": "--filesystem projectb",
    },
}

class PeriodicArchiver(object):
    """Class that periodically runs an archiver process

    Args:
        archiver_method (function): main() method for a pytokio archiver CLI
            package to call to trigger the process of archiving timeseries data
            into TOKIO Time Series.  Must support --init-start, --init-end, and
            --output as CLI arguments.
        config (str): Path to configuration json file to use in conjunction with
            archiver_method
        verbose (int): Level of verbosity to use when executing an archival
            process

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
    def __init__(self, archiver_method, config=None, verbose=1, max_attempts=None, max_age=30):
        self.archiver_method = archiver_method
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
        self.max_attempts = max_attempts
        self.max_age = datetime.timedelta(days=max_age)


        self.load_config(config=config)

    def vprint(self, msg, level=1):
        """Prints messages based on verbosity level

        Args:
            msg (str): Message to print to stdout
            level (int): Minimum verbosity level required to display msg
        """
        if level <= self.verbosity:
            print(msg)

    def verror(self, msg):
        """Prints error messages

        Args:
            msg (str): Message to send to stderr
        """
        sys.stderr.write("ERROR: " + msg + "\n")

    def load_config(self, config):
        """Loads the site-specific configuration file

        Args:
            config (str): Path to configuration json file to use in conjunction with
                self.archiver_method
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
        """Launches the archiver process

        Args:
            start (datetime.datetime): Begin retrieving and archiving data
                starting at this time, inclusive
            end (datetime.datetime): Stop retrieving and archiving data after
                this time, exclusive
            fsname (str): file system whose config/schedule should be used
            output_file (str): File to which output should be written
        """

        argv = self.archiver_args.get(fsname, "").split()
        if not os.path.isfile(output_file):
            argv += [
                "--init-start", start.strftime("%Y-%m-%dT00:00:00"),
                "--init-end", (end + FILE_INTERVAL).strftime("%Y-%m-%dT00:00:00")
            ]

        argv += ["--output", output_file]

        argv.append(start.strftime("%Y-%m-%dT%H:%M:%S"))
        argv.append(end.strftime("%Y-%m-%dT%H:%M:%S"))

        # ensure that two archivers aren't running at the same time
        lockf_name = self.get_schedule_file(fsname)
        lockfp = open(lockf_name, 'r')
        try:
            fcntl.flock(lockfp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as err: # Python 3 only
            if err.errno == errno.EAGAIN:
                raise type(err)("Instance of archiver already running for %s" % fsname)
            raise

        # call the archiver
        if self.archiver_method:
            self.vprint("Calling archiver with args: %s" % " ".join(argv))
            self.archiver_method(argv=argv)
        else:
            # unlock (though not usually necessary since process is about to die)
            fcntl.flock(lockfp, fcntl.LOCK_UN)
            lockfp.close()
            raise RuntimeError("No archiver defined for %s" % fsname)

        # unlock schedulefile
        fcntl.flock(lockfp, fcntl.LOCK_UN)
        lockfp.close()

    def init_schedule(self, fsname):
        """Initializes internal schedule state and schedule file

        Args:
            fsname (str): file system whose config/schedule should be used

        Returns:
            tuple of datetime.datetime: Start and end times of the interval just
            initialized
        """
        self.sched_start = datetime.datetime.now() - self.max_age
        self.sched_start = datetime.datetime(
            year=self.sched_start.year,
            month=self.sched_start.month,
            day=self.sched_start.day,
            hour=self.sched_start.hour,
            minute=0,
            second=0)
        self.attempts = 0

        # because we set the start time to the top of the hour, it may push the
        # start time into an invalid date, resulting in an endless loop of
        # invalid dates
        while (datetime.datetime.now() - self.sched_start) > self.max_age:
            self.sched_start += datetime.timedelta(hours=1)
        self.commit_schedule(fsname)

        return self.sched_start, self.sched_start + SCHEDULE_STEP

    def get_schedule_file(self, fsname):
        """Simple way to resolve fsname into a schedule file name

        Args:
            fsname (str): file system whose schedule file should be returned

        Returns:
            str: Path to the schedule file for fsname
        """
        return self.schedule_file % fsname

    def load_schedule(self, fsname):
        """Reads schedule file and updates internal schedule state

        Args:
            fsname (str): file system whose config/schedule should be used

        Returns:
            tuple of datetime.datetime: Start and end times of the next interval
        """
        if not os.path.isfile(self.get_schedule_file(fsname)):
            return self.init_schedule(fsname)

        self.sched_start, self.attempts = (None, None)
        with open(self.get_schedule_file(fsname), 'r') as sched_file:
            for line in sched_file:
                if line.lstrip().startswith('#'):
                    continue
                try:
                    self.sched_start, self.attempts = line.split()[0:2]
                    break
                except ValueError:
                    # invalid schedule file; initialize and re-try
                    return self.init_schedule(fsname)

        self.sched_start = datetime.datetime.fromtimestamp(int(self.sched_start))
        self.attempts = int(self.attempts)

        if self.max_attempts and self.attempts >= self.max_attempts:
            self.vprint("Exceeded max attempts (%d >= %d); moving to new step" % (self.attempts, self.max_attempts))
            self.sched_start += SCHEDULE_STEP
            self.attempts = 0

        # if schedule is ancient (e.g., because monitoring was paused), reset it
        if (datetime.datetime.now() - self.sched_start) > self.max_age:
            self.vprint("Dropping schedulefile due to age (%s)" % (datetime.datetime.now() - self.sched_start))
            return self.init_schedule(fsname)

        return self.sched_start, self.sched_start + SCHEDULE_STEP

    def update_schedule(self, fsname, success):
        """Updates the schedule file with either a new starting interval or the
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
        with open(self.get_schedule_file(fsname), 'w') as sched_file:
            sched_file.write("# start of next interval, number of failed attempts at this start time\n")
            sched_file.write("%d %d\n" % (tokio.common.to_epoch(self.sched_start), self.attempts))

    def archive(self, start, end, fsname, max_attempts=None):
        """Reads the schedule file, executes the archiver, and updates the
        schedule file.

        Args:
            start (datetime.datetime): Begin retrieving and archiving data
                starting at this time, inclusive.  If not specified, use the
                schedule file
            end (datetime.datetime): Stop retrieving and archiving data after
                this time, exclusive.  If not specified, use the schedule
                file
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
            self.verror("Valid file systems are: " + ", ".join(self.fsname_to_hdf5.keys()))
            return

        # get schedule using fsname
        if self.use_schedule:
            start, end = self.load_schedule(fsname)

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
        try:
            self.run_archiver(
                start=start,
                end=end,
                fsname=fsname,
                output_file=output_file)
        except: # blind except to emulate behavior of running a subprocess
            self.verror("Archiver raised an unhandled exception")
            self.update_schedule(fsname, success=False)
            raise

        # check for a total failure to create an output file
        if not os.path.isfile(output_file):
            self.verror("Failed to create %s" % output_file)
            self.update_schedule(fsname, success=False)
            return

        # fix permissions if file created
        chmod(output_file, stat.S_IROTH|stat.S_IRGRP|stat.S_IWGRP)

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
                        help="start time of query in %s format (default: use schedule file)"
                        % DATE_FMT_PRINT)
    parser.add_argument("-e", "--end", type=str,
                        help="end time of query in %s format (default: use schedule file)"
                        % DATE_FMT_PRINT)
    parser.add_argument('-c', '--config', type=str, default=None,
                        help="path to JSON configuration file")
    parser.add_argument('-v', '--verbose', action='count', default=0,
                        help="verbosity level (default: none)")
    parser.add_argument('--max-attempts', type=int, default=MAX_ATTEMPTS,
                        help="number of consecutive failures before skipping a time step (default: %d)" % MAX_ATTEMPTS)
    parser.add_argument('--max-age', type=int, default=MAX_AGE,
                        help="ignore intervals more than this many days in the past (default: %d)" % MAX_AGE)
    args = parser.parse_args(argv)

    # convert CLI options into datetime
    if args.start and args.end:
        try:
            start = datetime.datetime.strptime(args.start, DATE_FMT)
            end = datetime.datetime.strptime(args.end, DATE_FMT)
        except ValueError:
            sys.stderr.write("Start and end times must be in format %s\n" % DATE_FMT_PRINT)
            raise
    else:
        start = None
        end = None

    archiver = PeriodicArchiver(
        archiver_method=ARCHIVERS.get(args.fsname),
        verbose=args.verbose,
        config=args.config,
        max_attempts=args.max_attempts,
        max_age=args.max_age)
    archiver.archive(start=start, end=end, fsname=args.fsname)

if __name__ == "__main__":
    main()
