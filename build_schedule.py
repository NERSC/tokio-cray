#!/usr/bin/env python
"""Tool that builds a schedule for an arbitrary time range

This tool builds a schedulefile for an arbitrary time range for those times when
old data needs to be archived through the regular scheduled process.
"""

import argparse
import datetime

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('start', type=str, help='start date in YYYY-MM-DD format, inclusive')
    parser.add_argument('end', type=str, help='end date in YYYY-MM-DD format, inclusive')
    args = parser.parse_args(argv)

    date_start = datetime.datetime.strptime(args.start, "%Y-%m-%d")
    date_end = datetime.datetime.strptime(args.end, "%Y-%m-%d") + datetime.timedelta(days=1, seconds=-1)

    if date_end < date_start:
        raise RuntimeError("end < start")

    now = date_start
    while now <= date_end:
        print("%s %s" % (now.strftime("%Y-%m-%d %H"), (now + datetime.timedelta(hours=1)).strftime("%Y-%m-%d %H")))
        now += datetime.timedelta(hours=1)

if __name__ == "__main__":
    main()
