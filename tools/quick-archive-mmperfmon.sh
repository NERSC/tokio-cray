#!/usr/bin/env bash
#
#  Convenience wrapper around the archive_mmperfmon CLI
#

HERE="$(dirname $(readlink -f ${BASH_SOURCE[0]}))/.."

ARCHIVE_MMPERFMON="/global/project/projectdirs/pma/glock/pytokio-master/bin/archive_mmperfmon"
VALID_FS="projecta projectb project2 cfs"

export PYTHONPATH="$(readlink -f $HERE/../pytokio-master/lib/python3.7/site-packages)"
export PYTOKIO_CONFIG="$HERE/pytokio-config.json"

DATE=$1
FS=$2

if [ -z "$DATE" -o -z "$FS" ]; then
    echo "Syntax: $(basename $0) YYYY-MM-DD {${VALID_FS}}" >&2
    exit 1
fi

if [[ "$VALID_FS" != *"$FS"* ]]; then
    echo "Invalid FS '$FS'; valid options are: $VALID_FS" >&2
    exit 1
fi

DATE_START=$(date -d "$DATE" "+%Y-%m-%dT00:00:00")
DATE_END=$(date -d "$DATE + 1 day" "+%Y-%m-%dT00:00:00")
OUTPUT_FILE="${OUTPUT_FILE:-$HOME/h5lmt/${DATE}/$FS.hdf5}"
echo "Archiving $DATE_START to $DATE_END to $OUTPUT_FILE"

set -x
$ARCHIVE_MMPERFMON --init-start "$DATE_START" --init-end "$DATE_END" --debug $DATE_START $DATE_END --output "$OUTPUT_FILE" --filesystem $FS
set +x
