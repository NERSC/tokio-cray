#!/usr/bin/env bash

FS="cfs"
OUTPUT_BASE="/global/homes/g/glock/h5lmt/"

################################################################################

HERE="$(dirname $(readlink -f ${BASH_SOURCE[0]}))"
DATE_FMT="+%Y-%m-%d"

START_DATE="2021-05-01"
END_DATE=$(date -d "yesterday" "$DATE_FMT")

echo "Ending after $END_DATE"

END_EPOCH=$(date -d "$END_DATE" "+%s")
NOW="$START_DATE"
set -e
while /bin/true
do
    NOW_EPOCH=$(date -d "$NOW" "+%s")
    if [ $NOW_EPOCH -gt $END_EPOCH ]; then
        break
    fi

    OUTPUT_DIR="${OUTPUT_BASE}/${NOW}"
    export OUTPUT_FILE="$OUTPUT_DIR/${FS}-ph2.hdf5"

    if [ -f "$OUTPUT_FILE" ]; then
        echo "$OUTPUT_FILE already exists; skipping"
    else
        echo "$OUTPUT_FILE does not exist; executing [$HERE/quick-archive-mmperfmon.sh \"$NOW\" \"$FS\"]"
        $HERE/quick-archive-mmperfmon.sh "$NOW" "$FS"
    fi

    NOW_EPOCH=$((NOW_EPOCH + 86400))
    NOW="$(date -d "@${NOW_EPOCH}" "${DATE_FMT}")"
done
