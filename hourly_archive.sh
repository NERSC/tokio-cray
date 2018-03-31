#!/bin/bash -l
################################################################################
#  Wrapper to call archive_lmtdb.py and track the history of data that has been
#  already successfully archived.
#
#  Glenn K. Lockwood, September 2018       Lawrence Berkeley National Laboratory
################################################################################

BASE_DIR="$(dirname $(readlink -f ${BASH_SOURCE[0]}))"

source ${BASE_DIR}/siteconfig.sh # Load host-specific parameters

SCHEDULE_DIR=${SCHEDULE_DIR:-$BASE_DIR/schedule.lmtdb}
ARCHIVE_LMTDB=${ARCHIVE_LMTDB:-${PYTOKIO_HOME}/bin/archive_lmtdb.py}

# Intervals that start over MAX_AGE seconds ago are discarded without being run
MAX_AGE=${MAX_AGE:-$((24*3600))} # in seconds

################################################################################
### Begin archival process #####################################################
################################################################################

# Nothing below should be site-specific or require modification

echo "[$(date)] Beginning archive process"

if [ -z "$FILE_SYSTEMS" ]; then
    echo "[$(date)] FILE_SYSTEMS not defined; aborting" >&2
    exit 1
fi

echo "[$(date)] Populating HDF5 files for [${FILE_SYSTEMS}]"

### Specify a start/stop date/time on CLI to bypass the use of the schedulefile
USE_SCHEDULE=1
if [ ! -z "$2" ]; then
    START_DATE="$(date -d "$1" +"%Y-%m-%d")"
    START_HOUR="$(date -d "$1" +"%H")"
    END_DATE="$(date -d "$2" +"%Y-%m-%d")"
    END_HOUR="$(date -d "$2" +"%H")"
    if [ -z "$START_DATE" -o -z "$START_HOUR" -o -z "$END_DATE" -o -z "$END_HOUR" ]; then
        echo "[$(date)] WARNING: Invalid start/end dates/times passed via CLI; using schedule" >&2
    else
        USE_SCHEDULE=0
    fi
fi

### Create output directory if it does not exist
if [ ! -d "$HDF5_OUTPUT_DIR" ]; then
    mkdir -p "$HDF5_OUTPUT_DIR"
    if [ ! -d "$HDF5_OUTPUT_DIR" ]; then
        echo "[$(date)] Unable to create output dir [${HDF5_OUTPUT_DIR}]" >&2
        exit -1
    fi
fi
cd "$HDF5_OUTPUT_DIR"                

global_failure=0                      # can be used to raise alerts by caller
for host_fs in $FILE_SYSTEMS; do
    ### Ensure the requisite schedule directory exists
    if [ ! -d "$SCHEDULE_DIR" ]; then
        mkdir -p "$SCHEDULE_DIR" || exit -1
    fi

    ### when USE_SCHEDULE=0, the schedulefile is still used for locking
    SCHEDULE_FILE="${SCHEDULE_DIR}/${host_fs}.schedule"

    if [ "$USE_SCHEDULE" -ne 0 ]; then
        ### If schedule does not exist, create an empty one
        if [ ! -f "$SCHEDULE_FILE" ]; then
            touch "$SCHEDULE_FILE"
        fi

        ### Ensure the permissions on the schedule aren't screwed up
        if [ ! -r "$SCHEDULE_FILE" ]; then
            echo "[$(date)] ERROR: Schedule file is not readable [$SCHEDULE_FILE $(stat -c '%U:%G@%a')]" >&2
            global_failure=-1
            continue
        fi
    fi

    ### Walk through the schedule file until we find a valid schedule to execute
    jit_schedule=0
    NOW_EPOCH=$(date "+%s")

    ### Below is a sloppy way to do /bin/true since $USE_SCHEDULE never changes
    while [ "$USE_SCHEDULE" -ne 0 ]; do
        ### Parse the first line of the schedule file and split it into a
        ### start/end date and hour
        SCHEDULE_LINE="$(head -n1 $SCHEDULE_FILE | sed -e 's/(^ *|* $)//g' -e 's/  */ /g')"

        ### If the schedule file is empty, schedule just-in-time
        if [ -z "$SCHEDULE_LINE" ]; then
            SCHEDULE_LINE="$(date -d "1 hour  ago" +"%Y-%m-%d %H") $(date -d "now" +"%Y-%m-%d %H")"
            echo "[$(date)] Schedule file [${SCHEDULE_FILE}] is empty; scheduling [${SCHEDULE_LINE}] just in time" >&2
            echo "$SCHEDULE_LINE" > "$SCHEDULE_FILE"
            jit_schedule=1
        fi

        ### Parse the schedule line
        IFS=' ' read START_DATE START_HOUR END_DATE END_HOUR LOCK_TOKEN <<< "$SCHEDULE_LINE"

        ### If the line didn't have enough args, toss it and try again
        if [ -z "$END_HOUR" ]; then
            echo "[$(date)] WARNING: Removing malformed schedule date [$SCHEDULE_LINE]" >&2
            sed -i "1d" "$SCHEDULE_FILE"
            ### avoid a deadlock if JIT scheduling generates bogus lines
            if [ "$jit_schedule" -ne 0 ]; then
                break
            fi
            continue
        fi

        ### If schedule line is too old, don't bother trying to archive that far
        ### back (because it likely doesn't exist in the MySQL db anymore)
        START_EPOCH=$(date -d "${START_DATE} ${START_HOUR}:00:00" "+%s")
        END_EPOCH=$(date -d "${END_DATE} ${END_HOUR}:00:00" "+%s")
        if [ $START_EPOCH -lt $(( $NOW_EPOCH - $MAX_AGE )) ]; then
            echo "[$(date)] WARNING: Discarding schedule [$SCHEDULE_LINE] due to age being > $(($MAX_AGE/3600)) hours" >&2
            sed -i "1d" "$SCHEDULE_FILE"
        else
            jit_schedule=0
            break
        fi
    done

    ### If jit_schedule is still 1, the JIT scheduler issued a malformed line
    ### and we broke out of the schedule parser to avoid deadlock.  Fail now.
    if [ "$USE_SCHEDULE" -ne 0 -a "$jit_schedule" -ne 0 ]; then
        echo "[$(date)] ERROR: JIT scheduler issued a bad schedule in [$SCHEDULE_FILE]" >&2
        global_failure=-1
        continue
    fi

    ###
    ### Don't run schedules that include the future.
    ###
    if [ -z "$START_EPOCH" -o -z "$END_EPOCH" ]; then
        START_EPOCH=$(date -d "${START_DATE} ${START_HOUR}:00:00" "+%s")
        END_EPOCH=$(date -d "${END_DATE} ${END_HOUR}:00:00" "+%s")
    fi
    if [ "$START_EPOCH" -gt "$NOW_EPOCH" -o "$END_EPOCH" -gt "$NOW_EPOCH" ]; then
        if [ "$USE_SCHEDULE" -ne 0 ]; then
            echo "[$(date)] Next job in [${SCHEDULE_FILE}] ($START_DATE $START_HOUR:00 - $END_DATE $END_HOUR:00) includes the future; deferring" >&2
        else
            echo "[$(date)] Scheduled job from CLI ($START_DATE $START_HOUR:00 - $END_DATE $END_HOUR:00) includes the future; aborting" >&2
        fi
        continue
    fi

    echo "[$(date)] Executing schedule from $START_DATE ${START_HOUR}:00 to $END_DATE ${END_HOUR}:00 for ${host_fs}"

    ### Read from MySQL and write to hdf5
    output_file="${FS_NAME_TO_HDF5[$host_fs]}"
    if [ -z "$output_file" ]; then
        echo "[$(date)] ERROR: Target file system [$host_fs] not in FS_NAME_TO_HDF5"
        global_failure=-1
        continue
    fi

    ### Convert to an absolute path
    output_file="${HDF5_OUTPUT_DIR}/${START_DATE}/${output_file}"

    ### Ensure target output HDF5 file exists; if not, initialize it
    if [ ! -f "$output_file" ]; then
        echo "[$(date)] Target output file [$output_file] does not exist" >&2
        echo "[$(date)] Preparing h5lmt for $host_fs for  $START_DATE"
        init_end=$(date -d "$START_DATE + 1 day" "+%Y-%m-%d")
        INIT_ARGS="--init-start ${START_DATE}T00:00:00 --init-end ${init_end}T00:00:00"
        output_dir="$(dirname $output_file)"
        if [ ! -d "$output_dir" ]; then
            mkdir -vp "$output_dir"
        fi
    fi

    echo "[$(date)] Archiving $host_fs to $(basename $output_file)"

    set -x
    /usr/bin/flock --nonblock "$SCHEDULE_FILE" "$ARCHIVE_LMTDB" \
        --output "$output_file" \
        $INIT_ARGS \
        --timestep 5 \
        ${CONNECT_ARGS[$host_fs]} \
        --database $host_fs \
        "${START_DATE}T${START_HOUR}:00:00" "${END_DATE}T${END_HOUR}:00:00"
    ret=$?
    set +x

    ### Fix the permissions on the file--don't rely on umask (implicit)
    if [ -d "${START_DATE}" ]; then
        chmod o+rx ${START_DATE}
        chmod o+r ${START_DATE}/*
        chmod g+rwx ${START_DATE}
        chmod g+rw ${START_DATE}/*
    elif [ ! -f "$output_file" ]; then
        echo "[$(date)] ERROR: $(basename $ARCHIVE_LMTDB) did not create $output_file" >&2
        global_failure=-1
        continue
    else
        echo "[$(date)] ERROR: $(basename $ARCHIVE_LMTDB) resulted in an unknown error" >&2
        global_failure=-2
        continue
    fi

    if [ "$ret" -eq 0 ]; then
        ### if successful, we can retire this schedule
        if [ "$USE_SCHEDULE" -ne 0 ]; then
            sed -i '1d' "$SCHEDULE_FILE"
        fi
    else
        ### Try to catch failures due to locking.  The version of flock on
        ### Edison does not support --conflict-exit-code, so we cannot
        ### differentiate failures of flock from failures of archive_lmtdb.py 
        ### Re-checking the flock is the best we can do for now.
        if /usr/bin/flock --nonblock "$SCHEDULE_FILE" /bin/true; then
            ### flock came back clean, so the error was not a lock issue
            echo "[$(date)] ERROR: $(basename $ARCHIVE_LMTDB) returned code $ret" >&2
        else
            ### flock came back with an error, so the schedule is still locked
            echo "[$(date)] ERROR: Another instance of $(basename $ARCHIVE_LMTDB) is already running" >&2
        fi

        ### only retain the last failure code for global_failure
        global_failure=$ret
    fi

    ### If we just retired the last schedule, add more scheduled archives
    ### Note that we could just add a single schedule here so that all
    ### invocations are just-in-time.  However if the archiver skips an hour or
    ### two (up to MAX_AGE) due to cron temporarily not being available (e.g.,
    ### node crash + quick recovery), having some lead time in the schedule lets
    ### us catch up.
    if [ "$USE_SCHEDULE" -ne 0 -a ! -s "$SCHEDULE_FILE" ]; then
        last_schedule_fmted="$(date -d "$START_DATE ${START_HOUR}:00:00" '+%Y-%m-%d %H:%M:%S %Z')"
        max_hours=$(($MAX_AGE/3600))
        for i in $(seq 1 $max_hours); do
            ### use `date` for arithmetic so DST doesn't break our schedule
            t_start=$(date -d "$last_schedule_fmted + $i hours" +"%Y-%m-%d %H")
            t_end=$(date -d "$last_schedule_fmted  + $((i+1)) hours" +"%Y-%m-%d %H")
            schedule_line="$t_start $t_end"
            echo "[$(date)] Updating schedule [$schedule_line] to $SCHEDULE_FILE"
            echo "$schedule_line" >> "$SCHEDULE_FILE" 
        done
    fi
done
