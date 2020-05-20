#!/bin/bash -l
#
#  Generate a summary of a single day's HDF5 data for each file system, generate
#  a report, and e-mail it (along with any detected errors) to an administrator.
#

BASE_DIR="$(dirname $(readlink -f ${BASH_SOURCE[0]}))"
source ${BASE_DIR}/siteconfig.sh # load host-specific parameters

LOGFILE=$(mktemp)

errors=0
yesterdate=$(date -d yesterday +"%Y-%m-%d")
echo "Daily Lustre summary for $yesterdate" > "$LOGFILE"

for fs_name in "${!FS_NAME_TO_HDF5[@]}"
do
    fs_file="${FS_NAME_TO_HDF5[$fs_name]}"

    echo -e "\n=== Checking $fs_file ===" >> "$LOGFILE"
    hdf5_path="${HDF5_OUTPUT_DIR}/$yesterdate/$fs_file"
    if [ -f "$hdf5_path" ]; then
        $PYTOKIO_HOME/bin/summarize_h5lmt --tibs --bins 4 --summary $hdf5_path >> "$LOGFILE" 2>&1
        ret=$?
        if [ $ret -ne 0 -a "$errors" -eq 0 ]; then
            errors=$ret
        fi
    else
        echo "*** FILE NOT FOUND ***" >> "$LOGFILE"
        let errors++
    fi
done

if [ $errors -ne 0 ]; then
   mailx -s "ERRORS: daily file system summary" $REPORT_EMAIL < "$LOGFILE"
else
   mailx -s "daily file system summary" $REPORT_EMAIL < "$LOGFILE"
fi

rm -f "$LOGFILE"
