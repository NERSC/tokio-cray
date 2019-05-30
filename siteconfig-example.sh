#
# This script contains site-specific configurations and secrets
#

BASE_DIR="$(dirname $(readlink -f ${BASH_SOURCE[0]}))"

export ADMIN_EMAIL="admin@mycenter.edu" # gets errors
export REPORT_EMAIL="admin@mycenter.edu" # gets daily reports

# Location of the TOKIO repository--even if pytokio is in PYTHONPATH, you must
# define this so hourly_archive.sh knows where to find the archive_lmtdb.py
# script
export PYTOKIO_HOME=${PYTOKIO_HOME:-/home/jane/src/git/pytokio}

# Make sure python and pytokio are available
module load python
export PYTHONPATH="${PYTOKIO_HOME}${PYTHONPATH:+:$PYTHONPATH}"

# The base of the tree where HDF5 files will be saved
export HDF5_OUTPUT_DIR=${HDF5_OUTPUT_DIR:-$BASE_DIR}

# Determine which file system(s) to archive
if [ "$HOST" == "cluster1" ]; then
    export FILE_SYSTEMS=${FILE_SYSTEMS:-"filesystem_snx10001 filesystem_snx10002 filesystem_snx10003"}
elif [ "$HOST" == "cluster2" ]; then
    export FILE_SYSTEMS=${FILE_SYSTEMS:-"filesystem_snx11001"}
fi

# Map the file system name to an output file name
declare -A FS_NAME_TO_HDF5
FS_NAME_TO_HDF5[filesystem_snx11001]="snx11001.hdf5"
FS_NAME_TO_HDF5[filesystem_snx10001]="snx10001.hdf5"
FS_NAME_TO_HDF5[filesystem_snx10002]="snx10002.hdf5"
FS_NAME_TO_HDF5[filesystem_snx10003]="snx10003.hdf5"
export FS_NAME_TO_HDF5

# Map each file system name to a MySQL database hostname
declare -A CONNECT_ARGS
CONNECT_ARGS[filesystem_snx11001]="--host cluster2-mgmt.mycenter.edu"
CONNECT_ARGS[filesystem_snx10001]="--host dbserver1.mycenter.edu"
CONNECT_ARGS[filesystem_snx10002]="--host dbserver2.mycenter.edu"
CONNECT_ARGS[filesystem_snx10003]="--host dbserver3.mycenter.edu"
export CONNECT_ARGS


