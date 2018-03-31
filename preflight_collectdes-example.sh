#!/usr/bin/env bash
#
#  This script is executed before the archival process begins and is useful for
#  setting up runtime environment settings such as establishing SSH tunnels.
#

SSH_CMD="ssh -L 9200:elasticsearch.mycenter.edu:9200 gateway.mycenter.edu -Nf"
pgrep -xf "$SSH_CMD" >/dev/null
existing_connection=$?

if [ $existing_connection -eq 0 ]; then
    echo "[$(date)] SSH tunnel already exists"
    retcode=0
else
    echo "[$(date)] Creating new SSH tunnel"
    $SSH_CMD
    retcode=$?
fi

exit $return
