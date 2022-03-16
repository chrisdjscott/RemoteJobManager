#!/bin/bash

LOGIN_NODES=(mahuika01 mahuika02)
PRIMARY_NODE=login.mahuika.nesi.org.nz
ENDPOINT_NAME=default
LOG=~/funcx-endpoint-persist-nesi.log

echo "Running $0 at $(date)" >> $LOG

# first check if there is a funcx endpoint already running somewhere
running=0
for node in ${LOGIN_NODES[@]}; do
    echo "checking: ${node}" >> $LOG

    ssh ${node} "source /etc/profile; module load funcx-endpoint; funcx-endpoint list" | grep default | grep Running > /dev/null
    if [ $? -eq 0 ]; then
        echo "  funcx endpoint is running on ${node}" >> $LOG
        running=$((running+1))
    else
        echo "  funcx endpoint is not running on ${node}" >> $LOG
    fi
done

if [ $running -eq 1 ]; then
    # default endpoint is running, nothing to do
    echo "a funcx endpoint is running" >> $LOG
elif [ $running -gt 1 ]; then
    # bad, should probably kill them all and start a new one
    echo "error: funcx default endpoint is running on multiple nodes" >> $LOG
    exit 1
else
    # start the default endpoint
    echo "no funcx endpoint running, starting it" >> $LOG

    primary=$(ssh ${PRIMARY_NODE} hostname)
    ssh ${PRIMARY_NODE} "source /etc/profile; module load funcx-endpoint; funcx-endpoint start" >> $LOG 2>&1
    if [ $? -eq 0 ]; then
        echo "  started funcx default endpoint on ${primary}" >> $LOG
    else
        echo "error: could not start funcx default endpoint on $primary" >> $LOG
        exit 1
    fi
fi
