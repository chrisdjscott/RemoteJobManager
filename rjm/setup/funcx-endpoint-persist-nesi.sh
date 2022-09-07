#!/bin/bash

LOGIN_NODES=(mahuika01 mahuika02)
PRIMARY_NODE=login.mahuika.nesi.org.nz
ENDPOINT_NAME=default
LOG=~/.funcx-endpoint-persist-nesi.log

echo "Running $0 at $(date)" >> $LOG

# delete old funcx endpoint logs
EP_LOG_DIR=~/.funcx/${ENDPOINT_NAME}/HighThroughputExecutor/worker_logs/
mkdir -p ${EP_LOG_DIR}
nold=$(find ${EP_LOG_DIR} -type f -mtime +10 | wc -l)
find ${EP_LOG_DIR} -type f -mtime +10 -delete
find ${EP_LOG_DIR}/* -type d -empty -delete > /dev/null 2>&1
echo "  deleted ${nold} old funcx endpoint log files" >> $LOG

# check if there is a funcx endpoint already running somewhere
running=0
running_nodes=()
for node in ${LOGIN_NODES[@]}; do
    echo "  checking for endpoint running on ${node}" >> $LOG

    ssh -oStrictHostKeyChecking=no ${node} "source /etc/profile; module load funcx-endpoint; funcx-endpoint list" | grep "${ENDPOINT_NAME}" | grep Running > /dev/null
    if [ $? -eq 0 ]; then
        echo "    funcx '${ENDPOINT_NAME}' endpoint is running on ${node}" >> $LOG
        running=$((running+1))
        running_nodes[${#running_nodes[@]}]="${node}"
    else
        echo "    funcx '${ENDPOINT_NAME}' endpoint is not running on ${node}" >> $LOG
    fi
done

# if more than one endpoint is running, kill them all so we can start just one
if [ $running -gt 1 ]; then
    echo "  warning: funcx '${ENDPOINT_NAME}' endpoint is running on multiple nodes; stopping them" >> $LOG
    for node in ${running_nodes[@]}; do
        echo "    stopping endpoint running on ${node}"
        ssh -oStrictHostKeyChecking=no ${node} "source /etc/profile; module load funcx-endpoint; funcx-endpoint stop ${ENDPOINT_NAME}" >> $LOG 2>&1
    done
fi

if [ $running -eq 1 ]; then
    # endpoint is running, nothing to do
    echo "  the funcx '${ENDPOINT_NAME}' endpoint is already running on ${running_nodes[0]}" >> $LOG
else
    # start the default endpoint
    echo "  the funcx '${ENDPOINT_NAME}' endpoint is not running, starting it" >> $LOG

    primary=$(ssh -oStrictHostKeyChecking=no ${PRIMARY_NODE} hostname)
    ssh -oStrictHostKeyChecking=no ${PRIMARY_NODE} "source /etc/profile; module load funcx-endpoint; funcx-endpoint start ${ENDPOINT_NAME}" >> $LOG 2>&1
    if [ $? -eq 0 ]; then
        echo "    started funcx '${ENDPOINT_NAME}' endpoint on ${primary}" >> $LOG
    else
        echo "error: could not start funcx '${ENDPOINT_NAME}' endpoint on ${primary}" >> $LOG
        exit 1
    fi
fi
