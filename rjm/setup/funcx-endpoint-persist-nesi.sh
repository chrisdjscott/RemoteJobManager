#!/bin/bash

# source functions file
source ${HOME}/.funcx-endpoint-persist-nesi-functions.sh

echo "" >> $LOG
echo "Running $0 at $(date)" >> $LOG

# delete old funcx endpoint logs
cleanup_logs

# check if there is a funcx endpoint already running somewhere
check_endpoint_running_nodes

# restart the endpoint if asked to or if there is not exactly one endpoint running already
if [ "${ENDPOINT_RUNNING_COUNT}" -ne 1 ] || [ "${ENDPOINT_RESTART}" -eq 1 ]; then
    restart_endpoint
else
    # endpoint is running, nothing to do
    echo "  the funcx '${ENDPOINT_NAME}' endpoint is already running on ${ENDPOINT_RUNNING_NODES[0]}" >> $LOG
fi


# we want one endpoint to be running
#if [ ${ENDPOINT_RUNNING_COUNT} -eq 1 ]; then
#    # endpoint is running, nothing to do
#    echo "  the funcx '${ENDPOINT_NAME}' endpoint is already running on ${ENDPOINT_RUNNING_NODES[0]}" >> $LOG
#else
#    # stop any endpoints that may be running and start one
#    restart_endpoint
#fi
