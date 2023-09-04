#!/bin/bash

# source functions file
source ${HOME}/.funcx-endpoint-persist-nesi-functions.sh

echo ""
echo "Running $0 at $(date)"

# migrate to globus compute
#migrate_to_globus_compute

# delete old funcx endpoint logs
cleanup_logs

# check if there is a funcx endpoint already running somewhere
check_endpoint_running_nodes

# restart the endpoint if asked to or if there is not exactly one endpoint running already
if [ -z ${ENDPOINT_RUNNING_COUNT+x} ]; then
    echo "Error: checking if endpoint running did not work!"
elif [ "${ENDPOINT_RUNNING_COUNT}" != "1" ] || [ "${ENDPOINT_RESTART}" == "1" ]; then
    restart_endpoint
else
    # endpoint is running, nothing to do
    echo "  the funcx '${ENDPOINT_NAME}' endpoint is already running on ${ENDPOINT_RUNNING_NODES[0]}"
fi
