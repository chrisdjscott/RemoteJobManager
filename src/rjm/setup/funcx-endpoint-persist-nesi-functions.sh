
export LOGIN_NODES=(mahuika01 mahuika02)
export PRIMARY_NODE=login.mahuika.nesi.org.nz
export ENDPOINT_NAME=default
export LOG=${HOME}/.funcx-endpoint-persist-nesi.log
export FUNCX_MODULE="funcx-endpoint/1.0.7-gimkl-2020a-Python-3.9.9"
export INIT_COMMAND="source /etc/profile; source ~/.funcx-endpoint-persist-nesi-functions.sh; module load ${FUNCX_MODULE}"
export ENDPOINT_PIDFILE="${HOME}/.funcx/${ENDPOINT_NAME}/daemon.pid"

cleanup_logs () {
    local EP_LOG_DIR=~/.funcx/${ENDPOINT_NAME}/HighThroughputExecutor/worker_logs/

    # delete old funcx endpoint logs
    mkdir -p ${EP_LOG_DIR}
    local nold=$(find ${EP_LOG_DIR} -type f -mtime +10 | wc -l)
    find ${EP_LOG_DIR} -type f -mtime +10 -delete
    find ${EP_LOG_DIR}/* -type d -empty -delete > /dev/null 2>&1
    echo "  deleted ${nold} old funcx endpoint log files" >> $LOG

    return 0
}

check_daemon_process_owner () {
    # first we check for a pidfile and look at who owns the process
    # it's possible another user may have a process with the same pid on the other login node
    # if our user doesn't own the process in the pidfile, then assume it isn't running on this node
    if [ -f "${ENDPOINT_PIDFILE}" ]; then
        local puser=$(ps -o user= -p $(cat "${ENDPOINT_PIDFILE}"))
        local pcomm=$(ps -o comm= -p $(cat "${ENDPOINT_PIDFILE}"))
        if [ -z "${puser}" ]; then
            # process doesn't exist
            echo "    no running process with pid: $(cat "${ENDPOINT_PIDFILE}")" >> $LOG
            return 1
        elif [ "${puser}" != "${USER}" ]; then
            # user doesn't own process or no process exists
            echo "    process is owned by another user: ${puser}" >> $LOG
            return 1
        elif ! grep -qi funcx <<< "${pcomm}"; then
            # process is not running funcx-endpoint
            echo "    process is not running funcx-endpoint: ${pcomm}" >> $LOG
            return 1
        fi
    fi

    return 0
}

get_endpoint_id () {
    module load ${FUNCX_MODULE}
    ENDPOINT_ID=$(funcx-endpoint list | grep default | awk -F '|' '{print $(NF-1)}')
    export ENDPOINT_ID
}

check_endpoint_running_local () {
    # return value of this function will be:
    # - 0 if the endpoint is running
    # - 1 if the endpoint is not running
    # - other values indicate an error

    # first we check for a pidfile and look at who owns the process
    # it's possible another user may have a process with the same pid on the other login node
    # if our user doesn't own the process in the pidfile, then assume it isn't running on this node
    check_daemon_process_owner
    retval=$?
    if [ $retval -eq 1 ]; then
        # assume not running if process owned by another user
        return 1
    elif [ $retval -ne 0 ]; then
        return 201
    fi

    # run the endpoint command to check if it is running too
    funcx-endpoint list | grep "${ENDPOINT_NAME}" | grep Running > /dev/null
    if [ $? -eq 0 ]; then
        # running
        return 0
    else
        # not running
        return 1
    fi
}

check_endpoint_running_nodes () {
    # check if there is a funcx endpoint already running somewhere
    ENDPOINT_RUNNING_COUNT=0
    ENDPOINT_RUNNING_NODES=()
    for node in ${LOGIN_NODES[@]}; do
        echo "  checking for endpoint running on ${node}" >> $LOG
        ssh -oStrictHostKeyChecking=no ${node} "${INIT_COMMAND}; check_endpoint_running_local"
        if [ $? -eq 0 ]; then
            echo "    funcx '${ENDPOINT_NAME}' endpoint is running on ${node}" >> $LOG
            ENDPOINT_RUNNING_COUNT=$((ENDPOINT_RUNNING_COUNT+1))
            ENDPOINT_RUNNING_NODES[${#ENDPOINT_RUNNING_NODES[@]}]="${node}"
        elif [ $? -eq 1 ]; then
            echo "    funcx '${ENDPOINT_NAME}' endpoint is not running on ${node}" >> $LOG
        else
            echo "Error: failed to determine whether endpoint is running on ${node}" >> $LOG
            return 201
        fi
    done

    export ENDPOINT_RUNNING_COUNT
    export ENDPOINT_RUNNING_NODES

    return 0
}

stop_endpoints () {
    # if not already done, run check_endpoint_running_nodes
    if [ -z "${ENDPOINT_RUNNING_COUNT}" ]; then
        check_endpoint_running_nodes
        if [ $? -ne 0 ]; then
            echo "Error: failed to check nodes for running endpoints" >> $LOG
            return 201
        fi
    fi

    if [ -z ${ENDPOINT_PIDFILE+x} ]; then
        echo "Error: ENDPOINT_PIDFILE is unset" >> $LOG
        return 200
    fi

    echo "  stopping endpoints" >> $LOG
    for node in ${ENDPOINT_RUNNING_NODES[@]}; do
        echo "    stopping endpoint if running on ${node}" >> $LOG
        ssh -oStrictHostKeyChecking=no ${node} "${INIT_COMMAND}; check_daemon_process_owner && funcx-endpoint stop ${ENDPOINT_NAME}" >> $LOG 2>&1
    done

    # also delete endpoint pidfile for case where no endpoint was running but pidfile existed and another user owned the process
    if [ -f "${ENDPOINT_PIDFILE}" ]; then
        echo "    cleaning up endpoint pid file" >> $LOG
        rm -f "${ENDPOINT_PIDFILE}"
    fi

    unset ENDPOINT_RUNNING_NODES
    unset ENDPOINT_RUNNING_COUNT

    return 0
}

start_endpoint () {
    echo "  starting endpoint" >> $LOG

    # hostname of primary login node
    primary=$(ssh -oStrictHostKeyChecking=no ${PRIMARY_NODE} hostname)
    echo "     starting endpoint on ${primary}" >> $LOG

    # start endpoint on primary node
    ssh -oStrictHostKeyChecking=no ${primary} "${INIT_COMMAND}; funcx-endpoint start ${ENDPOINT_NAME}" >> $LOG 2>&1
    if [ $? -eq 0 ]; then
        echo "    started funcx '${ENDPOINT_NAME}' endpoint on ${primary}" >> $LOG
    else
        echo "error: could not start funcx '${ENDPOINT_NAME}' endpoint on ${primary}" >> $LOG
        return 201
    fi

    return 0
}

restart_endpoint () {
    stop_endpoints
    start_endpoint
}