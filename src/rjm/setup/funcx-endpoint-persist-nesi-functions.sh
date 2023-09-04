
export LOGIN_NODES=(mahuika01 mahuika02)
export PRIMARY_NODE=login.mahuika.nesi.org.nz
export ENDPOINT_NAME=default
export FUNCX_MODULE="globus-compute-endpoint/2.3.2-gimkl-2022a-Python-3.10.5"
export INIT_COMMAND="source /etc/profile; source ~/.funcx-endpoint-persist-nesi-functions.sh; module load ${FUNCX_MODULE}"
export ENDPOINT_PIDFILE="${HOME}/.globus_compute/${ENDPOINT_NAME}/daemon.pid"

cleanup_logs () {
    local EP_LOG_DIR=~/.globus_compute/${ENDPOINT_NAME}/HighThroughputEngine/worker_logs/

    # delete old funcx endpoint logs
    mkdir -p ${EP_LOG_DIR}
    local nold=$(find ${EP_LOG_DIR} -type f -mtime +10 | wc -l)
    find ${EP_LOG_DIR} -type f -mtime +10 -delete
    find ${EP_LOG_DIR}/* -type d -empty -delete > /dev/null 2>&1
    echo "  deleted ${nold} old globus-compute-endpoint log files"

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
            echo "    no running process with pid: $(cat "${ENDPOINT_PIDFILE}")"
            return 1
        elif [ "${puser}" != "${USER}" ]; then
            # user doesn't own process or no process exists
            echo "    process is owned by another user: ${puser}"
            return 1
        elif ! grep -qi "Globus Compute\|funcx" <<< "${pcomm}"; then
            # process is not running globus-compute-endpoint
            echo "    process is not running globus-compute-endpoint: ${pcomm}"
            return 1
        fi
    fi

    return 0
}

get_endpoint_id () {
    module load ${FUNCX_MODULE}
    ENDPOINT_ID=$(globus-compute-endpoint list | grep ${ENDPOINT_NAME} | awk -F '|' '{print $2}')
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
    globus-compute-endpoint list | grep "${ENDPOINT_NAME}" | grep Running > /dev/null
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
        echo "  checking for endpoint running on ${node}"
        ssh -oStrictHostKeyChecking=no ${node} "${INIT_COMMAND}; check_endpoint_running_local"
        if [ $? -eq 0 ]; then
            echo "    globus compute '${ENDPOINT_NAME}' endpoint is running on ${node}"
            ENDPOINT_RUNNING_COUNT=$((ENDPOINT_RUNNING_COUNT+1))
            ENDPOINT_RUNNING_NODES[${#ENDPOINT_RUNNING_NODES[@]}]="${node}"
        elif [ $? -eq 1 ]; then
            echo "    globus compute '${ENDPOINT_NAME}' endpoint is not running on ${node}"
        else
            echo "Error: failed to determine whether endpoint is running on ${node}"
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
            echo "Error: failed to check nodes for running endpoints"
            return 201
        fi
    fi

    if [ -z ${ENDPOINT_PIDFILE+x} ]; then
        echo "Error: ENDPOINT_PIDFILE is unset"
        return 200
    fi

    echo "  stopping endpoints"
    for node in ${ENDPOINT_RUNNING_NODES[@]}; do
        echo "    stopping endpoint if running on ${node}"
        ssh -oStrictHostKeyChecking=no ${node} "${INIT_COMMAND}; check_daemon_process_owner && globus-compute-endpoint stop ${ENDPOINT_NAME}"
    done

    # also delete endpoint pidfile for case where no endpoint was running but pidfile existed and another user owned the process
    if [ -f "${ENDPOINT_PIDFILE}" ]; then
        echo "    cleaning up endpoint pid file"
        rm -f "${ENDPOINT_PIDFILE}"
    fi

    unset ENDPOINT_RUNNING_NODES
    unset ENDPOINT_RUNNING_COUNT

    return 0
}

start_endpoint () {
    echo "  starting endpoint"

    # hostname of primary login node
    primary=$(ssh -oStrictHostKeyChecking=no ${PRIMARY_NODE} hostname)
    echo "     starting endpoint on ${primary}"

    # start endpoint on primary node
    ssh -tt -oStrictHostKeyChecking=no ${primary} "${INIT_COMMAND}; globus-compute-endpoint start ${ENDPOINT_NAME}"
    if [ $? -eq 0 ]; then
        echo "    started globus compute '${ENDPOINT_NAME}' endpoint on ${primary}"
    else
        echo "error: could not start globus compute '${ENDPOINT_NAME}' endpoint on ${primary}"
        return 201
    fi

    return 0
}

restart_endpoint () {
    stop_endpoints
    start_endpoint
}

migrate_to_globus_compute () {
    # copy the directory if required
    if [ ! -d "${HOME}/.globus_compute" ]; then
        echo "  copying ~/.funcx directory to ~/.globus_compute"
        cp -r "${HOME}/.funcx" "${HOME}/.globus_compute"
    fi

    # config file should exist
    config_file="${HOME}/.globus_compute/${ENDPOINT_NAME}/config.py"
    if [ ! -f "${config_file}" ]; then
        echo "Error: config file does not exist: \"${config_file}\""
        exit 1
    fi

    # do the migration, if needed
    if grep -q "from funcx_endpoint" "${config_file}"; then
        echo "  updating endpoint config for globus compute"
        sed -i'.funcx-bkp' 's/from funcx_endpoint/from globus_compute_endpoint/g' "${config_file}"
    fi
}
