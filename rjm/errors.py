class RemoteJobRunnerError(Exception):
    """
    Errors related to running the job on the remote system.

    """


class RemoteJobTransfererError(Exception):
    """
    Errors related to transferring file to and from the remote system.

    """


class RemoteJobBatchError(Exception):
    """
    Errors related to running a batch of jobs

    """
