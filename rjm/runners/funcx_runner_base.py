
import sys
import logging

from funcx.sdk.client import FuncXClient
from funcx.sdk.executor import FuncXExecutor

from rjm import utils
from rjm.errors import RemoteJobRunnerError
from rjm.runners.runner_base import RunnerBase


FUNCX_TIMEOUT = 180  # default timeout for waiting for funcx functions

logger = logging.getLogger(__name__)


class FuncxRunnerBase(RunnerBase):
    """
    Base class for FuncX runners

    """
    def __init__(self, config=None):
        super(FuncxRunnerBase, self).__init__(config=config)

        # the FuncX endpoint on the remote machine
        self._funcx_endpoint = self._config.get("FUNCX", "remote_endpoint")

        # funcx client and executor
        self._funcx_client = None
        self._funcx_executor = None

    def _log(self, level, message, *args, **kwargs):
        """Add a label to log messages, identifying this specific RemoteJob"""
        logger.log(level, self._label + message, *args, **kwargs)

    def __repr__(self):
        return f"FuncxBaseRunner({self._funcx_endpoint})"

    def get_globus_scopes(self):
        """If any Globus scopes are required, override this method and return them in a list"""
        self._required_scopes = [
            utils.OPENID_SCOPE,
            utils.SEARCH_SCOPE,
            utils.FUNCX_SCOPE,
        ]

        return self._required_scopes

    def setup_globus_auth(self, globus_cli):
        """Do any Globus auth setup here, if required"""
        # offprocess checker not working well with freezing currently
        if getattr(sys, "frozen", False):
            # application is frozen
            use_offprocess_checker = False
            self._log(logging.DEBUG, "Disabling offprocess_checker when frozen")
        else:
            use_offprocess_checker = True

        # setting up the FuncX client
        authorisers = globus_cli.get_authorizers_by_scope(requested_scopes=self._required_scopes)
        self._funcx_client = FuncXClient(
            fx_authorizer=authorisers[utils.FUNCX_SCOPE],
            search_authorizer=authorisers[utils.SEARCH_SCOPE],
            openid_authorizer=authorisers[utils.OPENID_SCOPE],
            use_offprocess_checker=use_offprocess_checker,
        )

        # create a funcX executor
        self._funcx_executor = FuncXExecutor(self._funcx_client)

    def run_function(self, function, *args, **kwargs):
        """Run the given function and pass back the return value"""
        if self._funcx_executor is None:
            self._log(logging.ERROR, "Make sure you setup_globus_auth before trying to run something")
            raise RuntimeError("Make sure you setup_globus_auth before trying to run something")

        # start the function
        self._log(logging.DEBUG, f"Submitting function to FuncX executor: {function}")
        future = self._funcx_executor.submit(function, *args, endpoint_id=self._funcx_endpoint, **kwargs)

        # wait for it to complete and get the result
        self._log(logging.DEBUG, "Waiting for FuncX function to complete")
        result = future.result(timeout=FUNCX_TIMEOUT)

        return result

    def make_remote_directory(self, remote_base_path, prefix):
        """
        Make one or more remote directories, using the given prefix(es).

        :param remote_base_path: The base path on the remote machine to create
            the directories in
        :param prefix: Single prefix, or list of prefixes, for remote directories

        :return: Tuple, or list of tuples, containing the full remote path and
            remote path relative to the base path

        """
        # remote function expects a list
        if type(prefix) is not list:
            single = True
            prefix_list = [prefix]
        else:
            single = False
            prefix_list = prefix

        # run the remote function
        self._log(logging.DEBUG, f"Creating remote directories for: {prefix_list}")
        self._log(logging.DEBUG, f"Creating remote directories in: {remote_base_path}")
        remote_dirs = self.run_function_with_retries(_make_remote_directories, remote_base_path, prefix_list)

        # handle result
        if type(remote_dirs) is list:  # success
            if single:  # if single value passed in, return single value
                remote_dirs = remote_dirs[0]
        else:  # failed with error message passed back
            raise RemoteJobRunnerError(f"Make remote directory failed: {remote_dirs}")

        return remote_dirs


def _make_remote_directories(base_path, prefixes):
    try:
        import os
        import tempfile

        remote_dirs = []
        for prefix in prefixes:
            remote_full_path = tempfile.mkdtemp(prefix=prefix + "-", dir=base_path)
            os.chmod(remote_full_path, 0o755)
            remote_dirs.append((remote_full_path, os.path.relpath(remote_full_path, start=base_path)))

    except Exception as exc:
        remote_dirs = repr(exc)

    return remote_dirs
