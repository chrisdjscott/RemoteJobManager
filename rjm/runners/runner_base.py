
import logging

from retry.api import retry_call

from rjm import utils
from rjm import config as config_helper
from rjm.errors import RemoteJobRunnerError


logger = logging.getLogger(__name__)


class RunnerBase:
    """
    Base class for runner objects

    """
    def __init__(self, config=None):
        self._label = ""

        # load config
        if config is None:
            self._config = config_helper.load_config()
        else:
            self._config = config

        self._retry_tries = self._config.getint("RETRY", "tries", fallback=utils.DEFAULT_RETRY_TRIES)
        self._retry_backoff = self._config.getint("RETRY", "backoff", fallback=utils.DEFAULT_RETRY_BACKOFF)
        self._retry_delay = self._config.getint("RETRY", "delay", fallback=utils.DEFAULT_RETRY_DELAY)

    def _log(self, level, message, *args, **kwargs):
        """Add a label to log messages, identifying this specific RemoteJob"""
        logger.log(level, self._label + message, *args, **kwargs)

    def save_state(self):
        """Return state dict if required for restarting"""
        state_dict = {}

        return state_dict

    def load_state(self, state_dict):
        """Get saved state if required for restarting"""
        pass

    def get_upload_files(self):
        """If any files are required to be uploaded by this runner, list them here"""
        return []

    def get_globus_scopes(self):
        """If any Globus scopes are required, override this method and return them in a list"""
        return []

    def setup_globus_auth(self, globus_cli):
        """Do any Globus auth setup here, if required"""
        pass

    def make_remote_directory(self, prefix: list[str]):
        """
        Make one or more remote directories, using the given prefix(es).

        """
        raise NotImplementedError

    def check_directory_exists(self, directory_path):
        """Check the working directory exists"""
        # sanity check the directory exists on the remote
        dir_exists = self.run_function_with_retries(check_dir_exists, directory_path)
        if not dir_exists:
            self._log(logging.ERROR, f"The specified directory does not exist on remote: {directory_path}")
            raise RemoteJobRunnerError(f"The specified directory does not exist on remote: {directory_path}")

    def run_function(self, function, *args, **kwargs):
        """Run the given function and pass back the return value"""
        raise NotImplementedError

    def run_function_with_retries(self, function, *args, **kwargs):
        """Run the given function with retries if the function fails"""
        return retry_call(self.run_function, fargs=(function,) + args, fkwargs=kwargs,
                          tries=self._retry_tries, backoff=self._retry_backoff,
                          delay=self._retry_delay)

    def start(self):
        """Starts running the processing asynchronously"""
        raise NotImplementedError

    def wait(self, polling_interval=None):
        """Blocks until the processing has finished"""
        raise NotImplementedError

    def cancel(self):
        """Cancel the processing"""
        raise NotImplementedError


# function for joining two paths on funcx endpoint
def path_join(path1, path2):
    import os.path
    return os.path.join(path1, path2)


# function for checking directory exists on funcx endpoint
def check_dir_exists(dirpath):
    import os
    return os.path.isdir(dirpath)
