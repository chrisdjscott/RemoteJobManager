
import logging

from rjm import config as config_helper


logger = logging.getLogger(__name__)


class RunnerBase:
    """
    Base class for runner objects

    """
    def __init__(self, config=None):
        self._local_path = None

        # load config
        if config is None:
            self._config = config_helper.load_config()
        else:
            self._config = config

        self._cwd = None

    def save_state(self):
        """Return state dict if required for restarting"""
        state_dict = {}

        # save working directory
        if self._cwd is not None:
            state_dict["working_directory"] = self._cwd

        return state_dict

    def load_state(self, state_dict):
        """Get saved state if required for restarting"""
        if "working_directory" in state_dict:
            self._cwd = state_dict["working_directory"]

    def get_upload_files(self):
        """If any files are required to be uploaded by this runner, list them here"""
        return []

    def get_globus_scopes(self):
        """If any Globus scopes are required, override this method and return them in a list"""
        return []

    def setup_globus_auth(self, globus_cli):
        """Do any Globus auth setup here, if required"""
        pass

    def set_local_directory(self, local_dir):
        """Set the local directory"""
        self._local_path = local_dir

    def set_working_directory(self, working_dir_tuple):
        """Set the remote working directory"""
        self._cwd = self.run_function(path_join, working_dir_tuple[0], working_dir_tuple[1])
        logger.debug(f"Setting remote working directory to: {self._cwd}")

        # sanity check the directory exists on the remote
        dir_exists = self.run_function(check_dir_exists, self._cwd)
        if not dir_exists:
            logger.error(f"The specified working directory does not exist on remote: {self._cwd}")
            raise ValueError(f"The specified working directory does not exist on remote: {self._cwd}")

    def run_function(self, function, *args, **kwargs):
        """Run the given function and pass back the return value"""
        raise NotImplementedError

    def start(self):
        """Starts running the processing asynchronously"""
        raise NotImplementedError

    def wait(self, polling_interval=None):
        """Blocks until the processing has finished"""
        raise NotImplementedError


# function for joining two paths on funcx endpoint
def path_join(path1, path2):
    import os.path
    return os.path.join(path1, path2)


# function for checking directory exists on funcx endpoint
def check_dir_exists(dirpath):
    import os
    return os.path.isdir(dirpath)
