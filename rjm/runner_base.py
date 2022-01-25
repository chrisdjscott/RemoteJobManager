
import logging

from . import utils


logger = logging.getLogger(__name__)


class RunnerBase:
    """
    Base class for runner objects

    """
    def __init__(self, config=None):
        # load config
        if config is None:
            self._config = utils.load_config()
        else:
            self._config = config

        self._cwd = None

    def get_upload_files(self):
        """If any files are required to be uploaded by this runner, list them here"""
        return []

    def get_globus_scopes(self):
        """If any Globus scopes are required, override this method and return them in a list"""
        return []

    def setup_globus_auth(self, globus_cli):
        """Do any Globus auth setup here, if required"""
        pass

    def set_working_directory(self, working_dir):
        """Set the remote working directory"""
        self._cwd = working_dir
        logger.debug(f"Set remote working directory to: {working_dir}")

        # sanity check the directory exists on the remote
        dir_exists = self.run_function(check_dir_exists, self._cwd)
        if not dir_exists:
            logger.error(f"The specified working directory does not exist on remote: {self._cwd}")
            raise ValueError(f"The specified working directory does not exist on remote: {self._cwd}")

    def run_function(self, function, *args, **kwargs):
        """Run the given function and pass back the return value"""
        raise NotImplementedError

    def start_script(self, script_name):
        """Starts running the given script and returns an id to identify the script"""
        raise NotImplementedError

    def wait_for_script(self, script_id):
        """Wait fof the script to stop running"""
        raise NotImplementedError


# function for checking directory exists on funcx endpoint
def check_dir_exists(dirpath):
    import os
    return os.path.isdir(dirpath)
