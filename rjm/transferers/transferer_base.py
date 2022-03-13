
import os
import logging
from typing import List

from rjm import utils
from rjm import config as config_helper


logger = logging.getLogger(__name__)


class TransfererBase:
    """
    Base class for objects that transfer files between local and remote.

    """
    def __init__(self, config=None):
        # load config
        if config is None:
            self._config = config_helper.load_config()
        else:
            self._config = config

        self._remote_base_path = None
        self._remote_path = None
        self._local_path = None
        self._label = ""

    def _log(self, level, message, *args, **kwargs):
        """Add a label to log messages, identifying this specific RemoteJob"""
        logger.log(level, self._label + message, *args, **kwargs)

    def save_state(self):
        """Return state dict if required for restarting"""
        state_dict = {}

        # need the remote path
        if self._remote_path is not None:
            state_dict["remote_path"] = self._remote_path

        return state_dict

    def load_state(self, state_dict):
        """Get saved state if required for restarting"""
        if "remote_path" in state_dict:
            self._remote_path = state_dict["remote_path"]

    def get_globus_scopes(self):
        """If any Globus scopes are required, override this method and return them in a list"""
        return []

    def list_directory(self, path):
        """List the contents (just names) of the provided path (directory)"""
        raise NotImplementedError

    def set_local_directory(self, local_dir):
        """Set the local directory"""
        self._local_path = local_dir
        self._label = f"[{os.path.basename(local_dir)}] "

    def set_remote_directory(self, remote_path):
        """Set the remote directory to the given value"""
        self._remote_path = remote_path

    def make_directory(self, path):
        """Create a directory at the specified path"""
        raise NotImplementedError

    def get_remote_base_directory(self):
        """Return the base directory on the remote system"""
        return self._remote_base_path

    def get_remote_directory(self):
        """
        Return tuple with two components: path to globus share and relative
        path to remote directory within share

        """
        return None if self._remote_path is None else (self._remote_base_path, self._remote_path)

    def setup_globus_auth(self, globus_cli):
        """Do any Globus auth setup here, if required"""
        pass

    def log_transfer_time(self, text: str, local_file: str, elapsed_time: float, log_level: int = logging.DEBUG):
        """Report the time taken to upload/download a file"""
        file_size = os.path.getsize(local_file)
        file_size, file_size_units = utils.pretty_size_from_bytes(file_size)
        self._log(log_level, f"{text} {local_file}: {file_size:.1f} {file_size_units} in {elapsed_time:.1f} s ({file_size / elapsed_time:.1f} {file_size_units}/s)")

    def upload_files(self, filenames: List[str]):
        """
        Upload the given files (which should be relative to `local_path`) to
        the remote directory.

        :param filenames: List of file names relative to the `local_path`
            directory to upload to the remote directory.
        :type filenames: iterable of str

        """
        raise NotImplementedError

    def download_files(self, filenames: List[str]):
        """
        Download the given files (which should be relative to `remote_path`) to
        the local directory.

        :param filenames: List of file names relative to the `remote_path`
            directory to download to the local directory.
        :type filenames: iterable of str

        """
        raise NotImplementedError
