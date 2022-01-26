
import os
import logging

from . import utils


logger = logging.getLogger(__name__)


class TransfererBase:
    """
    Base class for objects that transfer files between local and remote.

    """
    def __init__(self, local_path, config=None):
        # load config
        if config is None:
            self._config = utils.load_config()
        else:
            self._config = config

        self._remote_path = None
        self._local_path = local_path

    def get_globus_scopes(self):
        """If any Globus scopes are required, override this method and return them in a list"""
        return []

    def list_directory(self, path):
        """List the contents (just names) of the provided path (directory)"""
        raise NotImplementedError

    def make_directory(self, path):
        """Create a directory at the specified path"""
        raise NotImplementedError

    def make_remote_directory(self, prefix):
        """
        Create a directory on the remote end, for running the job in, trying to
        ensure it is unique.

        """
        # get a unique directory name based on the prefix
        workdirname = prefix
        got_dirname = False
        existing_names = self.list_directory(path="/")
        count = 0
        while not got_dirname:
            # check the directory does not already exist
            if workdirname in existing_names:
                count += 1
                workdirname = f"{prefix}-{count:06d}"
            else:
                got_dirname = True

        # create the directory
        logger.debug(f"Creating remote directory: {workdirname}")
        self.make_directory(workdirname)
        self._remote_path = workdirname

        return os.path.join(self._remote_base_path, self._remote_path)

    def setup_globus_auth(self, globus_cli):
        """Do any Globus auth setup here, if required"""
        pass

    def log_transfer_time(self, text, local_file, elapsed_time, log_method="debug"):
        """Report the time taken to upload/download a file"""
        file_size = os.path.getsize(local_file)
        file_size, file_size_units = utils.pretty_size_from_bytes(file_size)
        log = getattr(logger, log_method)
        log(f"{text} {local_file}: {file_size:.1f} {file_size_units} in {elapsed_time:.1f} s ({file_size / elapsed_time:.1f} {file_size_units}/s)")

    def upload_file(self, filename):
        """Method to be extended"""
        raise NotImplementedError

    def download_file(self, filename):
        """Method to be extended"""
        raise NotImplementedError
