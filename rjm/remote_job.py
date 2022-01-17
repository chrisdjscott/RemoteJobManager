
import os
import logging
import configparser

from . import utils


logger = logging.getLogger(__name__)


class RemoteJob:
    """
    A remote job is based on a local directory.

    - input files are uploaded from the local directory
    - commands are executed on the remote system
    - output files are downloaded and stored in the local directory

    """
    def __init__(self, local_dir):
        # the local directory this job is based on
        if not os.path.isdir(local_dir):
            raise ValueError(f'RemoteJob directory does not exist: "{local_dir}"')
        self._local_path = local_dir
        self._job_name = os.path.basename(local_dir)
        logger.info(f"Creating remote job: {self._job_name}")

        # load the config
        if os.path.exists(utils.CONFIG_FILE_LOCATION):
            config = configparser.ConfigParser()
            config.read(utils.CONFIG_FILE_LOCATION)
            self._remote_endpoint = config.get("GLOBUS", "remote_endpoint")
            self._remote_path = config.get("GLOBUS", "remote_path")
            self._funcx_endpoint = config.get("FUNCX", "remote_endpoint")
            uploads_file = config.get("FILES", "uploads_file")
            downloads_file = config.get("FILES", "downloads_file")
        else:
            raise RuntimeError(f"Config file does not exist: {utils.CONFIG_FILE_LOCATION}")

        # reading upload/download files
        with open(os.path.join(self._local_path, uploads_file)) as fh:
            upload_files = [fn.strip() for fn in fh.readlines() if len(fn.strip())]
        self._upload_files = []
        for fname in upload_files:
            fpath = os.path.join(self._local_path, fname)
            if os.path.exists(fpath):
                self._upload_files.append(fpath)
            else:
                logger.warning(f'Skipping upload file specified in "{uploads_file}" that does not exist: "{fpath}"')
        logger.debug(f"Upload files: {self._upload_files}")

        with open(os.path.join(self._local_path, downloads_file)) as fh:
            self._download_files = [f.strip() for f in fh.readlines() if len(f.strip())]
        for fn in self._download_files:
            if os.path.exists(os.path.join(self._local_path, fn)):
                logger.warning(f"Local file will be overwritten by download: {os.path.join(self._local_path, fn)}")
        logger.debug(f"Download files: {self._download_files}")



    def __repr__(self):
        return f'RemoteJob({self._job_name})'




# TODO:
#   - check config is ok by uploading a temporary file with unique name and then use funcx to ls that file
#   - maybe have a separate config module that does that checking
#   - option to pass config as args to init and only load config file if not all args are passed

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.WARNING)
    logger.setLevel(logging.DEBUG)
    rj = RemoteJob(sys.argv[1])
    print(rj)
