
import os
import logging
from datetime import datetime

import requests

from . import utils
from . import globus_https_transferer
from . import funcx_slurm_runner


logger = logging.getLogger(__name__)


class RemoteJob:
    """
    A remote job is based on a local directory.

    - input files are uploaded from the local directory
    - commands are executed on the remote system
    - output files are downloaded and stored in the local directory

    """
    def __init__(self, local_dir, remote_dir_prefix=None):
        # the local directory this job is based on
        if not os.path.isdir(local_dir):
            raise ValueError(f'RemoteJob directory does not exist: "{local_dir}"')
        self._local_path = local_dir
        self._job_name = os.path.basename(local_dir)
        logger.info(f"Creating remote job: {self._job_name}")

        # load the config
        config = utils.load_config()
        uploads_file = config.get("FILES", "uploads_file")
        downloads_file = config.get("FILES", "downloads_file")

        # reading upload/download files
        with open(os.path.join(self._local_path, uploads_file)) as fh:
            upload_files = [fn.strip() for fn in fh.readlines() if len(fn.strip())]
        self._upload_files = []
        for fname in upload_files:
            fpath = os.path.join(self._local_path, fname)
            if os.path.exists(fpath):
                if os.path.isfile(fpath):
                    self._upload_files.append(fname)
                else:
                    logger.warning(f'Skipping upload file specified in "{uploads_file}" that is not a file: "{fpath}"')
            else:
                logger.warning(f'Skipping upload file specified in "{uploads_file}" that does not exist: "{fpath}"')
        logger.info(f"Upload files: {self._upload_files}")

        with open(os.path.join(self._local_path, downloads_file)) as fh:
            self._download_files = [f.strip() for f in fh.readlines() if len(f.strip())]
        for fn in self._download_files:
            if os.path.exists(os.path.join(self._local_path, fn)):
                logger.warning(f"Local file will be overwritten by download: {os.path.join(self._local_path, fn)}")
        logger.info(f"Download files: {self._download_files}")

        # file transferer
        self._transfer = globus_https_transferer.GlobusHttpsTransferer(self._local_path, config=config)

        # remote runner
        self._runner = funcx_slurm_runner.FuncxSlurmRunner(config=config)

        # handle Globus here
        required_scopes = self._transfer.get_globus_scopes()
        required_scopes.extend(self._runner.get_globus_scopes())
        if len(required_scopes):
            globus_cli = utils.handle_globus_auth(required_scopes)
            self._transfer.setup_globus_auth(globus_cli)
            self._runner.setup_globus_auth(globus_cli)

        # creating a remote directory for running in
        if remote_dir_prefix is None:
            remote_dir_prefix = datetime.now().strftime("rjm.%Y%m%dT%H%M%S")
        remote_work_dir = self._transfer.make_remote_directory(remote_dir_prefix)
        logger.info(f"Remote working directory: {remote_work_dir}")
        self._runner.set_working_directory(remote_work_dir)


    def __repr__(self):
        return f'RemoteJob({self._job_name})'

    def upload_files(self):
        """Upload files to remote"""
        for fname in self._upload_files:
            self._transfer.upload_file(fname)

    def download_files(self, missing_ok=True):
        """Download file from remote"""
        for fname in self._download_files + ["rjm_start_script.txt"]:
            if missing_ok:
                # don't fail if file doesn't exist, just print a warning
                try:
                    self._transfer.download_file(fname)
                except requests.exceptions.HTTPError as exc:
                    logger.warning(f"Failed to download file '{self._local_path}/{fname}': {exc}")
            else:
                self._transfer.download_file(fname)

    def run_script(self, script_name):
        """Run the given script"""
        script_id = self._runner.start_script(script_name)
        self._runner.wait_for_script(script_id)







# TODO:
#   - storing progress in dir, e.g. files uploaded and dirpath, slurm job id, slurm job completed, files downloaded...
#   - implement retries
#   - check config is ok by uploading a temporary file with unique name and then use funcx to ls that file
#   - maybe have a separate config module that does that checking
#   - option to pass config as args to init and only load config file if not all args are passed
#   - cleanup functions that deletes the remote directory

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("globus_sdk").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    rj = RemoteJob(sys.argv[1])
    print(rj)
    print(">>> uploading files")
    rj.upload_files()

    print(">>> running script")
    rj.run_script("run.sl")

    print(">>> downloading files")
    rj.download_files()
