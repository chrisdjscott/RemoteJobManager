
import os
import logging
from datetime import datetime
import concurrent.futures
import time
import json

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
    STATE_FILE = "remote_job.json"

    def __init__(self, local_dir, timestamp=None, max_threads=5):
        # the local directory this job is based on
        if not os.path.isdir(local_dir):
            raise ValueError(f'RemoteJob directory does not exist: "{local_dir}"')
        self._local_path = local_dir
        self._job_name = os.path.basename(local_dir)
        logger.info(f"Creating remote job: {self._job_name}")

        # max number of threads
        self._max_threads = max_threads

        # timestamp for working directory name
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

        # load the config
        config = utils.load_config()
        uploads_file = config.get("FILES", "uploads_file")
        downloads_file = config.get("FILES", "downloads_file")

        # state file
        self._uploaded = False
        self._downloaded = False
        self._run_started = False
        self._run_completed = False
        self._remote_path = None
        self._state_file = os.path.join(local_dir, self.STATE_FILE)

        # reading upload files
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

        # reading download files
        with open(os.path.join(self._local_path, downloads_file)) as fh:
            self._download_files = [f.strip() for f in fh.readlines() if len(f.strip())]
        for fn in self._download_files:
            if os.path.exists(os.path.join(self._local_path, fn)):
                logger.warning(f"Local file will be overwritten by download: {os.path.join(self._local_path, fn)}")
        logger.info(f"Download files: {self._download_files}")

        # file transferer
        self._transfer = globus_https_transferer.GlobusHttpsTransferer(self._local_path, config=config, max_threads=max_threads)

        # remote runner
        self._runner = funcx_slurm_runner.FuncxSlurmRunner(self._local_path, config=config, max_threads=max_threads)

        # load saved state if any
        self._load_state()

        # handle Globus here
        required_scopes = self._transfer.get_globus_scopes()
        required_scopes.extend(self._runner.get_globus_scopes())
        if len(required_scopes):
            globus_cli = utils.handle_globus_auth(required_scopes)
            self._transfer.setup_globus_auth(globus_cli)
            self._runner.setup_globus_auth(globus_cli)

        # creating a remote directory for running in
        if self._remote_path is None:
            self._remote_path = self._transfer.make_remote_directory(f"{self._local_path}-{timestamp}")
        logger.info(f"Remote working directory: {self._remote_path}")
        self._runner.set_working_directory(self._remote_path)

        # save state and making remote dir
        self._save_state()

    def __repr__(self):
        return f'RemoteJob({self._job_name})'

    def _load_state(self):
        """
        Load the saved state, if any.

        """
        if os.path.exists(self._state_file):
            logger.debug(f"Loading state from: {self._state_file}")
            with open(self._state_file) as fh:
                state_dict = json.load(fh)
            logger.debug(f"Loading state: {state_dict}")

            self._remote_path = state_dict["remote_directory"]
            self._uploaded = state_dict["uploaded"]
            self._run_started = state_dict["started_run"]
            self._run_completed = state_dict["finished_run"]
            self._downloaded = state_dict["downloaded"]

            if "transfer" in state_dict:
                self._transfer.load_state(state_dict["transfer"])
            if "runner" in state_dict:
                self._runner.load_state(state_dict["runner"])

    def _save_state(self):
        """
        Save the current state of the remote job so it can be resumed later.

        """
        state_dict = {
            "remote_directory": self._remote_path,
            "uploaded": self._uploaded,
            "started_run": self._run_started,
            "finished_run": self._run_completed,
            "downloaded": self._downloaded,
        }

        transfer_state = self._transfer.save_state()
        if len(transfer_state):
            state_dict["transfer"] = transfer_state

        runner_state = self._runner.save_state()
        if len(runner_state):
            state_dict["runner"] = runner_state

        logger.debug(f"Saving state: {state_dict}")
        with open(self._state_file, 'w') as fh:
            json.dump(state_dict, fh, indent=4)

    def upload_files(self):
        """Upload files to remote"""
        if self._uploaded:
            logger.info("Already uploaded files")
        else:
            logger.info("Uploading files...")
            upload_time = time.perf_counter()
            self._transfer.upload_files(self._upload_files)
            upload_time = time.perf_counter() - upload_time
            logger.debug(f"Uploaded files in {upload_time:.1f} seconds")
            self._uploaded = True
            self._save_state()

    def download_files(self):
        """Download file from remote"""
        if self._downloaded:
            logger.info("Already downloaded files")
        else:
            logger.info("Downloading files...")
            download_time = time.perf_counter()
            self._transfer.download_files(self._download_files)
            download_time = time.perf_counter() - download_time
            logger.debug(f"Downloaded files in {download_time:.1f} seconds")
            self._downloaded = True
            self._save_state()

    def run_start(self):
        """Start running the processing"""
        if self._run_started:
            logger.info("Run already started")
        else:
            logger.info("Starting run")
            self._runner.start()
            self._run_started = True
            self._save_state()

    def run_wait(self):
        """Wait for the processing to complete"""
        if self._run_completed:
            logger.info("Run already completed")
        else:
            logger.info("Waiting for run to complete")
            self._runner.wait()
            self._run_completed = True
            self._save_state()

    def workflow(self):
        """do everything: upload, run, download"""
        self.upload_files()
        self.run_start()
        self.run_wait()
        self.download_files()


# TODO:
#   - storing progress in dir, e.g. files uploaded and dirpath, slurm job id, slurm job completed, files downloaded...
#   - implement retries
#   - check config is ok by uploading a temporary file with unique name and then use funcx to ls that file
#   - option to pass config as args to init and only load config file if not all args are passed
#   - cleanup function that deletes the remote directory

if __name__ == "__main__":
    import sys

    utils.setup_logging()

    rj = RemoteJob(sys.argv[1])
    print(rj)
    print(">>> uploading files")
    rj.upload_files()

    print(">>> running script")
    rj.run_start()
    rj.run_wait()

    print(">>> downloading files")
    rj.download_files()
