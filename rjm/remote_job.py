
import os
import logging
from datetime import datetime
import time
import json

from rjm import utils
from rjm import config as config_helper
from rjm.transferers import globus_https_transferer
from rjm.runners import funcx_slurm_runner


logger = logging.getLogger(__name__)


class RemoteJob:
    """
    A remote job is based on a local directory.

    - input files are uploaded from the local directory
    - commands are executed on the remote system
    - output files are downloaded and stored in the local directory

    """
    STATE_FILE = "remote_job.json"

    def __init__(self, timestamp=None):
        self._local_path = None

        # timestamp for working directory name
        self._timestamp = timestamp
        if self._timestamp is None:
            self._timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

        # load the config
        config = config_helper.load_config()
        self._uploads_file = config.get("FILES", "uploads_file")
        self._downloads_file = config.get("FILES", "downloads_file")

        # file transferer
        self._transfer = globus_https_transferer.GlobusHttpsTransferer(config=config)

        # remote runner
        self._runner = funcx_slurm_runner.FuncxSlurmRunner(config=config)

    def get_required_globus_scopes(self):
        """
        Return list of Globus auth scopes required by the RemoteJob.

        """
        required_scopes = self._transfer.get_globus_scopes()
        required_scopes.extend(self._runner.get_globus_scopes())

        return required_scopes

    def setup(self, local_dir, force=False):
        """
        Set up the remote job (authentication, remote directory...)

        """
        # the local directory this job is based on
        if not os.path.isdir(local_dir):
            raise ValueError(f'RemoteJob directory does not exist: "{local_dir}"')
        self._local_path = local_dir
        self._job_name = os.path.basename(local_dir)
        logger.info(f"Setting up remote job: {self._job_name}")

        # setting up transferer and runner
        self._transfer.set_local_directory(self._local_path)
        self._runner.set_local_directory(self._local_path)

        # state file
        self._uploaded = False
        self._downloaded = False
        self._run_started = False
        self._run_completed = False
        self._state_file = os.path.join(local_dir, self.STATE_FILE)

        # reading upload files
        with open(os.path.join(self._local_path, self._uploads_file)) as fh:
            upload_files = [fn.strip() for fn in fh.readlines() if len(fn.strip())]
        self._upload_files = []
        for fname in upload_files:
            fpath = os.path.join(self._local_path, fname)
            if os.path.exists(fpath):
                if os.path.isfile(fpath):
                    self._upload_files.append(fname)
                else:
                    logger.warning(f'Skipping upload file specified in "{self._uploads_file}" that is not a file: "{fpath}"')
            else:
                logger.warning(f'Skipping upload file specified in "{self._uploads_file}" that does not exist: "{fpath}"')
        logger.info(f"File to be uploaded: {self._upload_files}")

        # reading download files
        with open(os.path.join(self._local_path, self._downloads_file)) as fh:
            self._download_files = [f.strip() for f in fh.readlines() if len(f.strip())]
        for fn in self._download_files:
            if os.path.exists(os.path.join(self._local_path, fn)):
                logger.warning(f"Local file will be overwritten by download: {os.path.join(self._local_path, fn)}")
        logger.info(f"Files to be downloaded: {self._download_files}")

        # load saved state if any
        self._load_state(force)

        # handle Globus here
        self.do_globus_auth()

        # creating a remote directory for running in
        if self._transfer.get_remote_directory() is None:
            local_basename = os.path.basename(self._local_path)
            remote_path_tuple = self._transfer.make_unique_directory(f"{local_basename}-{self._timestamp}")
            self._runner.set_working_directory(remote_path_tuple)

        # save state and making remote dir
        self._save_state()

    def do_globus_auth(self):
        """Handle globus auth here"""
        required_scopes = self.get_required_globus_scopes()
        if len(required_scopes):
            globus_cli = utils.handle_globus_auth(required_scopes)
            self._transfer.setup_globus_auth(globus_cli)
            self._runner.setup_globus_auth(globus_cli)

    def cleanup(self):
        """
        Cleanup the remote job (delete remote directory...)

        """
        raise NotImplementedError

    def __repr__(self):
        return f'RemoteJob({self._job_name})'

    def _load_state(self, force):
        """
        Load the saved state, if any.

        """
        if os.path.exists(self._state_file) and not force:
            logger.debug(f"Loading state from: {self._state_file}")
            with open(self._state_file) as fh:
                state_dict = json.load(fh)
            logger.debug(f"Loading state: {state_dict}")

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
        elif not self._run_completed:
            logger.warning("Run must be completed before downloading files")
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
        elif not self._upload_files:
            logger.warning("Files must be uploaded before starting the run")
        else:
            logger.info("Starting run")
            self._run_started = self._runner.start()
            self._save_state()

    def run_wait(self, polling_interval=None):
        """Wait for the processing to complete"""
        if self._run_completed:
            logger.info("Run already completed")
        elif not self._run_started:
            logger.warning("Run must be started before it can complete")
        else:
            logger.info("Waiting for run to complete")
            self._run_completed = self._runner.wait(polling_interval=polling_interval)
            self._save_state()

    def upload_and_start(self):
        """
        Complete the upload files and start running steps.

        """
        self.upload_files()
        self.run_start()

    def wait_and_download(self, polling_interval=None):
        """
        Wait for the run to complete and then download files.

        """
        self.run_wait(polling_interval=polling_interval)
        self.download_files()

    def workflow(self):
        """do everything: upload, run, download"""
        self._upload_and_start()
        self.wait_and_download()

    def get_transferer(self):
        """Return transferer"""
        return self._transfer

    def get_runner(self):
        """Return runner"""
        return self._runner


# TODO:
#   - implement retries
#   - cleanup function that deletes the remote directory

if __name__ == "__main__":
    import sys

    utils.setup_logging()

    rj = RemoteJob()
    rj.setup(sys.arv[1])
    print(rj)

    print(">>> uploading files")
    rj.upload_files()

    print(">>> running script")
    rj.run_start()
    rj.run_wait()

    print(">>> downloading files")
    rj.download_files()
