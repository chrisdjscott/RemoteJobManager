
import os
import sys
import logging
from datetime import datetime

from rjm import utils
from rjm.remote_job import RemoteJob
from rjm.runners.funcx_slurm_batch_runner import FuncxSlurmBatchRunner


logger = logging.getLogger(__name__)


class RemoteJobBatch:
    """
    Class for managing a batch of RemoteJobs

    """
    def __init__(self):
        self._remote_jobs = []
        self._batch_runner = FuncxSlurmBatchRunner()

    def setup(self, remote_jobs_file: str, force: bool = False):
        """Setup the runner"""
        # timestamp to use when creating remote directories
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

        # read the list of local directories and create RemoteJobs
        local_dirs = self._read_jobs_file(remote_jobs_file)
        logger.info(f"Loaded {len(local_dirs)} local directories from {remote_jobs_file}")
        self._remote_jobs = []
        for local_dir in local_dirs:
            rj = RemoteJob(timestamp=timestamp)
            self._remote_jobs.append(rj)
            rj.setup(local_dir, force=force)

        # Globus auth
        scopes = self._batch_runner.get_globus_scopes()
        globus_cli = utils.handle_globus_auth(scopes)
        self._batch_runner.setup_globus_auth(globus_cli)

    def upload_and_start(self):
        """
        Setup the batch of remote jobs, upload files and start running.

        """
        logger.info("Uploading files and starting jobs")

        # loop over local directories and create RemoteJobs
        errors = []
        for rj in self._remote_jobs:
            try:
                rj.upload_and_start()
            except Exception as exc:
                msg = f"Upload and start failed for '{rj.get_local_dir()}': {exc}"
                logger.error(msg)
                errors.append(msg)
        self._handle_errors(errors)

    def wait_and_download(self, polling_interval=None):
        """
        Wait for jobs to complete and download once completed.

        """
        logger.info(f"Waiting and downloading {len(self._remote_jobs)} jobs")

        # now wait for the jobs to complete
        self._batch_runner.wait_and_download(self._remote_jobs, polling_interval=polling_interval)

    def _handle_errors(self, errors: list[str]):
        """
        Print summary of errors and exit

        :param errors: list of error messages

        """
        if len(errors):
            logger.error(f"{len(errors)} local directories failed, errors listed below:")
            for msg in errors:
                logger.error(msg)
            sys.exit(1)

    def _read_jobs_file(self, remote_jobs_file: str) -> list[str]:
        """
        Read list of local directories

        :param remote_jobs_file: File containing list of local directories
            to create remote jobs for
        :returns: List of local directories that exist

        """
        # open the file and read the lines
        with open(remote_jobs_file) as fh:
            local_dirs = fh.readlines()
        local_dirs = [d.strip() for d in local_dirs]

        # keep entries that exist in the filesystem
        local_dirs_exist = []
        for local_dir in local_dirs:
            if os.path.isdir(local_dir):
                local_dirs_exist.append(local_dir)
            else:
                logger.warning(f'Local directory does not exist: "{local_dir}" (skipping)')

        return local_dirs_exist
