
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
        self._runner = FuncxSlurmBatchRunner()

    def setup(self, remote_jobs_file: str, force: bool = False):
        """Setup the runner"""
        # timestamp to use when creating remote directories
        self._timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

        # read the list of local directories and create RemoteJobs
        local_dirs = self._read_jobs_file(remote_jobs_file)
        logger.info(f"Loaded {len(local_dirs)} local directories from {remote_jobs_file}")
        self._remote_jobs = []
        for local_dir in local_dirs:
            rj = RemoteJob(timestamp=self._timestamp)
            self._remote_jobs.append(rj)
            rj.setup(local_dir, force=force)

        # Globus auth
        scopes = self._runner.get_globus_scopes()
        globus_cli = utils.handle_globus_auth(scopes)
        self._runner.setup_globus_auth(globus_cli)

    def make_directories(self):
        """Make directories for the remote jobs"""
        # build the list of prefixes for the remote directories
        remote_base_path = None
        rjs = []
        prefixes = []
        for rj in self._remote_jobs:
            if remote_base_path is None:
                remote_base_path = rj.get_remote_base_directory()

            if rj.get_remote_directory() is None:
                # remote directory is based on local path basename
                local_basename = os.path.basename(rj.get_local_dir())
                prefixes.append(f"{local_basename}-{self._timestamp}")
                rjs.append(rj)

        # create the remote directories
        if len(rjs):
            remote_directories = self._runner.make_remote_directory(remote_base_path, prefixes)
            logger.debug(f"Created {len(remote_directories)} remote directories")

            # set remote directories on RemoteJob objects
            for rj, (remote_full_path, remote_basename) in zip(rjs, remote_directories):
                rj.set_remote_directory(remote_full_path, remote_basename)

    def upload_and_start(self):
        """
        Setup the batch of remote jobs, upload files and start running.

        """
        logger.info("Uploading files and starting jobs")

        # make remote directories
        self.make_directories()

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
        self._runner.wait_and_download(self._remote_jobs, polling_interval=polling_interval)

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
