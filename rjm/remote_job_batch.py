
import os
import sys
import logging
from datetime import datetime

from rjm.remote_job import RemoteJob


logger = logging.getLogger(__name__)


class RemoteJobBatch:
    """
    Class for managing a batch of RemoteJobs

    """
    def __init__(self):
        self._remote_jobs = []

    def upload_and_start(self, remote_jobs_file: str, force: bool = False):
        """
        Setup the batch of remote jobs, upload files and start running.

        :param remote_jobs_file: File containing list of local directories
            to create remote jobs for
        :param force: Optional, ignore RemoteJob progress and start from
            scratch (default: False)

        """
        logger.info("Uploading files and starting jobs")

        # read the list of local directories
        local_dirs = self._read_jobs_file(remote_jobs_file)
        logger.info(f"Loaded {len(local_dirs)} local directories from {remote_jobs_file}")

        # timestamp to use when creating remote directories
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

        # loop over local directories and create RemoteJobs
        self._remote_jobs = {}
        errors = []
        for local_dir in local_dirs:
            try:
                rj = RemoteJob(timestamp=timestamp)
                self._remote_jobs[local_dir] = rj
                rj.setup(local_dir, force=force)
                rj.upload_and_start()
            except Exception as exc:
                msg = f"Upload and start failed for '{local_dir}': {exc}"
                logger.error(msg)
                errors.append(msg)
        self._handle_errors(errors)

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
