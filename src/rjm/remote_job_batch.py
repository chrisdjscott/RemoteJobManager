
import os
import sys
import time
import logging
import concurrent.futures
from datetime import datetime

from rjm import utils
from rjm.errors import RemoteJobBatchError
from rjm.remote_job import RemoteJob
from rjm.runners.globus_compute_slurm_runner import GlobusComputeSlurmRunner
from rjm.transferers.globus_https_transferer import GlobusHttpsTransferer


logger = logging.getLogger(__name__)


class RemoteJobBatch:
    """
    Class for managing a batch of RemoteJobs

    """
    def __init__(self):
        self._remote_jobs = []
        self._runner = GlobusComputeSlurmRunner()
        self._transfer = GlobusHttpsTransferer()

    def setup(self, remote_jobs_file: str, force: bool = False):
        """Setup the runner"""
        # timestamp to use when creating remote directories
        self._timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

        # Globus auth
        scopes = self._runner.get_globus_scopes()
        scopes.extend(self._transfer.get_globus_scopes())
        globus_cli = utils.handle_globus_auth(scopes)
        self._runner.setup_globus_auth(globus_cli)
        self._transfer.setup_globus_auth(globus_cli)

        # read the list of local directories and create RemoteJobs
        local_dirs = self._read_jobs_file(remote_jobs_file)
        logger.info(f"Loaded {len(local_dirs)} local directories from {remote_jobs_file}")
        self._remote_jobs = []
        for local_dir in local_dirs:
            rj = RemoteJob(timestamp=self._timestamp)
            self._remote_jobs.append(rj)
            rj.setup(local_dir, force=force, runner=self._runner, transfer=self._transfer)

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
        Upload files and start jobs

        """
        logger.info(f"Uploading files and starting {len(self._remote_jobs)} jobs")

        # create directories
        self.make_directories()

        # categorising remote_jobs
        unuploaded_jobs, unstarted_jobs, unfinished_jobs, undownloaded_jobs = self._categorise_jobs()
        if len(unfinished_jobs):
            logger.info(f"Skipping {len(unfinished_jobs)} jobs that have already started running")
        if len(undownloaded_jobs):
            logger.info(f"Skipping {len(undownloaded_jobs)} jobs that have already finished running")
        if len(unuploaded_jobs):
            logger.info(f"{len(unuploaded_jobs)} jobs are ready to be uploaded and started")
        if len(unstarted_jobs):
            logger.info(f"{len(unstarted_jobs)} jobs are ready to be started")

        # tracking errors to report later
        errors = []

        # executor for processing uploads
        future_to_rj = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as uploader:  # separate thread for uploading
            # upload files
            for rj in unuploaded_jobs:
                future_to_rj[uploader.submit(rj.upload_files)] = rj

            # start jobs that were already uploaded but not started
            for rj in unstarted_jobs:
                try:
                    rj.run_start()
                except Exception as exc:
                    errors.append(repr(exc))
                    logger.error(repr(exc))

            # start jobs as their uploads complete
            if len(future_to_rj):
                for future in concurrent.futures.as_completed(future_to_rj):
                    rj = future_to_rj[future]
                    logger.debug(f"Received upload result for {rj}")
                    try:
                        future.result()
                    except Exception as exc:
                        errors.append(repr(exc))
                        logger.error(repr(exc))
                    else:
                        # upload succeeded, now start the job
                        try:
                            logger.debug(f"Starting run for {rj}")
                            rj.run_start()
                        except Exception as exc:
                            errors.append(repr(exc))
                            logger.error(repr(exc))

        # handle errors
        logger.debug(f"{len(errors)} errors to report")
        if len(errors):
            raise RemoteJobBatchError(errors)

    def _categorise_jobs(self):
        """
        Categorise RemoteJobs based on their current status

        :returns: tuple containing lists of RemoteJobs that:
            - haven't had their files uploaded yet
            - have uploaded file but haven't started running yet
            - have started running but haven't completed running yet
            - jave completed running but haven't downloaded files yet

        """
        unuploaded_jobs = []
        unstarted_jobs = []
        unfinished_jobs = []
        undownloaded_jobs = []
        for rj in self._remote_jobs:
            if not rj.files_uploaded():
                logger.debug(f"{rj} has not uploaded files yet")
                unuploaded_jobs.append(rj)
            elif not rj.run_started():
                logger.debug(f"{rj} has not started yet")
                unstarted_jobs.append(rj)
            elif not rj.run_completed():
                logger.debug(f"{rj} has not completed yet")
                unfinished_jobs.append(rj)
            elif not rj.files_downloaded():
                logger.debug(f"{rj} has not downloaded files yet")
                undownloaded_jobs.append(rj)
            else:
                logger.info(f"{rj} is done")

        return unuploaded_jobs, unstarted_jobs, unfinished_jobs, undownloaded_jobs

    def wait_and_download(self, polling_interval=None, min_polling_override=False):
        """
        Wait for jobs to complete and download once completed.

        """
        logger.info(f"Waiting and downloading {len(self._remote_jobs)} jobs")

        # override polling interval from config file?
        polling_interval = self._runner.get_poll_interval(polling_interval, min_polling_override=min_polling_override)

        # categorising remote_jobs
        unuploaded_jobs, unstarted_jobs, unfinished_jobs, undownloaded_jobs = self._categorise_jobs()

        # add errors for unuploaded and unstarted
        errors = []
        for rj in unuploaded_jobs:
            errors.append(f"Cannot wait for {rj} that hasn't uploaded files")
        for rj in unstarted_jobs:
            errors.append(f"Cannot wait for {rj} that hasn't started running")
        for err in errors:
            logger.error(err)

        logger.info(f"{len(undownloaded_jobs)} jobs to be downloaded")
        logger.info(f"{len(unfinished_jobs)} jobs to wait for and download")

        # executor for processing downloads
        future_to_rj = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as downloader:  # separate thread for downloading
            # first download jobs that have finished but not downloaded already
            for rj in undownloaded_jobs:
                future_to_rj[downloader.submit(rj.download_files)] = rj

            # loop until jobs have finished
            logger.info(f"Waiting for {len(unfinished_jobs)} Slurm jobs to finish")
            logger.debug(f"Polling interval is: {polling_interval} seconds")
            count_succeeded = 0
            count_failed = 0
            while len(unfinished_jobs):
                # get the finished status
                logger.debug(f"Checking statuses of {len(unfinished_jobs)} jobs")
                successful_jobs, failed_jobs, unfinished_jobs = self._runner.check_finished_jobs(unfinished_jobs)
                count_succeeded += len(successful_jobs)
                count_failed += len(failed_jobs)
                logger.info(f"{count_succeeded} succeeded; {count_failed} failed; {len(unfinished_jobs)} unfinished")

                # handle successful jobs
                for rj in successful_jobs:
                    logger.info(f"{rj} run has finished successfully")
                    rj.set_run_completed()
                    future_to_rj[downloader.submit(rj.download_files)] = rj

                # handle unsuccessful jobs
                for rj in failed_jobs:
                    logger.info(f"{rj} run has finished unsuccessfully")
                    rj.set_run_completed(success=False)
                    future_to_rj[downloader.submit(rj.download_files)] = rj

                # wait before checking for finished jobs again
                if len(unfinished_jobs):
                    time.sleep(polling_interval)

            # wait for downloads to complete
            if len(future_to_rj):
                for future in concurrent.futures.as_completed(future_to_rj):
                    rj = future_to_rj[future]
                    logger.debug(f"Received download result for {rj}")
                    try:
                        future.result()
                    except Exception as exc:
                        errors.append(repr(exc))
                        logger.error(repr(exc))

        # handle errors
        logger.debug(f"{len(errors)} errors to report")
        if len(errors):
            raise RemoteJobBatchError(errors)

    def write_stderr_for_unfinshed_jobs(self, msg):
        """
        Write stderr files for WFN compatibility for jobs that have not finished

        """
        for rj in self._remote_jobs:
            rj.write_stderr_if_not_finished(msg)

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
