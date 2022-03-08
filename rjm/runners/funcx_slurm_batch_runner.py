
import time
import logging
import concurrent.futures

from retry.api import retry_call

from rjm.remote_job import RemoteJob
from rjm.runners.funcx_runner_base import FuncxRunnerBase
from rjm.errors import RemoteJobRunnerError


logger = logging.getLogger(__name__)


class FuncxSlurmBatchRunner(FuncxRunnerBase):
    """
    This class can handle batches of remote jobs.

    """
    def __init__(self, config=None):
        super(FuncxSlurmBatchRunner, self).__init__(config=config)

        # how often to poll for Slurm job completion
        self._poll_interval = self._config.getint("SLURM", "poll_interval")

    def wait_and_download(self, remote_jobs: list[RemoteJob], polling_interval=None):
        """
        Wait for the jobs to finish and download files

        """
        # override polling interval from config file?
        if polling_interval is None:
            polling_interval = self._poll_interval

        # categorising remote_jobs
        errors = []
        unfinished_jobs = {}
        undownloaded_jobs = []
        for rj in remote_jobs:
            if not rj.run_started():  # trying to wait for jobs that haven't started yet is an error
                msg = f"{rj} cannot wait for job that has not started"
                logger.error(msg)
                errors.append(msg)
            elif rj.run_completed() and not rj.files_downloaded():  # jobs that have finished but need to be downloaded
                logger.debug(f"{rj} has already completed but not downloaded")
                undownloaded_jobs.append(rj)
            elif rj.files_downloaded():  # skip jobs that have already finished and been downloaded
                logger.info(f"{rj} has already completed and downloaded")
            else:  # jobs that we need to wait for and download
                r = rj.get_runner()
                jobid = r.get_jobid()
                unfinished_jobs[jobid] = rj

        # executor for processing downloads
        future_to_rj = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as downloader:  # separate thread for downloading
            # first download jobs that have finished but not downloaded already
            for rj in undownloaded_jobs:
                future_to_rj[downloader.submit(rj.download_files)] = rj

            # loop until jobs have finished
            logger.info(f"Waiting for {len(unfinished_jobs)} Slurm jobs to finish")
            logger.debug(f"Polling interval is: {polling_interval} seconds")
            while len(unfinished_jobs):
                logger.debug(f"Checking statuses of {len(unfinished_jobs)} jobs")
                # retry the remote function call
                job_status_text = retry_call(
                    self._check_slurm_jobs_wrapper,
                    fargs=(list(unfinished_jobs.keys()),),
                    tries=self._retry_tries,
                    backoff=self._retry_backoff,
                    delay=self._retry_delay,
                )

                # parse output for statuses
                for line in job_status_text.split("\n"):
                    try:
                        jobid, job_status = line.split("|")
                    except ValueError as exc:
                        msg = f"Error parsing job status line: '{line}' (line.split('|'))"
                        logger.error(repr(exc))
                        logger.error(msg)
                        raise RemoteJobRunnerError(msg)

                    if len(job_status) and job_status not in ("RUNNING", "PENDING"):
                        # job has finished
                        rj = unfinished_jobs.pop(jobid)
                        logger.info(f"{rj} has finished ({jobid}: {job_status})")
                        rj.set_run_completed()
                        future_to_rj[downloader.submit(rj.download_files)] = rj

                # wait before checking again
                if len(unfinished_jobs):
                    time.sleep(polling_interval)

            # wait for downloads to complete
            if len(future_to_rj):
                for future in concurrent.futures.as_completed(future_to_rj):
                    rj = future_to_rj[future]
                    try:
                        future.result()
                    except Exception as exc:
                        errors.append(repr(exc))
                        logger.error(repr(exc))

        # handle errors
        if len(errors):
            raise RemoteJobRunnerError(errors)

    def _check_slurm_jobs_wrapper(self, unfinished_jobids):
        """Wrapper function that raises exception if returncode is nonzero"""
        returncode, job_status_text = self.run_function(_check_slurm_job_statuses, unfinished_jobids)

        if returncode != 0:
            msg = f"Checking job statuses failed ({returncode}): {job_status_text}"
            logger.error(msg)
            raise RemoteJobRunnerError(msg)

        return job_status_text


def _check_slurm_job_statuses(jobids):
    """Return statuses for given jobids"""
    # have to load modules within the function
    import subprocess

    # query the status of the job using sacct
    cmd_args = ['sacct', '-X', '-o', 'JobID,State', '-n', '-P']
    for jobid in jobids:
        cmd_args.extend(['-j', jobid])

    p = subprocess.run(cmd_args, universal_newlines=True,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)

    return p.returncode, p.stdout.strip()
