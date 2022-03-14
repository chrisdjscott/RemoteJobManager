
import os
import logging

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

    def get_poll_interval(self):
        """Returns the poll interval from Slurm config"""
        return self._poll_interval

    def check_finished_jobs(self, remote_jobs: list[RemoteJob]):
        """
        Check whether jobs have finished

        :param remote_jobs: list of remote jobs to check

        :returns: tuple of lists of RemoteJobs containing:
            - finished jobs
            - unfinished jobs

        """
        # get list of job ids from list of remote jobs
        job_ids = [rj.get_runner().get_jobid() for rj in remote_jobs]

        # remote function call with retries
        job_status_text = retry_call(
            self._check_slurm_jobs_wrapper,
            fargs=(job_ids,),
            tries=self._retry_tries,
            backoff=self._retry_backoff,
            delay=self._retry_delay,
        )

        # parse output for statuses
        count = 0
        finished_jobs = []
        unfinished_jobs = []
        for line in job_status_text.splitlines():
            if not len(line.strip()):
                continue

            count += 1  # checking whether there was anything to parse

            try:
                jobid, job_status = line.split("|")
            except ValueError as exc:
                msg = f"Error parsing job status line: '{line}' (line.split('|'))"
                logger.error(repr(exc))
                logger.error(msg)
                logger.error("Full job status output:" + os.linesep + job_status)
                raise RemoteJobRunnerError(msg)

            # pop this job out of the incoming list
            idx = job_ids.index(jobid)
            job_ids.pop(idx)
            rj = remote_jobs.pop(idx)

            if len(job_status) and job_status not in ("RUNNING", "PENDING"):
                # job has finished
                finished_jobs.append(rj)
                self._log(logging.DEBUG, f"Job {jobid} has finished: {job_status}")
            else:
                unfinished_jobs.append(rj)
                self._log(logging.DEBUG, f"Job {jobid} is unfinished: {job_status}")

        if count == 0:
            self._log(logging.WARNING, "No job statuses parsed, trying again later")
            unfinished_jobs = remote_jobs

        return finished_jobs, unfinished_jobs

    def _check_slurm_jobs_wrapper(self, unfinished_jobids):
        """
        Wrapper function that raises exception if returncode is nonzero

        Required because raising exceptions in funcx functions breaks things
        due to exception dependency on parsl

        """
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
