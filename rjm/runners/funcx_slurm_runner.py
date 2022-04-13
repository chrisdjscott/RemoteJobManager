
import os
import sys
import time
import logging

from funcx.sdk.client import FuncXClient
from funcx.sdk.executor import FuncXExecutor
from retry.api import retry_call

from rjm import utils
from rjm.runners.runner_base import RunnerBase
from rjm.errors import RemoteJobRunnerError


FUNCX_SCOPE = FuncXClient.FUNCX_SCOPE
FUNCX_TIMEOUT = 180  # default timeout for waiting for funcx functions

logger = logging.getLogger(__name__)


class FuncxSlurmRunner(RunnerBase):
    """
    Runner that uses FuncX to submit a Slurm job on the remote machine and
    poll until the job completes.

    The default FuncX endpoint running on the login node is sufficient.

    """
    def __init__(self, config=None):
        super(FuncxSlurmRunner, self).__init__(config=config)

        # the FuncX endpoint on the remote machine
        self._funcx_endpoint = self._config.get("FUNCX", "remote_endpoint")

        # funcx client and executor
        self._funcx_client = None
        self._funcx_executor = None

        # the name of the Slurm script
        self._slurm_script = self._config.get("SLURM", "slurm_script")

        # how often to poll for Slurm job completion
        self._poll_interval = self._config.getint("SLURM", "poll_interval")

        # Slurm job id
        self._jobid = None

    def _log(self, level, message, *args, **kwargs):
        """Add a label to log messages, identifying this specific RemoteJob"""
        logger.log(level, self._label + message, *args, **kwargs)

    def __repr__(self):
        return f"FuncxSlurmRunner({self._funcx_endpoint})"

    def get_jobid(self):
        """Return the job id"""
        return self._jobid

    def save_state(self):
        """Append state to state_dict if required for restarting"""
        state_dict = super(FuncxSlurmRunner, self).save_state()
        if self._jobid is not None:
            state_dict["slurm_job_id"] = self._jobid

        return state_dict

    def load_state(self, state_dict):
        """Get saved state if required for restarting"""
        super(FuncxSlurmRunner, self).load_state(state_dict)
        if "slurm_job_id" in state_dict:
            self._jobid = state_dict["slurm_job_id"]

    def get_globus_scopes(self):
        """If any Globus scopes are required, override this method and return them in a list"""
        self._required_scopes = [
            utils.OPENID_SCOPE,
            utils.SEARCH_SCOPE,
            FUNCX_SCOPE,
        ]

        return self._required_scopes

    def setup_globus_auth(self, globus_cli, runner=None):
        """Do any Globus auth setup here, if required"""
        if runner is None:
            # offprocess checker not working well with freezing currently
            if getattr(sys, "frozen", False):
                # application is frozen
                use_offprocess_checker = False
                self._log(logging.DEBUG, "Disabling offprocess_checker when frozen")
            else:
                use_offprocess_checker = True

            # setting up the FuncX client
            authorisers = globus_cli.get_authorizers_by_scope(requested_scopes=self._required_scopes)
            self._funcx_client = FuncXClient(
                fx_authorizer=authorisers[FUNCX_SCOPE],
                search_authorizer=authorisers[utils.SEARCH_SCOPE],
                openid_authorizer=authorisers[utils.OPENID_SCOPE],
                use_offprocess_checker=use_offprocess_checker,
            )

            # create a funcX executor
            self._funcx_executor = FuncXExecutor(self._funcx_client)
        else:
            # use client and executor from passed in runner
            self._log(logging.DEBUG, "Initialising runner from another")
            self._funcx_client = runner.get_funcx_client()
            self._funcx_executor = runner.get_funcx_executor()

    def get_funcx_client(self):
        """Returns the funcx client"""
        return self._funcx_client

    def get_funcx_executor(self):
        """Returns the funcx executor"""
        return self._funcx_executor

    def run_function(self, function, *args, **kwargs):
        """Run the given function and pass back the return value"""
        if self._funcx_executor is None:
            self._log(logging.ERROR, "Make sure you setup_globus_auth before trying to run something")
            raise RuntimeError("Make sure you setup_globus_auth before trying to run something")

        # start the function
        self._log(logging.DEBUG, f"Submitting function to FuncX executor: {function}")
        future = self._funcx_executor.submit(function, *args, endpoint_id=self._funcx_endpoint, **kwargs)

        # wait for it to complete and get the result
        self._log(logging.DEBUG, "Waiting for FuncX function to complete")
        result = future.result(timeout=FUNCX_TIMEOUT)

        return result

    def make_remote_directory(self, remote_base_path, prefix):
        """
        Make one or more remote directories, using the given prefix(es).

        :param remote_base_path: The base path on the remote machine to create
            the directories in
        :param prefix: Single prefix, or list of prefixes, for remote directories

        :return: Tuple, or list of tuples, containing the full remote path and
            remote path relative to the base path

        """
        # remote function expects a list
        if type(prefix) is not list:
            single = True
            prefix_list = [prefix]
        else:
            single = False
            prefix_list = prefix

        # run the remote function
        self._log(logging.DEBUG, f"Creating remote directories for: {prefix_list}")
        self._log(logging.DEBUG, f"Creating remote directories in: {remote_base_path}")
        remote_dirs = self.run_function_with_retries(_make_remote_directories, remote_base_path, prefix_list)

        # handle result
        if type(remote_dirs) is list:  # success
            if single:  # if single value passed in, return single value
                remote_dirs = remote_dirs[0]
        else:  # failed with error message passed back
            raise RemoteJobRunnerError(f"Make remote directory failed: {remote_dirs}")

        return remote_dirs

    def start(self, working_directory):
        """
        Starts running the Slurm script

        :param working_directory: the directory to submit the job in

        """
        self._log(logging.DEBUG, f"Submitting Slurm script: {self._slurm_script}")
        returncode, stdout = self.run_function(
            submit_slurm_job,
            self._slurm_script,
            submit_dir=working_directory,
        )
        self._log(logging.DEBUG, f'returncode = {returncode}; output = "{stdout}"')

        if returncode == 0:
            # success
            self._jobid = stdout.split()[-1]
            self._log(logging.INFO, f"Submitted Slurm job with id: {self._jobid}")
            started = True

        else:
            self._log(logging.ERROR, f'submitting job failed in remote directory: "{working_directory}"')
            self._log(logging.ERROR, f'return code: {returncode}')
            self._log(logging.ERROR, f'output: {stdout}')
            started = False
            raise RemoteJobRunnerError(f"{self._label}failed to submit Slurm job: {stdout}")

        return started

    def wait(self, polling_interval=None):
        """Wait for the Slurm job to finish"""
        if self._jobid is None:
            raise ValueError("Must call 'run_start' before 'run_wait'")

        # override polling interval from config file?
        if polling_interval is None:
            polling_interval = self._poll_interval

        # loop until job has finished
        self._log(logging.INFO, f"Waiting for Slurm job {self._jobid} to finish")
        self._log(logging.DEBUG, f"Polling interval is: {polling_interval} seconds")
        job_finished = False
        while not job_finished:
            returncode, job_status = self.run_function(check_slurm_job_status, self._jobid)
            if returncode == 0:
                self._log(logging.INFO, f"Current job status is: '{job_status}'")
                if len(job_status) and job_status not in ("RUNNING", "PENDING"):
                    job_finished = True
                else:
                    time.sleep(polling_interval)
            else:
                self._log(logging.ERROR, f'Checking job status failed for {self._jobid}')
                self._log(logging.ERROR, f'return code: {returncode}')
                self._log(logging.ERROR, f'output: {job_status}')
                raise RemoteJobRunnerError(f"{self._label}failed to get Slurm job status: {job_status}")

        if job_finished:
            self._log(logging.INFO, f"Slurm job {self._jobid} has finished")

        return job_finished

    def cancel(self):
        """Cancel the Slurm job"""
        if self._jobid is None:
            raise ValueError("Cannot cancel a run that hasn't started")

        self._log(logging.DEBUG, f"Cancelling Slurm job: {self._jobid}")
        returncode, stdout = self.run_function(cancel_slurm_job, self._jobid)
        self._log(logging.DEBUG, f'returncode = {returncode}; output = "{stdout}"')

        if returncode == 0:
            # success
            self._log(logging.INFO, f"Cancelled Slurm job with id: {self._jobid}")

        else:
            self._log(logging.WARNING, f'Cancelling job failed ({returncode}): "{stdout}"')

    def get_poll_interval(self):
        """Returns the poll interval from Slurm config"""
        return self._poll_interval

    def check_finished_jobs(self, remote_jobs):
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

    def get_checksums(self, working_directory, files):
        """
        Return checksums for the list of files

        :param files: list of files to calculate checksums of
        :param working_directory: directory to switch to first

        :returns: dictionary with file names as keys and checksums as values

        """
        # remote function call with retries
        checksums = retry_call(
            self._get_checksums_wrapper,
            fargs=(files, working_directory),
            tries=self._retry_tries,
            backoff=self._retry_backoff,
            delay=self._retry_delay,
        )
        self._log(logging.DEBUG, f"Calculated checksums for {len([c for c in checksums if c is not None])} of {len(files)} files")

        return checksums

    def _get_checksums_wrapper(self, files, working_directory):
        """
        Wrapper function that raises exception if returncode is nonzero.

        """
        returncode, checksums = self.run_function(_calculate_checksums, files, working_directory)

        if returncode != 0:
            msg = f"Calculating checksums failed ({returncode}): {checksums}"
            self._log(logging.ERROR, msg)
            raise RemoteJobRunnerError(msg)

        return checksums

    def _check_slurm_jobs_wrapper(self, unfinished_jobids):
        """
        Wrapper function that raises exception if returncode is nonzero

        Required because raising exceptions in funcx functions breaks things
        due to exception dependency on parsl

        """
        returncode, job_status_text = self.run_function(_check_slurm_job_statuses, unfinished_jobids)

        if returncode != 0:
            msg = f"Checking job statuses failed ({returncode}): {job_status_text}"
            self._log(logging.ERROR, msg)
            raise RemoteJobRunnerError(msg)

        return job_status_text


# function that calculates checksums for a list of files
def _calculate_checksums(files, working_directory):
    # catch all errors due to problem with exceptions being wrapped in parsl class
    # and parsl may not be installed on host (particularly windows)
    try:
        import os.path
        import hashlib

        file_chunk_size = 8192

        checksums = {}
        for fn in files:
            file_path = os.path.join(working_directory, fn)
            if os.path.isfile(file_path):
                with open(file_path, 'rb') as fh:
                    checksum = hashlib.sha256()
                    while chunk := fh.read(file_chunk_size):
                        checksum.update(chunk)
                checksums[fn] = checksum.hexdigest()
            else:
                checksums[fn] = None

        return 0, checksums

    except Exception as exc:
        return 1, repr(exc)


# function that submits a job to Slurm (assumes submit script and other required inputs were uploaded via Globus)
def submit_slurm_job(submit_script, submit_dir=None):
    # catch all errors due to problem with exceptions being wrapped in parsl class
    # and parsl may not be installed on host (particularly windows)
    try:
        import os
        import subprocess

        # if submit_dir is specified, it must exist
        if submit_dir is not None:
            if not os.path.exists(submit_dir):
                return 1, f"working directory does not exist: '{submit_dir}'"
            submit_script_path = os.path.join(submit_dir, submit_script)
        else:
            submit_script_path = submit_script

        # submit script must also exist
        if not os.path.exists(submit_script_path):
            return 1, f"submit_script does not exist: '{submit_script_path}'"

        # submit the Slurm job and return the job id
        p = subprocess.run(['sbatch', submit_script], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           universal_newlines=True, check=False, cwd=submit_dir)

        return p.returncode, p.stdout.strip()

    except Exception as exc:
        # return nonzero code and string representation of the exception
        return 1, repr(exc)


# function that checks Slurm job status
def check_slurm_job_status(jobid):
    """Check Slurm job status."""
    # have to load modules within the function
    import subprocess

    # query the status of the job using sacct
    p = subprocess.run(['sacct', '-j', jobid, '-X', '-o', 'State', '-n'], universal_newlines=True,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)

    return p.returncode, p.stdout.strip()


# function to cancel a Slurm job
def cancel_slurm_job(jobid):
    """Cancel the Slurm job"""
    # have to load modules within the function
    import subprocess

    # cancel the job using scancel
    p = subprocess.run(["scancel", jobid], universal_newlines=True, check=False,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    return p.returncode, p.stdout.strip()


# function that checks multiple Slurm job statuses at once
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


# function to make directories on the remote machine
def _make_remote_directories(base_path, prefixes):
    try:
        import os
        import tempfile

        remote_dirs = []
        for prefix in prefixes:
            remote_full_path = tempfile.mkdtemp(prefix=prefix + "-", dir=base_path)
            os.chmod(remote_full_path, 0o755)
            remote_dirs.append((remote_full_path, os.path.relpath(remote_full_path, start=base_path)))

    except Exception as exc:
        remote_dirs = repr(exc)

    return remote_dirs
