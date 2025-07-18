
import os
import time
import logging

from globus_compute_sdk import Client
from globus_compute_sdk import Executor
from globus_compute_sdk.sdk.login_manager.manager import LoginManager
from globus_compute_sdk.serialize import CombinedCode
from retry.api import retry_call

from rjm.runners.runner_base import RunnerBase
from rjm.errors import RemoteJobRunnerError


GLOBUS_COMPUTE_TIMEOUT = 120  # default timeout for waiting for functions
SLURM_UNFINISHED_STATUS = ['RUNNING', 'PENDING', 'NODE_FAIL', 'COMPLETING']
SLURM_WARN_STATUS = ["NODE_FAIL"]
SLURM_SUCCESSFUL_STATUS = ['COMPLETED']
MIN_POLLING_INTERVAL = 60
MIN_WARMUP_POLLING_INTERVAL = 10
MAX_WARMUP_DURATION = 300

logger = logging.getLogger(__name__)


class GlobusComputeSlurmRunner(RunnerBase):
    """
    Runner that uses Globus Compute to submit a Slurm job on the remote machine and
    poll until the job completes.

    The default Globus Compute endpoint running on the login node is sufficient.

    """
    def __init__(self, config=None):
        super(GlobusComputeSlurmRunner, self).__init__(config=config)

        self._setup_done = False

        # the Globus Compute endpoint on the remote machine
        self._endpoint = self._config.get("FUNCX", "remote_endpoint")

        # globus compute login manager
        self._login_manager = CustomLoginManager()

        # globus compute client and executor
        self._external_runner = None
        self._client = None
        self._executor = None

        # the name of the Slurm script
        self._slurm_script = self._config.get("SLURM", "slurm_script")

        # how often to poll for Slurm job completion
        self._poll_interval = self._config.getint("SLURM", "poll_interval")
        self._warmup_poll_interval = self._config.getint("SLURM", "warmup_poll_interval")
        self._warmup_duration = self._config.getint("SLURM", "warmup_duration")

        # Slurm job id
        self._jobid = None

    def _log(self, level, message, *args, **kwargs):
        """Add a label to log messages, identifying this specific RemoteJob"""
        logger.log(level, self._label + message, *args, **kwargs)

    def __repr__(self):
        return f"GlobusComputeSlurmRunner({self._endpoint})"

    def get_jobid(self):
        """Return the job id"""
        return self._jobid

    def save_state(self):
        """Append state to state_dict if required for restarting"""
        state_dict = super(GlobusComputeSlurmRunner, self).save_state()
        if self._jobid is not None:
            state_dict["slurm_job_id"] = self._jobid

        return state_dict

    def load_state(self, state_dict):
        """Get saved state if required for restarting"""
        super(GlobusComputeSlurmRunner, self).load_state(state_dict)
        if "slurm_job_id" in state_dict:
            self._jobid = state_dict["slurm_job_id"]

    def get_globus_scopes(self):
        """If any Globus scopes are required, override this method and return them in a list"""
        return self._login_manager.get_scopes()

    def setup_globus_auth(self, globus_cli, runner=None):
        """Do any Globus auth setup here, if required"""
        self._setup_done = True

        # prepare to create the globus compute client
        if runner is None:
            # setting up the login manager
            self._login_manager.set_cli(globus_cli)

        else:
            # store reference to passed in runner
            self._log(logging.DEBUG, "Initialising runner from another")
            self._external_runner = runner

        # now create the globus compute client
        self.reset_globus_compute_client()

    def _create_globus_compute_client(self):
        """Return new globus compute client instance"""
        if not self._setup_done:
            raise RuntimeError("setup_globus_auth must be called before create_globus_compute_client")

        self._log(logging.DEBUG, "Creating Globus Compute client")

        # setting up the Globus Compute client
        client = Client(
            login_manager=self._login_manager,
            code_serialization_strategy=CombinedCode(),
        )

        return client

    def _create_globus_compute_executor(self):
        """Return new globus compute executor instance"""
        if self._client is None:
            raise RuntimeError("create_globus_compute_executor requires _client to be set first")
        if self._endpoint is None:
            raise RuntimeError("create_globus_compute_executor requires _endpoint to be set first")

        self._log(logging.DEBUG, f"Creating Globus Compute executor for endpoint: {self._endpoint}")

        # create a Globus Compute executor
        executor = Executor(client=self._client, endpoint_id=self._endpoint)

        return executor

    def reset_globus_compute_client(self, propagate=False):
        """
        Force the runner to create a new Globus Compute client.

        If propagate is True and an external runner is being used, then call
        reset on the external runner too.

        """
        if not self._setup_done:
            raise RuntimeError("setup_globus_auth must be called before reset_globus_compute_client")

        if self._external_runner is None:
            if self._executor is not None:
                self._log(logging.DEBUG, f"Shutting down old Globus Compute executor ({self._executor})")
                self._executor.shutdown()

            # create a client
            self._client = self._create_globus_compute_client()
            self._log(logging.DEBUG, f"Using new Globus Compute client: {self._client}")

            # create a executor
            self._executor = self._create_globus_compute_executor()
            self._log(logging.DEBUG, f"Using new Globus Compute executor: {self._executor}")

        else:
            # if required, force the external runner to reset too
            if propagate:
                self._log(logging.DEBUG, "Resetting Globus Compute client on passed in runner")
                self._external_runner.reset_globus_compute_client(propagate=True)

            # update references
            self._client = self._external_runner.get_client()
            self._executor = self._external_runner.get_executor()

    def get_client(self):
        """Returns the globus compute client"""
        if self._external_runner is not None:
            client = self._external_runner.get_client()
        else:
            client = self._client

        return client

    def get_executor(self):
        """Returns the globus compute executor"""
        if self._external_runner is not None:
            executor = self._external_runner.get_executor()
        else:
            executor = self._executor

        return executor

    def run_function(self, function, *args, **kwargs):
        """Run the given function and pass back the return value"""
        if self._external_runner is not None:
            # update reference to executor
            self._executor = self._external_runner.get_executor()

        if self._executor is None:
            self._log(logging.ERROR, "Make sure you setup_globus_auth before trying to run something")
            raise RuntimeError("Make sure you setup_globus_auth before trying to run something")

        # see if we can detect in advance that the executor has been shutdown
        # somehow and start a new one before running the function
        # note: this is likely to break with upstream changes
        if hasattr(self._executor, "_stopped"):
            if self._executor._stopped:
                self._log(logging.WARNING, "Globus Compute Executor detected as stopped - attempting to start a new one before running the function")
                self.reset_globus_compute_client(propagate=True)

        # start the function
        self._log(logging.DEBUG, f"Submitting function to Globus Compute executor ({self._executor}): {function}")
        try:
            future = self._executor.submit(function, *args, **kwargs)
        except RuntimeError as exc:
            # we are trying to catch the case where the executor has been shutdown somehow
            # in which we case we want to start a new executor and then reraise the error
            # so that retries can retry the function, if it is being used...
            self._log(logging.WARNING, f"Failed to submit function to executor: {str(exc)}")
            if "is shutdown" in str(exc):
                self._log(logging.WARNING, "Function submission failed due to executor being shutdown - attempting to start a new executor")
                self.reset_globus_compute_client(propagate=True)

            # always reraise at this point (run_function may be wrapped in a retry, let that
            # handle the retry in case we get into a loop of failures for some reason)
            raise exc

        # wait for it to complete and get the result
        self._log(logging.DEBUG, "Waiting for Globus Compute function to complete")
        result = future.result(timeout=GLOBUS_COMPUTE_TIMEOUT)

        return result

    def make_remote_directory(self, remote_base_path, prefix, retries=True):
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

        # with or without retries
        if retries:
            run_func = self.run_function_with_retries
        else:
            run_func = self.run_function

        # run the remote function
        self._log(logging.DEBUG, f"Creating remote directories for: {prefix_list}")
        self._log(logging.DEBUG, f"Creating remote directories in: {remote_base_path}")
        remote_dirs = run_func(_make_remote_directories, remote_base_path, prefix_list)

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

    def wait(self, polling_interval=None, warmup_polling_interval=None, warmup_duration=None):
        """
        Wait for the Slurm job to finish

        Return True if the job succeeded, False if it failed

        """
        if self._jobid is None:
            raise ValueError("Must call 'run_start' before 'run_wait'")

        # get the polling interval
        polling_interval, warmup_polling_interval, warmup_duration = self.get_poll_interval(
            polling_interval, warmup_polling_interval, warmup_duration
        )

        # loop until job has finished
        self._log(logging.INFO, f"Waiting for Slurm job {self._jobid} to finish")
        self._log(logging.DEBUG, f"Polling interval is: {polling_interval} seconds")
        job_finished = False
        job_succeeded = None
        while not job_finished:
            job_status_dict, job_status_msg = self.run_function(_check_slurm_job_statuses, [self._jobid])
            self._log(logging.DEBUG, "Output from check Slurm job status function follows:")
            self._log(logging.DEBUG, os.linesep.join(job_status_msg))

            if job_status_dict is not None:
                if self._jobid in job_status_dict:
                    job_status = job_status_dict[self._jobid]
                else:
                    job_status = None

                self._log(logging.INFO, f"Current job status is: '{job_status}'")
                if job_status in SLURM_WARN_STATUS:  # job should (?) be requeued so keep checking
                    self._log(logging.WARNING, f'Job status "{job_status}" may require manual intervention, continuing to check')

                if job_status is not None and job_status not in SLURM_UNFINISHED_STATUS:
                    job_finished = True
                    if job_status in SLURM_SUCCESSFUL_STATUS:
                        job_succeeded = True
                    else:
                        job_succeeded = False
                else:
                    time.sleep(polling_interval)

            else:
                self._log(logging.ERROR, f'Checking job status failed for {self._jobid}')
                self._log(logging.ERROR, f'output: {job_status_msg}')
                raise RemoteJobRunnerError(f"{self._label}failed to get Slurm job status: {job_status_msg}")

        assert job_succeeded is not None, "Unexpected error during wait"
        if job_finished:
            self._log(logging.INFO, f"Slurm job {self._jobid} has finished")

        return job_succeeded

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

    def get_poll_interval(
        self,
        requested_interval: int | None,
        requested_warmup_interval: int | None,
        requested_warmup_duration: int | None,
    ):
        """Returns the poll interval from Slurm config"""
        if requested_interval is None:
            polling_interval = self._poll_interval
            self._log(logging.DEBUG, f"Using polling interval from config file: {polling_interval}")
        else:
            polling_interval = requested_interval
            self._log(logging.DEBUG, f"Using requested polling interval: {polling_interval}")

        if polling_interval < MIN_POLLING_INTERVAL:
            polling_interval = MIN_POLLING_INTERVAL
            self._log(logging.WARNING, f"Overriding polling interval with minimum value: {polling_interval}")

        if requested_warmup_interval is None:
            warmup_polling_interval = self._warmup_poll_interval
            self._log(logging.DEBUG, f"Using warmup polling interval from config file: {warmup_polling_interval}")
        else:
            warmup_polling_interval = requested_warmup_interval
            self._log(logging.DEBUG, f"Using requested warmup polling interval: {warmup_polling_interval}")

        if warmup_polling_interval < MIN_WARMUP_POLLING_INTERVAL:
            warmup_polling_interval = MIN_WARMUP_POLLING_INTERVAL
            self._log(logging.WARNING, f"Overriding warmup polling interval with minimum value: {warmup_polling_interval}")


        if requested_warmup_duration is None:
            warmup_duration = self._warmup_duration
            self._log(logging.DEBUG, f"Using warmup duration from config file: {warmup_duration}")
        else:
            warmup_duration = requested_warmup_duration
            self._log(logging.DEBUG, f"Using requested warmup duration: {warmup_duration}")

        if warmup_duration > MAX_WARMUP_DURATION:
            warmup_duration = MAX_WARMUP_DURATION
            self._log(logging.WARNING, f"Overriding warmup duration with maximum value: {warmup_duration}")

        return polling_interval, warmup_polling_interval, warmup_duration

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
        job_status_dict = retry_call(
            self._check_slurm_jobs_wrapper,
            fargs=(job_ids,),
            tries=self._retry_tries,
            backoff=self._retry_backoff,
            delay=self._retry_delay,
            max_delay=self._retry_max_delay,
        )

        # loop over job statuses
        successful_jobs = []
        failed_jobs = []
        unfinished_jobs = []
        for jobid in job_status_dict:
            job_status = job_status_dict[jobid]

            # pop this job out of the incoming list
            idx = job_ids.index(jobid)
            job_ids.pop(idx)
            rj = remote_jobs.pop(idx)

            if len(job_status) and job_status not in SLURM_UNFINISHED_STATUS:
                # job has finished, was it successful
                if job_status in SLURM_SUCCESSFUL_STATUS:
                    successful_jobs.append(rj)
                    self._log(logging.DEBUG, f"Job {jobid} has finished successfully: {job_status}")
                else:
                    failed_jobs.append(rj)
                    self._log(logging.DEBUG, f"Job {jobid} has finished unsuccessfully: {job_status}")
            else:
                unfinished_jobs.append(rj)
                self._log(logging.DEBUG, f"Job {jobid} is unfinished: {job_status}")

        if len(job_status_dict) == 0:
            self._log(logging.WARNING, "No job statuses parsed, trying again later")
            unfinished_jobs = remote_jobs

        return successful_jobs, failed_jobs, unfinished_jobs

    def get_checksums(self, working_directory, files):
        """
        Return checksums for the list of files

        :param files: list of files to calculate checksums of
        :param working_directory: directory to switch to first

        :returns: dictionary with file names as keys and checksums as values

        """
        # remote function call with retries
        self._log(logging.DEBUG, f"Calculating checksums for {len(files)} files")
        checksums = retry_call(
            self._get_checksums_wrapper,
            fargs=(files, working_directory),
            tries=self._retry_tries,
            backoff=self._retry_backoff,
            delay=self._retry_delay,
            max_delay=self._retry_max_delay,
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

        Required because raising exceptions in globus compute functions breaks things
        due to exception dependency on parsl (may not still be the case)

        """
        job_status_dict, msg = self.run_function(_check_slurm_job_statuses, unfinished_jobids)

        self._log(logging.DEBUG, "Output from check Slurm job status function follows:")
        self._log(logging.DEBUG, os.linesep.join(msg))

        if job_status_dict is None:
            msg = f"Checking job statuses failed: {msg}"
            self._log(logging.ERROR, os.linesep.join(msg))
            raise RemoteJobRunnerError(msg)

        return job_status_dict


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
        p = subprocess.run(f'module purge > /dev/null 2>&1 && sbatch "{submit_script}"', shell=True, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, universal_newlines=True, check=False, cwd=submit_dir)

        return p.returncode, p.stdout.strip()

    except Exception as exc:
        # return nonzero code and string representation of the exception
        return 1, repr(exc)


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
    import subprocess

    def run_cmd(cmd):
        p = subprocess.run(cmd, universal_newlines=True, stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, check=False)
        return p.returncode, p.stdout.strip()

    def parse_output(store, output, delim=None):
        for line in output.splitlines():
            if len(line.strip()):
                try:
                    jobid, jobstate = line.split(delim)
                except ValueError:
                    pass
                else:
                    store[jobid] = jobstate

    status_dict = {}
    msg = []

    # query job statuses using squeue first
    cmd_args = ['squeue', '--state', 'all', '-O', 'JobID,State', '--noheader', '--jobs', ','.join(jobids)]
    sq_status, sq_output = run_cmd(cmd_args)
    if sq_status == 0:
        # successful, parse list
        parse_output(status_dict, sq_output)
        msg.append(f"Retrieved status after squeue: {status_dict}")
    else:
        msg.append(f"squeue failed with status {sq_status}")
        msg.append(sq_output)

    # use sacct for job ids not returned by squeue
    remaining_job_ids = [j for j in jobids if j not in status_dict]
    if len(remaining_job_ids):
        # query the status of the job using sacct
        cmd_args = ['sacct', '-X', '-o', 'JobID,State', '-n', '-P']
        for jobid in remaining_job_ids:
            cmd_args.extend(['-j', jobid])
        sacct_status, sacct_output = run_cmd(cmd_args)
        if sacct_status == 0:
            parse_output(status_dict, sacct_output, delim="|")
            msg.append(f"Retrieved status after sacct: {status_dict}")
        else:
            msg.append(f"sacct failed with status {sacct_status}")
            msg.append(sacct_output)
    else:
        sacct_status = 0

    if len(status_dict) == 0 and (sq_status or sacct_status):
        status_dict = None

    return status_dict, msg


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


class CustomLoginManager(LoginManager):
    """
    Custom login manager that uses RJM token storage

    """
    def __init__(self):
        self._cli = None
        self._logger = logging.getLogger(__name__ + ".CustomLoginManager")

    def set_cli(self, cli):
        """Set the native auth client"""
        self._cli = cli

    def get_scopes(self):
        """Return list of required scopes"""
        scopes = [
            s for _rs_name, rs_scopes in self.login_requirements for s in rs_scopes
        ]

        return scopes

    def ensure_logged_in(self):
        self._logger.warning("ensure_logged_in has not been implemented")

    def logout(self):
        self._logger.warning("logout has not been implemented")

    def _get_authorizer(self, resource_server):
        if self._cli is None:
            raise RuntimeError('Must call "set_cli" on "CustomLoginManager"')

        # get the authorisers
        authorisers = self._cli.get_authorizers(self.get_scopes())

        # check the selected authoriser exists
        if resource_server not in authorisers:
            raise RuntimeError(f'resource server "{resource_server}" is not authorised - try "rjm_authenticate"')

        # return the selected authoriser
        return authorisers[resource_server]
