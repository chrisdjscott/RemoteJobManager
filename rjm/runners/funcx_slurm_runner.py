
import sys
import time
import logging

from funcx.sdk.client import FuncXClient
from funcx.sdk.executor import FuncXExecutor

from rjm.runners.runner_base import RunnerBase
from rjm import utils
from rjm.errors import RemoteJobRunnerError


FUNCX_TIMEOUT = 180

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

        # the name of the Slurm script
        self._slurm_script = self._config.get("SLURM", "slurm_script")

        # how often to poll for Slurm job completion
        self._poll_interval = self._config.getint("SLURM", "poll_interval")

        # funcx client and executor
        self._funcx_client = None
        self._funcx_executor = None

        # Slurm job id
        self._jobid = None

    def _log(self, level, message, *args, **kwargs):
        """Add a label to log messages, identifying this specific RemoteJob"""
        logger.log(level, self._label + message, *args, **kwargs)

    def __repr__(self):
        return f"FuncxSlurmRunner({self._funcx_endpoint})"

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
            utils.FUNCX_SCOPE,
        ]

        return self._required_scopes

    def setup_globus_auth(self, globus_cli):
        """Do any Globus auth setup here, if required"""
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
            fx_authorizer=authorisers[utils.FUNCX_SCOPE],
            search_authorizer=authorisers[utils.SEARCH_SCOPE],
            openid_authorizer=authorisers[utils.OPENID_SCOPE],
            use_offprocess_checker=use_offprocess_checker,
        )

        # create a funcX executor
        self._funcx_executor = FuncXExecutor(self._funcx_client)

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

    def start(self):
        """Starts running the Slurm script."""
        self._log(logging.DEBUG, f"Submitting Slurm script: {self._slurm_script}")
        returncode, stdout = self.run_function(submit_slurm_job, self._slurm_script, submit_dir=self._cwd)
        self._log(logging.DEBUG, f'returncode = {returncode}; output = "{stdout}"')

        if returncode == 0:
            # success
            self._jobid = stdout.split()[-1]
            self._log(logging.INFO, f"Submitted Slurm job with id: {self._jobid}")
            started = True

        else:
            self._log(logging.ERROR, f'submitting job failed in remote directory: "{self._cwd}"')
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


if __name__ == "__main__":
    # python -m rjm.funcx_slurm_runner
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("globus_sdk").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    runner = FuncxSlurmRunner()
    logger.info(f"Runner: {runner}")

    scopes = runner.get_globus_scopes()
    globus_cli = utils.handle_globus_auth(scopes)
    runner.setup_globus_auth(globus_cli)

    def get_hostname():
        import socket
        return socket.gethostname()
    hostname = runner.run_function(get_hostname)
    logger.info(f"Remote is running on: {hostname}")
