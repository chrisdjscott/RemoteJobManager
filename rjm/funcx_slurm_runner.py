
import time
import logging
from subprocess import CalledProcessError

from funcx.sdk.client import FuncXClient
from funcx.sdk.executor import FuncXExecutor

from .runner_base import RunnerBase
from . import utils


POLL_INTERVAL = 5  # how often to check Slurm job status, seconds  # TODO: move to config file?

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

    def __repr__(self):
        return f"FuncxSlurmRunner({self._funcx_endpoint})"

    def get_globus_scopes(self):
        """If any Globus scopes are required, override this method and return them in a list"""
        self._required_scopes = [
            utils.OPENID_SCOPE,
            utils.SEARCH_SCOPE,
            utils.FUNCX_SCOPE,
        ]
        logger.debug(f"Required Globus scopes are: {self._required_scopes}")

        return self._required_scopes

    def setup_globus_auth(self, globus_cli):
        """Do any Globus auth setup here, if required"""
        # setting up the FuncX client
        authorisers = globus_cli.get_authorizers_by_scope(requested_scopes=self._required_scopes)
        self._funcx_client = FuncXClient(
            fx_authorizer=authorisers[utils.FUNCX_SCOPE],
            search_authorizer=authorisers[utils.SEARCH_SCOPE],
            openid_authorizer=authorisers[utils.OPENID_SCOPE],
        )

        # create a funcX executor
        self._funcx_executor = FuncXExecutor(self._funcx_client)

    def run_function(self, function, *args, **kwargs):
        """Run the given function and pass back the return value"""
        if self._funcx_executor is None:
            logger.error("Make sure you setup_globus_auth before trying to run something")
            raise RuntimeError("Make sure you setup_globus_auth before trying to run something")

        # start the function
        logger.debug(f"Submitting function to FuncX executor: {function}")
        future = self._funcx_executor.submit(function, *args, endpoint_id=self._funcx_endpoint, **kwargs)

        # wait for it to complete and get the result
        logger.debug("Waiting for FuncX function to complete")
        result = future.result()

        return result

    def start_script(self, slurm_script_name):
        """Starts running the given script and returns an id to identify the script"""
        logger.debug(f"Submitting Slurm script: {slurm_script_name}")
        try:
            jobid = self.run_function(submit_slurm_job, slurm_script_name, work_dir=self._cwd)
        except CalledProcessError as exc:
            logger.error(f'submitting job failed in remote directory "{self._cwd}":')
            logger.error(f"    return code: {exc.returncode}")
            logger.error(f"    cmd: {exc.cmd}")
            logger.error(f"    output: {exc.stdout}")
            raise exc
        else:
            logger.debug(f"Slurm job submitted: {jobid}")

        return jobid

    def wait_for_script(self, slurm_job_id):
        """Wait for the script to stop running"""
        # loop until job has finished
        logger.debug(f"Waiting for Slurm job {slurm_job_id} to finish")
        job_finished = False
        while not job_finished:
            job_status = self.run_function(check_slurm_job_status, slurm_job_id)
            logger.debug(f"Current job status is: {job_status}")
            if job_status not in ("RUNNING", "PENDING"):
                job_finished = True
            else:
                time.sleep(POLL_INTERVAL)
        logger.debug(f"Slurm job {slurm_job_id} has finished")


# function that submits a job to Slurm (assumes submit script and other required inputs were uploaded via Globus)
def submit_slurm_job(submit_script, work_dir=None):
    import os
    import subprocess

    # change to working directory
    if work_dir is not None:
        if os.path.isdir(work_dir):
            with open("rjm_start_script.txt", "w") as fout:
                fout.write(f"INFO: Changing to directory: {work_dir}\n")
            os.chdir(work_dir)
        else:
            with open("rjm_start_script.txt", "w") as fout:
                fout.write(f"ERROR: could not change to directory: {work_dir}\n")

    # submit the Slurm job and return the job id
    with open("rjm_start_script.txt", "w") as fout:
        submit_cmd = f'sbatch {submit_script}'
        fout.write(f"{submit_cmd}\n")
        result = subprocess.run(submit_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                shell=True, universal_newlines=True, check=True)
        output = result.stdout
        fout.write(output.strip() + "\n")

    # the job id
    jobid = output.split()[-1]

    return jobid


# function that checks Slurm job status
def check_slurm_job_status(jobid):
    """Check Slurm job status."""
    # have to load modules within the function
    import subprocess

    # query the status of the job using sacct
    cmd = f'sacct -j {jobid} -X -o State -n'
    output = subprocess.check_output(cmd, shell=True, universal_newlines=True)

    return output.strip()


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
