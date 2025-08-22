
import uuid
import os
import time
import logging
import paramiko

from retry.api import retry_call

from rjm.runners.runner_base import RunnerBase
from rjm.errors import RemoteJobRunnerError


MIN_POLLING_INTERVAL = 60
MIN_WARMUP_POLLING_INTERVAL = 10
MAX_WARMUP_DURATION = 300

logger = logging.getLogger(__name__)


class ParamikoSSHRunner(RunnerBase):
    """
    Runner that uses the Paramiko SSH client to execute a command in a tmux
    session on the remote machine and poll until the command completes.

    It is up to the calling program to manage the amount of work submitted
    concurrently.

    """
    def __init__(self, config=None):
        super(ParamikoSSHRunner, self).__init__(config=config)

        self._setup_done = False
        self._ssh_client = None

        # config
        self._ssh_private_key_file = self._config.get("PARAMIKO", "private_key_file")
        self._remote_address = self._config.get("PARAMIKO", "remote_address")
        self._remote_user = self._config.get("PARAMIKO", "remote_user")
        self._job_script = self._config.get("PARAMIKO", "job_script")

        # how often to poll for job completion
        self._poll_interval = self._config.getint("POLLING", "poll_interval")
        self._warmup_poll_interval = self._config.getint("POLLING", "warmup_poll_interval")
        self._warmup_duration = self._config.getint("POLLING", "warmup_duration")

        # tmux session name
        self._tmux_session_name = None
        self._working_directory = None

    def _log(self, level, message, *args, **kwargs):
        """Add a label to log messages, identifying this specific RemoteJob"""
        logger.log(level, self._label + message, *args, **kwargs)

    def __repr__(self):
        return f"ParamikoSSHRunner({self._remote_user}@{self._remote_address})"

    def save_state(self):
        """Append state to state_dict if required for restarting"""
        state_dict = super(ParamikoSSHRunner, self).save_state()
        if self._tmux_session_name is not None:
            state_dict["tmux_session_name"] = self._tmux_session_name
        if self._working_directory is not None:
            state_dict["working_directory"] = self._working_directory

        return state_dict

    def __del__(self):
        if self._ssh_client is not None:
            self._ssh_client.close()

    def load_state(self, state_dict):
        """Get saved state if required for restarting"""
        super(ParamikoSSHRunner, self).load_state(state_dict)
        if "tmux_session_name" in state_dict:
            self._tmux_session_name = state_dict["tmux_session_name"]
        if "working_directory" in state_dict:
            self._working_directory = state_dict["working_directory"]

    def setup(self, *args, **kwargs):
        """Setup the SFTP client"""
        self._log(logging.DEBUG, "Setting up ParamikoSSHRunner...")
        self._ssh_client = paramiko.SSHClient()
        self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self._log(logging.DEBUG, f"Loading SSH key from {self._ssh_private_key_file}")
        # change this to load an RSA key, AI!
        self._private_key = paramiko.Ed25519Key.from_private_key_file(self._ssh_private_key_file)

        # Connect to server
        self._ssh_client.connect(
            hostname=self._remote_address,
            port=22,
            username=self._remote_user,
            pkey=self._private_key,
            timeout=30,
        )
        self._log(logging.DEBUG, f"Connected to: {self._remote_user}@{self._remote_address} ({self._ssh_client})")

        self._setup_done = True

    def run_command(self, command, background=False, retries=False):
        """
        Run the given command on the remote machine.

        Returns the output of the command if `background` is `False`;
        returns the tmux session name if `background` is `True`.

        :param command: The command to run on the remote machine
        :type command: str
        :param background: Whether to run the command in the background using tmux
        :type background: bool
        :param retries: Whether to retry the command if it fails
        :type retries: bool

        """
        if self._ssh_client is None:
            raise RuntimeError("Must call setup before run_command")

        self._log(logging.DEBUG, f"Running {'background ' if background else ''}command: {command}")

        if background:
            session_name = f"rjm-{uuid.uuid4()}"

            # Escape single quotes in command for safe shell use
            escaped_command = command.replace("'", r"'\''")

            # Full tmux command to create a new detached session and run the command
            command = (
                f"tmux new-session -d -s '{session_name}' '{escaped_command}'"
            )
            self._log(logging.DEBUG, f"Full background command: {command}")

        stdin, stdout, stderr = self._ssh_client.exec_command(command)

        stdout_output = stdout.read().decode().strip()
        stderr_output = stderr.read().decode().strip()
        full_output_not_time_ordered = stdout_output + stderr_output

        exit_code = stdout.channel.recv_exit_status()

        if exit_code:
            raise RemoteJobRunnerError(f"run_command failed (exit code {exit_code}): STDOUT: {stdout_output}; STDERR: {stderr_output}")

        if background:
            retval = session_name
        else:
            retval = full_output_not_time_ordered

        return retval

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
            self._log(logging.WARNING, "Retries not implemented yet (make_remote_directory)")

        # run the remote function
        self._log(logging.DEBUG, f"Creating remote directories for: {prefix_list}")
        self._log(logging.DEBUG, f"Creating remote directories in: {remote_base_path}")

        try:
            remote_dirs = []
            for p in prefix_list:
                # Construct the mktemp command to create a directory in base_path with the given prefix
                cmd = f"mktemp -d -p '{remote_base_path}' -t '{p}-XXXXXX'"

                stdin, stdout, stderr = self._ssh_client.exec_command(cmd)

                # Wait for command to complete
                exit_status = stdout.channel.recv_exit_status()

                if exit_status != 0:
                    error_msg = stderr.read().decode().strip()
                    raise RuntimeError(f"Failed to create temp directory with prefix '{p}': {error_msg}")

                # Read the stdout (the created directory path)
                remote_full_path = stdout.read().decode().strip()

                # Ensure base_path is normalized for relative path calculation
                base_path_clean = remote_base_path.rstrip('/')
                if not remote_full_path.startswith(base_path_clean):
                    raise RuntimeError(f"Created directory {remote_full_path} is not under base path {remote_base_path}")

                # Compute relative path
                rel_path = os.path.relpath(remote_full_path, start=remote_base_path)

                remote_dirs.append((remote_full_path, rel_path))

        except Exception as exc:
            raise RemoteJobRunnerError(f"Make remote directory failed: {exc}")

        if single:
            remote_dirs = remote_dirs[0]

        return remote_dirs

    def check_directory_exists(self, directory_path):
        """
        Check that the given directory exists on the remote machine

        :param directory_path
        :raises RemoteJobRunnerError: if the directory does not exist

        """
        # Use SSH to test if the directory exists
        command = f"test -d '{directory_path}'"
        stdin, stdout, stderr = self._ssh_client.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            raise RemoteJobRunnerError(f"Remote directory does not exist: {directory_path}")
        # Directory exists; nothing to return

    def start(self, working_directory):
        """
        Starts running the job script

        :param working_directory: the directory to run the script in

        """
        self._log(logging.DEBUG, f"Starting job for: {working_directory}")
        try:
            self._tmux_session_name = self.run_command(
                f'cd "{working_directory}" && bash {self._job_script} > stdout.txt 2> stderr.txt && touch "{working_directory}/.rjm-succeeded"',
                background=False
            )
        
        except RemoteJobRunnerError as exc:
            self._log(logging.ERROR, f'Starting job failed in remote directory: "{working_directory}"')
            self._log(logging.ERROR, f'{exc}')
            started = False
            raise exc

        self._log(logging.INFO, f'Started job with tmux session name: {self._tmux_session_name}')
        self._working_directory = working_directory
        started = True

        return started

    def wait(self, polling_interval=None, warmup_polling_interval=None, warmup_duration=None):
        """
        Wait for the job to finish

        Return True if the job succeeded, False if it failed

        """
        if self._tmux_session_name is None:
            raise ValueError("Must call 'run_start' before 'run_wait'")

        # get the polling interval
        polling_interval, warmup_polling_interval, warmup_duration = self.get_poll_interval(
            polling_interval, warmup_polling_interval, warmup_duration
        )

        # loop until job has finished
        self._log(logging.INFO, f"Waiting for job {self._jobid} to finish")
        self._log(logging.DEBUG, f"Polling interval is: {polling_interval} seconds")
        job_finished = False
        job_succeeded = None
        while not job_finished:
            cmd = f"tmux has-session {self._tmux_session_name}"
            stdin, stdout, stderr = self._ssh_client.exec_command(cmd)

            # Wait for command to complete
            exit_status = stdout.channel.recv_exit_status()
            self._log(logging.DEBUG, f'tmux has-session exit code: {exit_status} ({stdout}) ({stderr})')

            if exit_status:
                # TODO: should also check stderr as expected... eg should contain "can't find session"
                job_finished = True

                # TODO: need to confirm whether or not it succeeded, i.e. does the file exist?
                stdin, stdout, stderr = self._ssh_client.exec_command(f"test -f {self._working_directory}/.rjm-succeeded")
                exit_status = stdout.channel.recv_exit_status()
                if exit_status:
                    job_succeeded = False
                else:
                    job_succeeded = True

            else:
                self._log(logging.DEBUG, "Not finished yet")
                time.sleep(polling_interval)

        assert job_succeeded is not None, "Unexpected error during wait"
        if job_finished:
            self._log(logging.INFO, f"Remote job {self._tmux_session_name} has finished (success: {job_succeeded})")

        return job_succeeded

    def cancel(self):
        """Cancel the remote job"""
        raise NotImplementedError()

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
            - successful jobs
            - failed jobs
            - unfinished jobs

        """
        raise NotImplementedError

    def get_checksums(self, working_directory, files):
        """
        Return SHA256 checksums for the list of files

        :param files: list of files to calculate checksums of
        :param working_directory: directory to switch to first

        :returns: dictionary with file names as keys and checksums as values

        """
        raise NotImplementedError
