
import os
import logging
from datetime import datetime
import time
import json

from retry.api import retry_call

from rjm import utils
from rjm import config as config_helper
from rjm.transferers import globus_https_transferer
from rjm.runners.funcx_slurm_runner import FuncxSlurmRunner
from rjm.errors import RemoteJobRunnerError


logger = logging.getLogger(__name__)


class RemoteJob:
    """
    A remote job is based on a local directory.

    - input files are uploaded from the local directory
    - commands are executed on the remote system
    - output files are downloaded and stored in the local directory

    """
    STATE_FILE = "remote_job.json"

    def __init__(self, timestamp=None):
        self._local_path = None
        self._remote_full_path = None
        self._remote_basename = None
        self._label = ""
        self._uploaded = False
        self._downloaded = False
        self._run_started = False
        self._run_succeeded = False
        self._run_failed = False
        self._cancelled = False
        self._state_file = None

        # timestamp for working directory name
        self._timestamp = timestamp
        if self._timestamp is None:
            self._timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

        # load the config
        config = config_helper.load_config()
        self._uploads_file = config.get("FILES", "uploads_file")
        self._downloads_file = config.get("FILES", "downloads_file")
        self._retry_tries, self._retry_backoff, self._retry_delay = utils.get_retry_values_from_config(config)

        # file transferer
        self._transfer = globus_https_transferer.GlobusHttpsTransferer(config=config)

        # remote runner
        self._runner = FuncxSlurmRunner(config=config)

    def files_uploaded(self):
        """Return whether files have been uploaded"""
        return self._uploaded

    def files_downloaded(self):
        """Return whether files have been downloaded"""
        return self._downloaded

    def run_started(self):
        """Return whether the run has been started"""
        return self._run_started

    def run_completed(self):
        """Return whether the run has completed (regardless of success)"""
        return self._run_succeeded or self._run_failed

    def set_run_completed(self, success=True):
        """Marks the run as having been completed"""
        if success:
            self._run_succeeded = True
            self._run_failed = False
        else:
            self._run_failed = True
            self._run_succeeded = False

    def _log(self, level, message, *args, **kwargs):
        """Add a label to log messages, identifying this specific RemoteJob"""
        logger.log(level, self._label + message, *args, **kwargs)

    def get_local_dir(self):
        """Return the local directory of this RemoteJob"""
        return self._local_path

    def _read_uploads_file(self):
        """Read the file that lists files to be uploaded"""
        upload_file_path = os.path.join(self._local_path, self._uploads_file)
        self._upload_files = []

        if os.path.isfile(upload_file_path):
            with open(upload_file_path) as fh:
                upload_files = [fn.strip() for fn in fh.readlines() if len(fn.strip())]
            for fname in upload_files:
                # could be relative or absolute path
                if os.path.isabs(fname):
                    fpath = fname
                else:
                    fpath = os.path.join(self._local_path, fname)

                if os.path.exists(fpath):
                    if os.path.isfile(fpath):
                        self._upload_files.append(fpath)
                    else:
                        self._log(logging.WARNING, f'Skipping upload file specified in "{self._uploads_file}" that is not a file: "{fpath}"')
                else:
                    self._log(logging.WARNING, f'Skipping upload file specified in "{self._uploads_file}" that does not exist: "{fpath}"')
            self._log(logging.DEBUG, f"Files to be uploaded: {self._upload_files}")
        else:
            self._log(logging.WARNING, f"Uploads file does not exist: {upload_file_path}")

    def _read_downloads_file(self):
        """Read file that lists files to be downloaded"""
        download_file_path = os.path.join(self._local_path, self._downloads_file)

        if os.path.isfile(download_file_path):
            with open(download_file_path) as fh:
                self._download_files = [f.strip() for f in fh.readlines() if len(f.strip())]
            for fn in self._download_files:
                if os.path.exists(os.path.join(self._local_path, fn)):
                    self._log(logging.WARNING, f"Local file will be overwritten by download: {os.path.join(self._local_path, fn)}")
            self._log(logging.DEBUG, f"Files to be downloaded: {self._download_files}")
        else:
            self._download_files = []
            self._log(logging.WARNING, f"Downloads file does not exist: {download_file_path}")

    def setup(self, local_dir, force=False, runner=None, transfer=None):
        """
        Set up the remote job (authentication, remote directory...)

        :param local_dir: the local directory of the remote job
        :param force: ignore saved progress and start again
        :param runner: runner instance to base this job's runner off
        :param transfer: transferer instance to base this job's transferer off

        """
        # the local directory this job is based on
        self._label = f"[{os.path.basename(local_dir)}] "
        if not os.path.isdir(local_dir):
            raise ValueError(f'RemoteJob directory does not exist: "{local_dir}"')
        self._local_path = local_dir
        self._job_name = os.path.basename(local_dir)
        self._log(logging.DEBUG, f"Creating RemoteJob for local directory: {self._job_name}")

        # setting up transferer and runner
        self._runner.set_label(self._label)
        self._transfer.set_local_directory(self._local_path)

        # initialise and load saved state, if any
        self._state_file = os.path.join(local_dir, self.STATE_FILE)
        self._load_state(force)

        # handle Globus here
        self.do_globus_auth(runner=runner, transfer=transfer)

    def get_remote_directory(self):
        """Return the remote directory"""
        return self._remote_full_path

    def get_remote_base_directory(self):
        """Return the remote base directory"""
        return self._transfer.get_remote_base_directory()

    def set_remote_directory(self, remote_full_path, remote_basename):
        """Set the remote directory"""
        self._remote_full_path = remote_full_path
        self._transfer.set_remote_directory(remote_basename)
        self._save_state()

    def make_remote_directory(self, prefix=None, retries=True):
        """Create the remote directory"""
        # creating a remote directory for running in
        if self._remote_full_path is None:
            # remote path is based on local path basename
            local_basename = os.path.basename(self._local_path)

            if prefix is None:
                prefix = f"{local_basename}-{self._timestamp}"

            # create a remote directory
            remote_full_path, remote_basename = self._runner.make_remote_directory(
                self._transfer.get_remote_base_directory(),
                prefix,
                retries=retries,
            )
            self._log(logging.DEBUG, f"Remote directory created: {remote_full_path} ({remote_basename})")
            self._remote_full_path = remote_full_path

            # set the directory on the transferer too
            self._transfer.set_remote_directory(remote_basename)

            # save state
            self._save_state()

    def do_globus_auth(self, runner=None, transfer=None):
        """Handle globus auth here"""
        # get the scopes
        runner_scopes = self._runner.get_globus_scopes() if runner is None else []
        transfer_scopes = self._transfer.get_globus_scopes() if transfer is None else []
        all_scopes = runner_scopes + transfer_scopes
        self._log(logging.DEBUG, f"Globus scopes: {all_scopes}")

        # do the auth if required
        globus_cli = None
        if len(all_scopes) > 0:
            globus_cli = utils.handle_globus_auth(all_scopes)

        # setup runner
        self._log(logging.DEBUG, "Setting up globus auth for runner")
        self._runner.setup_globus_auth(globus_cli, runner=runner)

        # setup transferer
        self._log(logging.DEBUG, "Setting up globus auth for transferer")
        self._transfer.setup_globus_auth(globus_cli, transfer=transfer)

    def cleanup(self):
        """
        Cleanup the remote job (delete remote directory...)

        """
        raise NotImplementedError

    def __repr__(self):
        return f'RemoteJob({self._local_path})'

    def _load_state(self, force):
        """
        Load the saved state, if any.

        """
        if self._state_file is not None and os.path.exists(self._state_file) and not force:
            with open(self._state_file) as fh:
                state_dict = json.load(fh)
            self._log(logging.DEBUG, f"Loading state: {state_dict}")

            self._remote_full_path = state_dict["remote_directory"]
            self._remote_basename = state_dict["remote_basename"]
            self._uploaded = state_dict["uploaded"]
            self._run_started = state_dict["run_started"]
            self._run_succeeded = state_dict["run_succeeded"]
            self._run_failed = state_dict["run_failed"]
            self._downloaded = state_dict["downloaded"]
            self._cancelled = state_dict["cancelled"]

            if "transfer" in state_dict:
                self._transfer.load_state(state_dict["transfer"])
            if "runner" in state_dict:
                self._runner.load_state(state_dict["runner"])

    def _save_state(self):
        """
        Save the current state of the remote job so it can be resumed later.

        """
        if self._state_file is not None:
            state_dict = {
                "remote_directory": self._remote_full_path,
                "remote_basename": self._remote_basename,
                "uploaded": self._uploaded,
                "run_started": self._run_started,
                "run_succeeded": self._run_succeeded,
                "run_failed": self._run_failed,
                "downloaded": self._downloaded,
                "cancelled": self._cancelled,
            }

            transfer_state = self._transfer.save_state()
            if len(transfer_state):
                state_dict["transfer"] = transfer_state

            runner_state = self._runner.save_state()
            if len(runner_state):
                state_dict["runner"] = runner_state

            self._log(logging.DEBUG, f"Saving state: {state_dict}")
            with open(self._state_file, 'w') as fh:
                json.dump(state_dict, fh, indent=4)

    def upload_files(self):
        """Upload files to remote"""
        if self._uploaded:
            self._log(logging.INFO, "Already uploaded files")
        else:
            self._log(logging.INFO, "Uploading files...")

            # read in the files to be uploaded
            self._read_uploads_file()

            # do the upload
            upload_time = time.perf_counter()
            self._transfer.upload_files(self._upload_files)
            upload_time = time.perf_counter() - upload_time
            self._log(logging.INFO, f"Uploaded {len(self._upload_files)} files in {upload_time:.1f} seconds")
            self._uploaded = True
            self._save_state()

    def download_files(self):
        """Download file from remote"""
        if self._downloaded:
            self._log(logging.INFO, "Already downloaded files")
        elif self._cancelled:
            self._log(logging.ERROR, "Cannot download files for a cancelled run")
            raise RuntimeError("Cannot download files for a cancelled run")
        elif not self.run_completed():
            self._log(logging.ERROR, "Run must be completed before we can download files")
            raise RuntimeError("Run must be completed before we can download files")
        else:
            # read in files to be downloaded
            self._read_downloads_file()

            # get checksums
            downloads_checksums = self._runner.get_checksums(
                self._remote_full_path,
                self._download_files,
            )
            no_checksum = [f for f in downloads_checksums if downloads_checksums[f] is None]
            if len(no_checksum):
                self._log(logging.ERROR,
                          f"Could not calculate checksums for the following files, their downloads will not be verified: {', '.join(no_checksum)}")

            # do the download
            self._log(logging.INFO, "Downloading files...")
            download_time = time.perf_counter()
            self._transfer.download_files(self._download_files, downloads_checksums)
            download_time = time.perf_counter() - download_time
            self._log(logging.INFO, f"Downloaded {len(self._download_files)} files in {download_time:.1f} seconds")
            self._downloaded = True
            self._save_state()

            # if the run failed, raise an exception now
            if not self._run_succeeded:
                raise RemoteJobRunnerError("Running the job failed")

    def run_start(self):
        """Start running the processing"""
        if self._run_started:
            self._log(logging.INFO, "Run already started")
        elif not self._uploaded:
            self._log(logging.ERROR, "Files must be uploaded before we can start the run")
            raise RuntimeError("Files must be uploaded before we can start the run")
        else:
            self._log(logging.INFO, "Starting run...")
            self._run_started = retry_call(self._runner.start,
                                           fargs=(self._remote_full_path,),
                                           tries=self._retry_tries,
                                           backoff=self._retry_backoff,
                                           delay=self._retry_delay)
            self._save_state()

    def run_wait(self, polling_interval=None, min_polling_override=False):
        """Wait for the processing to complete"""
        if self.run_completed():
            self._log(logging.INFO, "Run already completed")
        elif self._cancelled:
            self._log(logging.ERROR, "Cannot wait for a run that has been cancelled")
            raise RuntimeError("Cannot wait for a run that has been cancelled")
        elif not self._run_started:
            self._log(logging.ERROR, "Run must be started before we can wait for it to complete")
            raise RuntimeError("Run must be started before we can wait for it to complete")
        else:
            self._log(logging.INFO, "Waiting for run to complete...")
            run_succeeded = retry_call(self._runner.wait, fkwargs={'polling_interval': polling_interval,
                                                                   'min_polling_override': min_polling_override},
                                       tries=self._retry_tries, backoff=self._retry_backoff,
                                       delay=self._retry_delay)
            self.set_run_completed(success=run_succeeded)
            self._save_state()

    def run_cancel(self):
        """Cancel the run."""
        if not self._run_started:
            self._log(logging.WARNING, "Cannot cancel a run that hasn't started")
        elif self.run_completed():
            self._log(logging.WARNING, "Cannot cancel a run that has already completed")
        elif self._cancelled:
            self._log(logging.WARNING, "Already cancelled")
        else:
            self._runner.cancel()
            self._cancelled = True
            self._save_state()

    def upload_and_start(self):
        """
        Complete the upload files and start running steps.

        """
        self.make_remote_directory()
        self.upload_files()
        self.run_start()

    def wait_and_download(self, polling_interval=None, min_polling_override=False):
        """
        Wait for the run to complete and then download files.

        """
        self.run_wait(polling_interval=polling_interval, min_polling_override=min_polling_override)
        self.download_files()

    def workflow(self):
        """do everything: upload, run, download"""
        self._upload_and_start()
        self.wait_and_download()

    def get_transferer(self):
        """Return transferer"""
        return self._transfer

    def get_runner(self):
        """Return runner"""
        return self._runner

    def write_stderr_if_not_finished(self, msg):
        """
        Write an stderr.txt file to the directory to indicate failure.

        :param msg: The message to be written to the stderr.txt file

        If the job has completed (i.e. downloaded files), don't do anything

        If stderr.txt already exists, don't do anything

        Note: this functionality exists only to help WFN (wings for nonmem) to
              detect when RJM has exited with an error

        """
        if self.files_downloaded():
            self._log(logging.DEBUG, "Skipping writing stderr on error since files have been downloaded already")
        else:
            stderr_file = os.path.join(self._local_path, "stderr.txt")
            if not os.path.exists(stderr_file):
                self._log(logging.DEBUG, f"Writing stderr file: {stderr_file}")
                with open(stderr_file, "w") as fh:
                    fh.write(msg)
            else:
                self._log(logging.DEBUG, "Skipping writing stderr file on error since it already exists")
