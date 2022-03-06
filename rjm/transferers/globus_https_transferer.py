
import logging
import os
import time
import shutil
import concurrent.futures
from typing import List

import globus_sdk
import requests
from retry.api import retry_call

from rjm.transferers.transferer_base import TransfererBase
from rjm import utils
from rjm.errors import RemoteJobTransfererError


logger = logging.getLogger(__name__)


class GlobusHttpsTransferer(TransfererBase):
    """
    Upload and download files to a remote Globus endpoint (guest collection)
    using HTTPS.

    """
    def __init__(self, config=None):
        super(GlobusHttpsTransferer, self).__init__(config=config)

        # the Globus endpoint for the remote guest collection
        self._remote_endpoint = self._config.get("GLOBUS", "remote_endpoint")
        self._remote_base_path = self._config.get("GLOBUS", "remote_path")
        self._https_scope = utils.HTTPS_SCOPE.format(endpoint_id=self._remote_endpoint)

        # retry params
        self._retry_tries = self._config.getint("RETRY", "tries", fallback=utils.DEFAULT_RETRY_TRIES)
        self._retry_backoff = self._config.getint("RETRY", "backoff", fallback=utils.DEFAULT_RETRY_BACKOFF)
        self._retry_delay = self._config.getint("RETRY", "delay", fallback=utils.DEFAULT_RETRY_DELAY)

        # https uploads/downloads
        self._https_base_url = None
        self._https_auth_header = None
        self._max_workers = None

        # transfer client
        self._tc = None

    def _log(self, level, message, *args, **kwargs):
        """Add a label to log messages, identifying this specific RemoteJob"""
        logger.log(level, self._label + message, *args, **kwargs)

    def get_globus_scopes(self):
        """Return list of required globus scopes."""
        required_scopes = [
            utils.TRANSFER_SCOPE,
            self._https_scope,
        ]

        return required_scopes

    def list_directory(self, path="/"):
        """List the contents (just names) of the provided path (directory)"""
        return [[item["name"] for item in self._tc.operation_ls(self._remote_endpoint, path=path)]]

    def make_directory(self, path):
        """Create a directory at the specified path"""
        resp = self._tc.operation_mkdir(self._remote_endpoint, path)
        self._log(logging.DEBUG, f"response from operation_mkdir: {resp}")

    def setup_globus_auth(self, globus_cli):
        """Setting up Globus authentication."""
        # creating Globus transfer client
        authorisers = globus_cli.get_authorizers_by_scope(requested_scopes=[utils.TRANSFER_SCOPE, self._https_scope])
        self._tc = globus_sdk.TransferClient(authorizer=authorisers[utils.TRANSFER_SCOPE])

        # setting up HTTPS uploads/downloads
        # get the base URL for uploads and downloads
        endpoint = self._tc.get_endpoint(self._remote_endpoint)
        self._https_base_url = endpoint['https_server']
        self._log(logging.DEBUG, f"Remote endpoint HTTPS base URL: {self._https_base_url}")
        # HTTPS authentication header
        a = authorisers[self._https_scope]
        self._https_auth_header = a.get_authorization_header()

    def _url_for_file(self, filename: str):
        """
        Create Globus HTTPS URL for given remote file name.

        :param filename: File name to create URL for (should be a base name)
        :type filename: str

        """
        return f"{self._https_base_url}/{self._remote_path}/{filename}"

    def _upload_file(self, filename: str):
        """
        Upload file to remote.

        :param filename: File to be uploaded
        :type filename: str

        """
        # use basename for remote file name
        basename = os.path.basename(filename)

        # make the URL to upload file to
        upload_url = self._url_for_file(basename)

        # authorisation
        headers = {
            "Authorization": self._https_auth_header,
        }

        # upload
        start_time = time.perf_counter()
        with open(filename, 'rb') as f:
            r = requests.put(upload_url, data=f, headers=headers)
        r.raise_for_status()
        upload_time = time.perf_counter() - start_time
        self.log_transfer_time("Uploaded", filename, upload_time)

    def _upload_file_with_retries(self, filename: str):
        """
        Upload file, retrying if the upload fails

        :param filename: File to be uploaded
        :type filename: str

        """
        retry_call(self._upload_file, fargs=(filename,), tries=self._retry_tries,
                   backoff=self._retry_backoff, delay=self._retry_delay)

    def upload_files(self, filenames: List[str]):
        """
        Upload the given files to the remote directory.

        :param filenames: List of files to upload to the
            remote directory.
        :type filenames: iterable of str

        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            # start the uploads and mark each future with its filename
            future_to_fname = {
                executor.submit(self._upload_file_with_retries, fname): fname for fname in filenames
            }

            # wait for completion
            errors = []
            for future in concurrent.futures.as_completed(future_to_fname):
                fname = future_to_fname[future]
                try:
                    future.result()
                except Exception as exc:
                    msg = f"Failed to upload '{fname}': {exc}"
                    self._log(logging.ERROR, msg)
                    errors.append(msg)

            # handle errors
            if len(errors):
                msg = ["Failed to upload files in '{self._local_path}':"]
                msg.append("")
                for err in errors:
                    msg.append("  - " + err)
                msg = "\n".join(msg)
                raise RemoteJobTransfererError(msg)

    def download_files(self, filenames: List[str]):
        """
        Download the given files (which should be relative to `remote_path`) to
        the local directory.

        :param filenames: List of file names relative to the `remote_path`
            directory to download to the local directory.
        :type filenames: iterable of str

        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # start the uploads and mark each future with its filename
            future_to_fname = {
                executor.submit(self._download_file, fname): fname for fname in filenames
            }

            # wait for completion
            for future in concurrent.futures.as_completed(future_to_fname):
                fname = future_to_fname[future]
                try:
                    future.result()
                except requests.exceptions.HTTPError as exc:
                    # if fail to download, just print warning
                    self._log(logging.WARNING, f"Failed to download file '{fname}': {exc}")

    def _download_file(self, filename: str):
        """
        Download a file from remote.

        :param filename: File name relative to `remote_path`
        :type filename: str

        """
        # file to download and URL
        download_url = self._url_for_file(filename)

        # path to local file
        local_file = os.path.join(self._local_path, filename)

        # authorisation
        headers = {
            "Authorization": self._https_auth_header,
        }

        # download
        start_time = time.perf_counter()
        with requests.get(download_url, headers=headers, stream=True) as r:
            with open(local_file, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        r.raise_for_status()
        download_time = time.perf_counter() - start_time
        self.log_transfer_time("Downloaded", local_file, download_time)
