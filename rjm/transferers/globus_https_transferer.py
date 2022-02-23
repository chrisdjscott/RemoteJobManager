
import logging
import os
import time
import shutil
import concurrent.futures
from typing import List

import globus_sdk
import requests

from rjm.transferers.transferer_base import TransfererBase
from rjm import utils


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

        # https uploads/downloads
        self._https_base_url = None
        self._https_auth_header = None

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
        self._log(logging.DEBUG, f"Required Globus scopes are: {required_scopes}")

        return required_scopes

    def list_directory(self, path="/"):
        """List the contents (just names) of the provided path (directory)"""
        return [[item["name"] for item in self._tc.operation_ls(self._remote_endpoint, path=path)]]

    def make_directory(self, path):
        """Create a directory at the specified path"""
        self._tc.operation_mkdir(self._remote_endpoint, path)

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

    def _upload_file(self, filename: str):
        """
        Upload file to remote.

        :param filename: File name relative to `local_path`
        :type filename: str

        """
        # basename for remote file name
        basename = os.path.basename(filename)

        # make the URL to upload file to
        upload_url = f"{self._https_base_url}/{self._remote_path}/{basename}"

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

    def upload_files(self, filenames: List[str]):
        """
        Upload the given files to the remote directory.

        :param filenames: List of files to upload to the
            remote directory.
        :type filenames: iterable of str

        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # start the uploads and mark each future with its filename
            future_to_fname = {
                executor.submit(self._upload_file, fname): fname for fname in filenames
            }

            # wait for completion
            for future in concurrent.futures.as_completed(future_to_fname):
                future.result()

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
        download_url = f"{self._https_base_url}/{self._remote_path}/{filename}"

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
