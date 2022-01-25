
import logging
import os
import time
import shutil

import globus_sdk
import requests

from .transferer_base import TransfererBase
from . import utils


logger = logging.getLogger(__name__)


class GlobusHttpsTransferer(TransfererBase):
    """
    Upload and download files to a remote Globus endpoint (guest collection)
    using HTTPS.

    """
    def __init__(self, local_path, config=None):
        super(GlobusHttpsTransferer, self).__init__(local_path, config=config)

        # the Globus endpoint for the remote guest collection
        self._remote_endpoint = self._config.get("GLOBUS", "remote_endpoint")
        self._remote_base_path = self._config.get("GLOBUS", "remote_path")
        self._https_scope = utils.HTTPS_SCOPE.format(endpoint_id=self._remote_endpoint)

        # https uploads/downloads
        self._https_base_url = None
        self._https_auth_header = None

        # transfer client
        self._tc = None

    def get_globus_scopes(self):
        """Return list of required globus scopes."""
        required_scopes = [
            utils.TRANSFER_SCOPE,
            self._https_scope,
        ]
        logger.debug(f"Required Globus scopes are: {required_scopes}")

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
        logger.debug(f"Remote endpoint HTTPS base URL: {self._https_base_url}")
        # HTTPS authentication header
        https_token_dict = globus_cli.load_tokens_by_scope()[self._https_scope]  # Globus SDK v2
        self._https_auth_header = f"{https_token_dict['token_type']} {https_token_dict['access_token']}"
        #a = authorisers[self._https_scope]  # Globus SDK v3???
        #self._https_auth_header = a.get_authorization_header()

    def upload_file(self, filename):
        """Upload file to remote"""
        # make the URL to upload file to
        upload_url = f"{self._https_base_url}/{self._remote_path}/{filename}"
        logger.debug(f"Uploading file to: {upload_url}")

        # path to local file
        local_file = os.path.join(self._local_path, filename)

        # authorisation
        headers = {
            "Authorization": self._https_auth_header,
        }

        # upload
        start_time = time.perf_counter()
        with open(local_file, 'rb') as f:
            r = requests.put(upload_url, data=f, headers=headers)
        r.raise_for_status()
        upload_time = time.perf_counter() - start_time
        self.log_transfer_time("Uploaded", local_file, upload_time)

    def download_file(self, filename):
        """Download a file from remote"""
        # file to download and URL
        download_url = f"{self._https_base_url}/{self._remote_path}/{filename}"
        logger.debug(f"Downloading file from: {download_url}")

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
