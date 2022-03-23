
import logging
import os
import time
import shutil
import concurrent.futures
import urllib.parse
import hashlib

import globus_sdk
import requests
from retry.api import retry_call

from rjm.transferers.transferer_base import TransfererBase
from rjm import utils
from rjm.errors import RemoteJobTransfererError


DOWNLOAD_CHUNK_SIZE = 8192
FILE_CHUNK_SIZE = 8192

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

        # Globus stuff
        self._https_authoriser = None
        self._https_auth_header = None

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

    def setup_globus_auth(self, globus_cli, transfer=None):
        """Setting up Globus authentication."""
        if transfer is None:
            # creating Globus transfer client
            authorisers = globus_cli.get_authorizers_by_scope(requested_scopes=[utils.TRANSFER_SCOPE, self._https_scope])
            tc = globus_sdk.TransferClient(authorizer=authorisers[utils.TRANSFER_SCOPE])

            # setting up HTTPS uploads/downloads
            # get the base URL for uploads and downloads
            endpoint = tc.get_endpoint(self._remote_endpoint)
            self._https_base_url = endpoint['https_server']
            self._log(logging.DEBUG, f"Remote endpoint HTTPS base URL: {self._https_base_url}")
            # HTTPS authoriser
            self._https_authoriser = authorisers[self._https_scope]
        else:
            # initialise from passed in transferer object
            self._log(logging.DEBUG, "Initialising transferer from another")
            self._https_base_url = transfer.get_https_base_url()
            self._https_authoriser = transfer.get_https_authoriser()

    def get_https_base_url(self):
        """Return the https base url"""
        return self._https_base_url

    def get_https_authoriser(self):
        """Return the globus authoriser"""
        return self._https_authoriser

    def _url_for_file(self, filename: str):
        """
        Create Globus HTTPS URL for given remote file name.

        :param filename: File name to create URL for (should be a base name)
        :type filename: str

        """
        url = urllib.parse.urljoin(
            self._https_base_url,
            urllib.parse.quote(f"{self._remote_path}/{filename}"),
        )

        return url

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

    def upload_files(self, filenames: list[str]):
        """
        Upload the given files to the remote directory.

        :param filenames: List of files to upload to the
            remote directory.
        :type filenames: iterable of str

        """
        # make sure we have a current access token
        self._https_auth_header = self._https_authoriser.get_authorization_header()

        # start a pool of threads to do the uploading
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
            msg = [f"Failed to upload files in '{self._local_path}':"]
            msg.append("")
            for err in errors:
                msg.append("  - " + err)
            msg = "\n".join(msg)
            raise RemoteJobTransfererError(msg)

    def download_files(self, filenames, checksums):
        """
        Download the given files (which should be relative to `remote_path`) to
        the local directory.

        :param filenames: list of file names relative to the `remote_path`
            directory to download to the local directory.
        :param checksums: dictionary with filenames as keys and checksums as
            values

        """
        # make sure we have a current access token
        self._https_auth_header = self._https_authoriser.get_authorization_header()

        # start a pool of threads to do the downloading
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # start the uploads and mark each future with its filename
            future_to_fname = {
                executor.submit(
                    self._download_file_with_retries,
                    fname,
                    checksums[fname],
                ): fname for fname in filenames
            }

            # wait for completion
            errors = []
            for future in concurrent.futures.as_completed(future_to_fname):
                fname = future_to_fname[future]
                try:
                    future.result()
                except Exception as exc:
                    msg = f"Failed to download '{fname}': {exc}"
                    self._log(logging.ERROR, msg)
                    errors.append(msg)

        # handle errors
        if len(errors):
            msg = [f"Failed to download files in '{self._local_path}':"]
            msg.append("")
            for err in errors:
                msg.append("  - " + err)
            msg = "\n".join(msg)
            raise RemoteJobTransfererError(msg)

    def _download_file_with_retries(self, filename: str, checksum: str):
        """
        Download file, retrying if the download fails

        :param filename: file to be downloaded, relative to `remote_path`
        :param checksum: the expected checksum of the file

        """
        retry_call(self._download_file, fargs=(filename, checksum),
                   tries=self._retry_tries, backoff=self._retry_backoff,
                   delay=self._retry_delay)

    def _calculate_checksum(self, filename):
        """
        Calculate the checksum of the given file

        """
        with open(filename, 'rb') as fh:
            checksum = hashlib.sha256()
            while chunk := fh.read(FILE_CHUNK_SIZE):
                checksum.update(chunk)

        return checksum.hexdigest()

    def _download_file(self, filename: str, checksum: str):
        """
        Download a file from remote.

        :param filename: file name relative to `remote_path`
        :param checksum: the expected checksum of the file

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
                for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
        r.raise_for_status()
        download_time = time.perf_counter() - start_time

        # check the checksum
        if checksum is not None:
            checksum_local = self._calculate_checksum(local_file)
            if checksum != checksum_local:
                msg = f"Checksums of downloaded {local_file} don't match ({checksum} vs {checksum_local})"
                self._log(logging.ERROR, msg)
                raise RemoteJobTransfererError(msg)

        self.log_transfer_time("Downloaded", local_file, download_time)
