
import logging
import os
import time
import concurrent.futures
import urllib.parse
import hashlib
import platform

import globus_sdk
import requests
from retry.api import retry_call

from rjm.transferers.transferer_base import TransfererBase
from rjm import utils
from rjm.errors import RemoteJobTransfererError


DOWNLOAD_CHUNK_SIZE = 8000000
DOWNLOAD_SUFFIX = '.rjm'
FILE_CHUNK_SIZE = 8000000
REQUESTS_TIMEOUT = 30

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
        self._retry_tries, self._retry_backoff, self._retry_delay, self._retry_max_delay = utils.get_retry_values_from_config(self._config)

        # https uploads/downloads
        self._https_base_url = None
        self._https_auth_header = None
        self._max_workers = None

        # Globus stuff
        self._https_authoriser = None
        self._https_auth_header = None
        self._transfer_client = None

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
            self._transfer_client = globus_sdk.TransferClient(authorizer=authorisers[utils.TRANSFER_SCOPE])
            self._log(logging.DEBUG, f"Created transfer client: {self._transfer_client}")

            # setting up HTTPS uploads/downloads
            # get the base URL for uploads and downloads
            endpoint = self._transfer_client.get_endpoint(self._remote_endpoint)
            self._https_base_url = endpoint['https_server']
            self._log(logging.DEBUG, f"Remote endpoint HTTPS base URL: {self._https_base_url}")
            # HTTPS authoriser
            self._https_authoriser = authorisers[self._https_scope]
        else:
            # initialise from passed in transferer object
            self._log(logging.DEBUG, "Initialising transferer from another")
            self._https_base_url = transfer.get_https_base_url()
            self._https_authoriser = transfer.get_https_authoriser()
            self._transfer_client = transfer.get_transfer_client()

    def get_transfer_client(self):
        """Return the transfer client"""
        return self._transfer_client

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
            r = requests.put(upload_url, data=f, headers=headers, timeout=REQUESTS_TIMEOUT)
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
                   backoff=self._retry_backoff, delay=self._retry_delay,
                   max_delay=self._retry_max_delay)

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
            msg = os.linesep.join(msg)
            raise RemoteJobTransfererError(msg)

    def download_files(self, filenames, checksums, retries=True):
        """
        Download the given files (which should be relative to `remote_path`) to
        the local directory.

        :param filenames: list of file names relative to the `remote_path`
            directory to download to the local directory.
        :param checksums: dictionary with filenames as keys and checksums as
            values
        :param retries: optional, retry downloads if they fail (default is True)

        """
        # list directory, so we only try downloading files that exist
        self._log(logging.DEBUG, f"Listing remote directory: {self._remote_path}")
        remote_files = self.list_directory(self._remote_path)

        # look for missing files
        errors = 0
        existing_files = []
        for fn in filenames:
            if fn in remote_files:
                existing_files.append(fn)
                self._log(logging.DEBUG, f"File to download: '{fn}': {remote_files[fn]}")
            else:
                errors += 1
                self._log(logging.ERROR, f"File to download is missing: '{fn}'")

        # make sure we have a current access token
        self._https_auth_header = self._https_authoriser.get_authorization_header()

        # function to download files
        download_func = self._download_file_with_retries if retries else self._download_file
        self._log(logging.DEBUG, f"Download function is: {download_func}")

        # start a pool of threads to do the downloading
        self._log(logging.DEBUG, "Using ThreadPoolExecutor to download files")
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            # start the downloads and mark each future with its filename
            future_to_fname = {
                executor.submit(
                    download_func,
                    fname,
                    checksums[fname],
                ): fname for fname in existing_files
            }

            # wait for completion
            self._log(logging.DEBUG, f"Waiting for {len(future_to_fname)} files to be downloaded")
            num_to_download = len(future_to_fname)
            downloaded_tmp_files = []
            count = 0
            for future in concurrent.futures.as_completed(future_to_fname):
                fname = future_to_fname[future]
                try:
                    downloaded_tmp_files.append(future.result())
                    self._log(logging.DEBUG, f"Received download success from thread for {fname} ({count+1} of {num_to_download})")
                except Exception as exc:
                    self._log(logging.ERROR, f"Failed to download '{fname}': {exc}")
                    errors += 1
                count += 1

        # at this point we have downloaded to temporary files, now we need to rename them to the actual files
        self._log(logging.DEBUG, f"Renaming {len(downloaded_tmp_files)} downloaded temporary files")
        start_time = time.perf_counter()
        for tmp_file in downloaded_tmp_files:
            save_file = tmp_file.removesuffix(DOWNLOAD_SUFFIX)
            self._log(logging.DEBUG, f'Renaming "{tmp_file}" -> "{save_file}"')
            os.replace(tmp_file, save_file)
        rename_time = time.perf_counter() - start_time
        self._log(logging.DEBUG, f"Finished renaming files in {rename_time:.1f} s")

        # if there were any errors downloading files, raise an exception now
        if errors > 0:
            raise RemoteJobTransfererError(f"Failed to download files in '{self._local_path}'")

        self._log(logging.DEBUG, "Finished downloading files")

    def _download_file_with_retries(self, filename: str, checksum: str):
        """
        Download file, retrying if the download fails

        :param filename: file to be downloaded, relative to `remote_path`
        :param checksum: the expected checksum of the file

        """
        return retry_call(self._download_file, fargs=(filename, checksum),
                          tries=self._retry_tries, backoff=self._retry_backoff,
                          delay=self._retry_delay, max_delay=self._retry_max_delay)

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
        self._log(logging.DEBUG, f"Starting download of: {filename}")

        # check destination directory exists
        if not os.path.exists(self._local_path):
            self._log(logging.WARNING, f"Download directory does not exist - creating it ({self._local_path})")
            os.makedirs(self._local_path, exist_ok=True)

        # file to download and URL
        download_url = self._url_for_file(filename)

        # download to a temporary file first
        local_file_tmp = os.path.join(self._local_path, filename + DOWNLOAD_SUFFIX)
        self._log(logging.DEBUG, f"Downloading {filename} to temporary file first: {local_file_tmp}")
        if len(local_file_tmp) > 255 and platform.system() == "Windows":
            self._log(logging.WARNING, f"Temporary filename is long ({len(local_file_tmp)} characters), may cause problems on Windows")

        # authorisation
        headers = {
            "Authorization": self._https_auth_header,
        }

        # download with temporary local file name
        start_time = time.perf_counter()
        with requests.get(download_url, headers=headers, stream=True, timeout=REQUESTS_TIMEOUT) as r:
            self._log(logging.DEBUG, f"Requests response for {filename}: {r.status_code}, {r.reason}")
            r.raise_for_status()
            with open(local_file_tmp, 'wb') as f:
                for chunk in r.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
        download_time = time.perf_counter() - start_time
        self._log(logging.DEBUG, f"Finished writing {local_file_tmp} (file exists? {os.path.exists(local_file_tmp)})")

        # check the checksum of the downloaded file
        if checksum is not None:
            self._log(logging.DEBUG, f"Verifying checksum of \"{local_file_tmp}\"...")
            checksum_local = self._calculate_checksum(local_file_tmp)
            if checksum != checksum_local:
                msg = f"Checksum of downloaded \"{local_file_tmp}\" doesn't match ({checksum_local} vs {checksum})"
                self._log(logging.ERROR, msg)
                raise RemoteJobTransfererError(msg)

        self.log_transfer_time("Downloaded", local_file_tmp, download_time)

        return local_file_tmp

    def list_directory(self, path: str):
        """
        Return a listing of the given directory.

        :param path: Path to the directory

        """
        self._log(logging.DEBUG, f"Listing remote directory: {path}")

        # list of attributes to show in the listing
        keep_attrs = [
            "type",
            "permissions",
            "size",
            "user",
        ]

        # call globus function with retries
        ls_result = retry_call(
            self._transfer_client.operation_ls,
            fargs=(self._remote_endpoint,),
            fkwargs={'path': path},
            tries=self._retry_tries,
            backoff=self._retry_backoff,
            delay=self._retry_delay,
            max_delay=self._retry_max_delay,
        )

        # extract listing with desired attributes
        listing = {}
        for entry in ls_result:
            listing[entry["name"]] = {attr: entry[attr] for attr in keep_attrs}

        self._log(logging.DEBUG, f"Contents: {listing}")

        return listing
