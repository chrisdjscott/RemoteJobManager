
import os
import stat
import time
import platform
import logging
import paramiko

from rjm.transferers.transferer_base import TransfererBase
from rjm import utils
from rjm.errors import RemoteJobTransfererError


DOWNLOAD_SUFFIX = '.rjm'


logger = logging.getLogger(__name__)


class ParamikoSftpTransferer(TransfererBase):
    """
    Upload and download files to a remote machine using SFTP via
    the Paramiko library.

    """
    def __init__(self, config=None):
        super(ParamikoSftpTransferer, self).__init__(config=config)

        # config
        self._ssh_private_key_file = self._config.get("PARAMIKO", "private_key_file")
        self._remote_address = self._config.get("PARAMIKO", "remote_address")
        self._remote_user = self._config.get("PARAMIKO", "remote_user")
        self._remote_base_path = self._config.get("PARAMIKO", "remote_base_path")

        # retry params
        self._retry_tries, self._retry_backoff, self._retry_delay, self._retry_max_delay = utils.get_retry_values_from_config(self._config)

        # TODO: move to a setup function??
        self._private_key = None
        self._ssh_client = None
        self._sftp_client = None

#    def __del__(self):
#        if self._sftp_client is not None:
#            self._sftp_client.close()
#        if self._ssh_client is not None:
#            self._ssh_client.close()

    def _log(self, level, message, *args, **kwargs):
        """Add a label to log messages, identifying this specific RemoteJob"""
        logger.log(level, self._label + message, *args, **kwargs)

    def setup(self, *args, **kwargs):
        """Setup the SFTP client"""
        self._log(logging.DEBUG, "Setting up ParamikoSftpTransferer...")
        self._ssh_client = paramiko.SSHClient()
        self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self._log(logging.DEBUG, f"Loading SSH key from {self._ssh_private_key_file}")
        self._private_key = paramiko.Ed25519Key.from_private_key_file(self._ssh_private_key_file)

        # Connect to server
        self._ssh_client.connect(
            hostname=self._remote_address,
            port=22,
            username=self._remote_user,
            pkey=self._private_key,
            timeout=30,
        )

        # Create SFTP client
        self._sftp_client = self._ssh_client.open_sftp()
        self._log(logging.DEBUG, f"Connected to: {self._remote_address} ({self._sftp_client})")

    def upload_files(self, filenames: list[str]):
        """
        Upload the given files to the remote directory.

        :param filenames: List of files to upload to the
            remote directory.
        :type filenames: iterable of str

        """
        self._log(logging.DEBUG, "Uploading files...")
        for filename in filenames:
            # use basename for remote file name
            basename = os.path.basename(filename)
            remote_filename = f"{self._remote_path}/{basename}"
            self._log(logging.DEBUG, f"Uploading: {filename} -> {remote_filename}")

            # upload
            start_time = time.perf_counter()
            self._sftp_client.put(filename, remote_filename)
            upload_time = time.perf_counter() - start_time
            self.log_transfer_time("Uploaded", filename, upload_time)

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
        errors = 0

        # check local path exists
        if not os.path.exists(self._local_path):
            self._log(logging.WARNING, f"Download directory does not exist - creating it ({self._local_path})")
            os.makedirs(self._local_path, exist_ok=True)

        # loop over the files to download
        self._log(logging.DEBUG, "Downloading files...")
        downloaded_tmp_files = []
        for fn in filenames:
            self._log(logging.DEBUG, f"Downloading: {fn}")
            remote_fn = f"{self._remote_path}/{fn}"

            # check it exists first


            # download to temporary file
            local_file_tmp = os.path.join(self._local_path, fn + DOWNLOAD_SUFFIX)
            self._log(logging.DEBUG, f"Downloading {fn} to temporary file first: {local_file_tmp}")
            if len(local_file_tmp) > 255 and platform.system() == "Windows":
                self._log(logging.WARNING, f"Temporary filename is long ({len(local_file_tmp)} characters), may cause problems on Windows")

            # run the download
            start_time = time.perf_counter()
            try:
                self._sftp_client.get(remote_fn, local_file_tmp)
            except FileNotFoundError as exc:
                errors += 1
                self._log(logging.ERROR, f"File to download is missing: '{fn}' ({exc})")
            else:
                download_time = time.perf_counter() - start_time
                self.log_transfer_time("Downloaded", local_file_tmp, download_time)

                # validate the checksum of the downloaded file
                if fn in checksums:
                    checksum = checksums[fn]
                    self._log(logging.DEBUG, f"Verifying checksum of \"{local_file_tmp}\"...")
                    checksum_local = self._calculate_checksum(local_file_tmp)
                    if checksum != checksum_local:
                        msg = f"Checksum of downloaded \"{local_file_tmp}\" doesn't match ({checksum_local} vs {checksum})"
                        self._log(logging.ERROR, msg)
                        errors += 1

                downloaded_tmp_files.append(local_file_tmp)

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

    def list_directory(self, path: str):
        """
        Return a listing of the given directory.

        :param path: Path to the directory

        """
        self._log(logging.DEBUG, f"Listing remote directory: {path}")

        raw_listing = self._sftp_client.listdir_attr(path=self._remote_path)

        listing = {}
        for entry in raw_listing:
            mode = entry.st_mode
            listing[entry.filename] = {
                "permissions": oct(mode)[-3:],
                "directory": stat.S_ISDIR(mode),
                "size": entry.st_size,
                "user": entry.st_uid,
            }

        self._log(logging.DEBUG, f"Listing: {listing}")

        return listing
