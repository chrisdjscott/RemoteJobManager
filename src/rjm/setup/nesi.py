
import os
import sys
import stat
import time
import logging
import getpass
import tempfile
import importlib.resources

import paramiko
from globus_sdk import GCSClient
from globus_sdk.services.gcs.data import GuestCollectionDocument
from globus_compute_sdk.sdk.login_manager import LoginManager as GlobusComputeLoginManager

from rjm import utils


logger = logging.getLogger(__name__)

GATEWAY = "lander.hpc.nesi.org.nz"
LOGIN_NODE = "login.hpc.nesi.org.nz"
FUNCX_NODES = [
    "login01",
    "login02",
]
FUNCX_MODULE = "globus-compute-endpoint/3.7.0-foss-2023a-Python-3.11.6"
FUNCX_ENDPOINT_NAME = "rjm"
GLOBUS_NESI_COLLECTION = 'cc45cfe3-21ae-4e31-bad4-5b3e7d6a2ca1'
GLOBUS_NESI_ENDPOINT = '90b0521d-ebf8-4743-a492-b07176fe103f'
GLOBUS_NESI_GCS_ADDRESS = "c61f4.bd7c.data.globus.org"
NESI_PERSIST_SCRIPT_PATH = "/home/{username}/.funcx-endpoint-persist-nesi.sh"
NESI_PERSIST_FUNCTIONS_PATH = "/home/{username}/.funcx-endpoint-persist-nesi-functions.sh"
NESI_PERSIST_LOG_PATH = "/home/{username}/.funcx-endpoint-persist-nesi.log"
NESI_STORAGE_DB_PATH = "/home/{username}/.globus_compute/storage.db"
SCRON_SECTION_START = "# BEGIN RJM AUTOMATICALLY ADDED SECTION"
SCRON_SECTION_END = "# END RJM AUTOMATICALLY ADDED SECTION"
ENDPOINT_CONFIG = """display_name: null
engine:
  type: GlobusComputeEngine
  max_retries_on_system_failure: 2
  max_workers_per_node: 8
  provider:
    type: LocalProvider
    init_blocks: 1
    max_blocks: 1
    min_blocks: 1
"""


class NeSISetup:
    """
    Runs setup steps specific to NeSI:

    - open SSH connection to Mahuika login node
    - configure funcx endpoint (TODO: how to do auth)
    - start default funcx endpoint
    - install scrontab entry to persist default endpoint (TODO: also, restart if newer version of endpoint?)

    """
    def __init__(self, username, account):
        self._username = username
        self._account = account
        self._num_handler_requests = 0
        self._client = None
        self._sftp = None
        self._lander_client = None

        # initialise values we are setting up
        self._funcx_id = None  # funcX endpoint id
        self._globus_id = None  # globus endpoint id
        self._globus_path = None  # path to globus share

        # funcx file locations
        self._funcx_config_file = f"/home/{self._username}/.funcx/{FUNCX_ENDPOINT_NAME}/config.py"
        self._globus_compute_endpoint_dir = f"/home/{self._username}/.globus_compute/{FUNCX_ENDPOINT_NAME}"
        self._globus_compute_config_file = f"/home/{self._username}/.globus_compute/{FUNCX_ENDPOINT_NAME}/config.py"
        self._globus_compute_config_file_new = f"/home/{self._username}/.globus_compute/{FUNCX_ENDPOINT_NAME}/config.yaml"

        # functions file path
        self._script_path = NESI_PERSIST_SCRIPT_PATH.format(username=self._username)
        self._functions_path = NESI_PERSIST_FUNCTIONS_PATH.format(username=self._username)
        self._persist_log_path = NESI_PERSIST_LOG_PATH.format(username=self._username)
        self._storage_db_path = NESI_STORAGE_DB_PATH.format(username=self._username)

        self._connect()

    def get_funcx_config(self):
        """Return funcx config values"""
        return self._funcx_id

    def get_globus_config(self):
        return self._globus_id, self._globus_path

    def _connect(self):
        # create SSH client for lander
        print("Connecting to NeSI, please wait...")
        logger.info(f"Connecting to {LOGIN_NODE} via {GATEWAY} as {self._username}")
        self._lander_client = paramiko.SSHClient()
        self._lander_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)

        # try to connect to setup transport etc
        try:
            self._lander_client.connect(GATEWAY, username=self._username)
        except paramiko.ssh_exception.SSHException:
            pass
        else:
            # expected to fail with 2-factor
            raise RuntimeError("Expected initial connection attempt to fail but it did not")

        # now get the transport and run auth_interactive
        try:
            self._lander_client.get_transport().auth_interactive(username=self._username, handler=self._auth_handler)
        except paramiko.ssh_exception.AuthenticationException as exc:
            logger.error(repr(exc))
            sys.stderr.write("Authentication error: please check your NeSI credentials and try again!\n")
            sys.exit(1)

        # run command
        stdin, stdout, stderr = self._lander_client.exec_command("echo $HOSTNAME")
        logger.debug(f"Hostname after first connect is {stdout.read().strip().decode('utf-8')}")

        # open channel to lander
        logger.debug("Opening tunnel from lander to login node")
        local_addr = (GATEWAY, 22)
        dest_addr = (LOGIN_NODE, 22)
        self._proxy = self._lander_client.get_transport().open_channel('direct-tcpip', dest_addr, local_addr)

        # connect to login node
        logger.debug("Connecting to login node through tunnel")
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        self._client.connect(GATEWAY, username=self._username, sock=self._proxy)

        # create an sftp client too
        self._sftp = self._client.open_sftp()

        # test run command
        status, stdout, stderr = self.run_command("echo $HOSTNAME", profile=False)
        logger.info(f"Successfully opened connection to '{stdout}'")

    def __del__(self):
        # close connection
        if self._client is not None:
            self._client.close()
        if self._sftp is not None:
            self._sftp.close()
        if self._lander_client is not None:
            self._lander_client.close()

    def _auth_handler(self, title, instructions, prompt_list):
        """auth handler that authenticates with NeSI"""
        self._num_handler_requests += 1
        logger.debug(f"Entering auth_handler (count = {self._num_handler_requests})")
        logger.debug(f"auth_handler called with:")
        logger.debug(f"  title: {title}")
        logger.debug(f"  instructions: {instructions}")
        logger.debug(f"  prompt_list: {prompt_list}")

        # fall back to interactive
        if len(title.strip()):
            print(title.strip())
        if len(instructions.strip()):
            print(instructions.strip())
        return_val = [echo and input(prompt) or getpass.getpass(os.linesep + prompt) for (prompt, echo) in prompt_list]

        logger.debug(f'Returning from auth handler: {return_val}')

        return return_val

    def run_command(self, command, input_text=None, profile=True):
        """
        Execute command on NeSI and return stdout and stderr.

        Ensure /etc/profile is sourced prior to running the command.

        If input_text is specified, write that to stdin

        If profile is specified, source /etc/profile before running the command

        """
        if profile:
            full_command = f"source /etc/profile && {command}"
        else:
            full_command = command
        logger.debug(f"Running command: '{full_command}'")
        stdin, stdout, stderr = self._client.exec_command(full_command)

        if input_text is not None:
            stdin.write(input_text)
            stdin.flush()
            stdin.channel.shutdown_write()  # send EOF

        output = stdout.read().decode('utf-8').strip()
        error = stderr.read().decode('utf-8').strip()
        status = stdout.channel.recv_exit_status()

        return status, output, error

    def setup_globus(self):
        """
        Sets up globus:

        1. Create a directory for the guest collection
        2. Create a guest collection to share the directory
        3. Report back the endpoint id and url for managing the new guest collection

        """
        print("Setting up Globus, please wait...")

        # select directory for sharing
        guest_collection_dir = f"/nesi/nobackup/{self._account}/{self._username}/rjm-jobs"
        print("="*120)
        print(f"Creating Globus guest collection at:\n    {guest_collection_dir}")
        response = input("Press enter to accept the above location or specify an alternative here: ").strip()
        if len(response):
            if response.startswith("/nesi/nobackup"):
                guest_collection_dir = response
            else:
                raise ValueError("Valid guest collection directories must start with '/nesi/nobackup'")
        logger.info(f"Guest collection directory: {guest_collection_dir}")
        print("Continuing, please wait...")

        # create the directory if it doesn't exist
        if self._remote_path_exists(guest_collection_dir):
            # confirm have write access to the directory
            write_access = self._remote_path_writeable(guest_collection_dir)
            logger.debug(f"Testing for write access: {write_access}")
            if not write_access:
                raise ValueError(f"User does not have write access to: {guest_collection_dir}")
            logger.debug("Guest collection directory already exists")
        else:
            logger.debug("Creating guest collection directory...")
            self._remote_dir_create(guest_collection_dir)

        # request scope for creating collection
        endpoint_scope = GCSClient.get_gcs_endpoint_scopes(GLOBUS_NESI_ENDPOINT).manage_collections
        collection_scope = GCSClient.get_gcs_collection_scopes(GLOBUS_NESI_COLLECTION).data_access
        required_scope = f"{endpoint_scope}[*{collection_scope}]"
        logger.debug(f"Requesting scope: {required_scope}")
        with tempfile.TemporaryDirectory() as tmpdir:
            print("="*120)
            print("Authorising Globus - this should open a browser where you need to authenticate with Globus and approve access")
            print("                     Globus is used by RJM to transfer files to and from NeSI")
            print("")
            print("NOTE: If you are asked for a linked identity with the NeSI Wellington OIDC Server please do one of the following:")
            print(f"      - If you already have a linked identity it should appear in the list like: '{self._username}@wlg-dtn-oidc.nesi.org.nz'")
            print("        If so, please select it and follow the instructions to authenticate with your NeSI credentials")
            print("      - Otherwise, choose the option to 'Link an identity from NeSI Wellington OIDC Server'")
            print("")
            print("="*120)

            # globus auth
            tmp_token_file = os.path.join(tmpdir, "tokens.json")
            globus_cli = utils.handle_globus_auth(
                [required_scope],
                token_file=tmp_token_file,
            )
            authorisers = globus_cli.get_authorizers_by_scope(endpoint_scope)

            # make certain they can view files on NeSI
            print("="*120)
            print("Before proceeding, please open this link in a browser and, if required, authenticate")
            print("with the NeSI Wellington OIDC Server (see instructions above):")
            print()
            print(f"    https://app.globus.org/file-manager?origin_id={GLOBUS_NESI_COLLECTION}")
            print("")
            print("Please confirm you can see your files on NeSI via the above link before continuing")
            print("")
            input("Once you can access your NeSI files at the above link, press enter to continue... ")
            print()
            print("Continuing, please wait...")

            # GCS client
            client = GCSClient(GLOBUS_NESI_GCS_ADDRESS, authorizer=authorisers[endpoint_scope])

            # user credentials
            cred = client.get("/user_credentials")
            assert cred["code"] == "success", "Error getting user_credentials"
            try:
                user_cred = cred["data"][0]
            except IndexError as exc:
                logger.error("Error retrieving user credentials")
                print("user_credentials:")
                print(cred)
                raise exc

            # collection document specifying options for new guest collection
            doc = GuestCollectionDocument(
                mapped_collection_id=GLOBUS_NESI_COLLECTION,
                user_credential_id=user_cred["id"],
                collection_base_path=guest_collection_dir,
                display_name="RJM_jobs",
                public=False,
                enable_https=True,
            )
            logger.debug(f"Collection document: {doc}")

            # create Globus collection, report back endpoint id for config
            response = client.create_collection(doc)
            endpoint_id = response.data["id"]
            logger.debug(f"Created Globus Guest Collection with Endpoint ID: {endpoint_id}")

        # report endpoint id for configuring rjm
        print("="*120)
        print(f"Globus guest collection endpoint id: '{endpoint_id}'")
        print(f"Globus guest collection endpoint path: '{guest_collection_dir}'")
        print("You can manage the endpoint you just created online at:")
        print(f"    https://app.globus.org/file-manager/collections/{endpoint_id}/overview")
        print("="*120)

        # also store the endpoint id and path
        self._globus_id = endpoint_id
        self._globus_path = guest_collection_dir

    def _check_home_permissions(self):
        """
        Check home directory permissions - there was a problem with the provisioner
        that resulted in some homes having group write permission which results in
        passwordless ssh between login nodes not working

        """
        print("Checking for suitable home directory permissions on NeSI, please wait...")

        s = self._sftp.stat(f"/home/{self._username}")
        logger.debug(f"Home directory stat: {s}")
        group_write = bool(s.st_mode & stat.S_IWGRP)
        logger.debug(f"Group write permission on home (should be False): {group_write}")

        if group_write:
            print("WARNING: your home directory on NeSI has group write permissions")
            print("         this was probably a mistake when your account was set up")
            proceed = input('Enter "yes" to fix this now (it should not cause any issues): ').strip() == "yes"
            if not proceed:
                sys.exit("Cannot proceed with bad home directory permissions")

            # remove group write from home directory
            cmd = f"chmod g-w /home/{self._username}"
            print(f'Fixing home directory permissions: "{cmd}"')
            status, output, stderr = self.run_command(cmd, profile=False)
            if status:
                raise RuntimeError(f"Failed to fix home directory permissions:\n\n{output}\n\n{stderr}")

    def setup_globus_compute(self, restart=True, reauthenticate=False):
        """
        Sets up the Globus Compute endpoint on NeSI.

        If restart is True, then restart the endpoint if it is already running

        """
        print("Setting up Globus Compute, please wait...")

        # check home directory permissions - there was a problem with the provisioner
        #  that resulted in some homes having group write permission which results in
        #  passwordless ssh between login nodes not working
        self._check_home_permissions()

        # remove existing scrontab to avoid interference
        self._remove_funcx_scrontab()

        # upload bash scripts
        self._upload_funcx_scripts()

        # configure funcx endpoint
        if not self.is_funcx_endpoint_configured():
            logger.info(f"Configuring Globus Compute '{FUNCX_ENDPOINT_NAME}' endpoint")
            print("Configuring Globus Compute, please wait...")

            # make sure the config directory exists
            logger.debug(f"Making sure endpoint config dir exists: {self._globus_compute_endpoint_dir}")
            self._remote_dir_create(self._globus_compute_endpoint_dir)

            # if it exists, delete the old config file
            logger.debug("Deleting old config if it exists")
            status, output, error = self.run_command(f"rm -f {self._globus_compute_config_file}", profile=False)
            if status:
                raise RuntimeError("Failed to delete old config if it exists")

            # write the new config file
            logger.debug(f"Writing endpoint config file to: {self._globus_compute_config_file_new}")
            with self._sftp.file(self._globus_compute_config_file_new, 'w') as fh:
                fh.write(ENDPOINT_CONFIG)

            assert self.is_funcx_endpoint_configured(), "funcX endpoint configuration failed"
            logger.info("Globus Compute endpoint configuration complete")

            restart = True

        # reauthenticate
        if reauthenticate or not self.is_globus_compute_endpoint_authenticated():
            self._globus_compute_endpoint_authentication()

        # run the bash script that will ensure one endpoint is running on NeSI
        if restart:
            print("Restarting the Globus Compute endpoint, please wait...")
        else:
            print("Ensuring the Globus Compute endpoint is running, please wait...")
        cmd = f"export ENDPOINT_RESTART={'1' if restart else '0'} && {self._script_path}"
        logger.debug(f'Running Globus Compute script: "{cmd}"')
        status, stdout, stderr = self.run_command(cmd)
        assert status == 0, f"Running Globus Compute script failed: {stdout} {stderr}"
        logger.debug(f"Stdout:\n{stdout}")

        # get the endpoint id
        cmd = f"source {self._functions_path} && get_endpoint_id && echo ${{ENDPOINT_ID}}"
        logger.debug(f"Getting endpoint id: '{cmd}'")
        status, stdout, stderr = self.run_command(cmd)
        assert status == 0, f"Getting endpoint id failed: {stdout} {stderr}"
        endpoint_id = stdout.strip()
        logger.debug(f"Got endpoint id: '{endpoint_id}'")

        # report endpoint id for configuring rjm
        print("="*120)
        print(f"Globus Compute endpoint is running and has id: '{endpoint_id}'")
        print("="*120)

        # store endpoint id
        self._funcx_id = endpoint_id

        # install scrontab if not already installed
        self._setup_funcx_scrontab()
        logger.info("Installed scrontab entry to ensure Globus Compute endpoint keeps running (run 'scrontab -l' on mahuika to view)")
        print("A scrontab entry has been added to periodically check the status of the Globus Compute endpoint and restart it if needed")
        print("On mahuika, run 'scrontab -l' to view it")
        print("You may also notice two Slurm jobs have been created with names 'funcxcheck' and 'funcxrestart', please do not cancel them!")
        print("="*120)

    def _globus_compute_endpoint_authentication(self):
        """
        Set up authentication for the Globus Compute endpoint

        Use the LoginManager from GlobusComputeSDK to create a 'storage.db' file
        locally, which we then copy to the remote machine. Work in a temp dir locally.

        """
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            # set environment variable so that globus compute writes tokens to tmp dir
            logger.debug(f"Setting GLOBUS_COMPUTE_USER_DIR={tmpdir}")
            os.environ["GLOBUS_COMPUTE_USER_DIR"] = tmpdir

            try:
                print("Authenticating Globus Compute Endpoint, please follow the instructions...")

                # run the login flow
                login_manager = GlobusComputeLoginManager()
                login_manager.run_login_flow()

                # check the file was created
                auth_file = os.path.join(tmpdir, "storage.db")
                if not os.path.exists(auth_file):
                    print(f"ERROR NO FILE: {auth_file}")
                    time.sleep(300)
                    raise RuntimeError("Globus compute authentication failed, no 'storage.db' file found")

                # copy the file onto NeSI
                logger.debug(f"Uploading new credentials file to NeSI: {self._storage_db_path}")
                self._upload_file(auth_file, self._storage_db_path, text=False)

            finally:
                # revert environment
                del os.environ["GLOBUS_COMPUTE_USER_DIR"]

    def _retrieve_current_scrontab(self):
        """
        Retrieve the current scrontab

        """
        status, stdout, stderr = self.run_command('scrontab -l')
        if status != 0:
            if "no crontab for" in stdout or "no crontab for" in stderr:
                # blank scrontab
                current_scrontab = ""
            else:
                raise RuntimeError(f"Failed to retrieve current scrontab contents: '{stdout}' '{stderr}'")
        else:
            current_scrontab = stdout

        return current_scrontab

    def _remove_funcx_scrontab(self):
        """
        Remove existing funcx scrontab entry

        """
        print("Removing existing scrontab to avoid interference, please wait...")

        current_scrontab = self._retrieve_current_scrontab()

        # remove rjm section from current scrontab, if any
        new_scrontab_lines = []
        in_rjm_section = False
        modified = False
        for line in current_scrontab.splitlines():
            if SCRON_SECTION_START in line:
                in_rjm_section = True
                logger.debug("Found beginning of rjm section in existing scrontab")
                modified = True
            elif in_rjm_section and SCRON_SECTION_END in line:
                in_rjm_section = False
                logger.debug("Found end of rjm section in existing scrontab")
                modified = True
            elif in_rjm_section:
                logger.debug(f"Removing current rjm section ({line})")
                modified = True
            else:
                new_scrontab_lines.append(line)

        # write the new scrontab if changed
        if modified:
            new_scrontab_text = "\n".join(new_scrontab_lines)
            self._write_new_scrontab(new_scrontab_text)

    def _write_new_scrontab(self, scrontab_text):
        """
        Write the text to the scrontab

        """
        logger.debug(f"New scrontab content follows:\n{scrontab_text}")
        status, stdout, stderr = self.run_command("scrontab -", input_text=scrontab_text)
        assert status == 0, f"Setting scrontab failed: {stdout} {stderr}"

    def _upload_file(self, local_path, remote_path, executable=False, text=True):
        """
        Upload the given file, optionally making it executable

        """
        logger.debug(f"Uploading file '{local_path}' to '{remote_path}'")
        self._sftp.put(local_path, remote_path)

        if executable:
            # make sure the script is executable
            status, stdout, stderr = self.run_command(f"chmod +x {remote_path}", profile=False)
            assert status == 0, f"Failed to make file executable: {stdout} {stderr}"

        if text:
            # make sure it has unix line endings (if it is a text file)
            status, stdout, stderr = self.run_command(f"dos2unix {remote_path}", profile=False)
            assert status == 0, f"Failed to convert convert file to unix format: {stdout} {stderr}"

    def _upload_funcx_scripts(self):
        """
        Upload the bash script and functions for interacting with funcx

        """
        print("Uploading funcx scripts, please wait...")

        # write script to NeSI
        with importlib.resources.path('rjm.setup', 'funcx-endpoint-persist-nesi.sh') as p:
            # upload the script to NeSI
            script_path = self._script_path
            assert os.path.exists(p), "Problem finding shell script resource ({p})"
            self._upload_file(p, script_path, executable=True)
        assert self._remote_path_exists(script_path), f"Failed to upload persist script: '{script_path}'"
        logger.debug(f"Uploaded file to: {script_path}")

        # write functions file to NeSI
        with importlib.resources.path('rjm.setup', 'funcx-endpoint-persist-nesi-functions.sh') as p:
            # upload the script to NeSI
            script_path = self._functions_path
            assert os.path.exists(p), "Problem finding shell script resource ({p})"
            self._upload_file(p, script_path, executable=False)
        assert self._remote_path_exists(script_path), f"Failed to upload persist script: '{script_path}'"
        logger.debug(f"Uploaded file to: {script_path}")

    def _setup_funcx_scrontab(self):
        """
        Create a scrontab job for keeping funcx endpoint running

        """
        print("Setting up funcX scrontab entry, please wait...")

        # retrieve current scrontab
        current_scrontab = self._retrieve_current_scrontab()

        # for storing new scrontab
        scrontab_lines = current_scrontab.splitlines()

        # add new rjm section (assume existing section was already removed)
        if len(scrontab_lines) > 0 and len(scrontab_lines[-1].strip()) > 0:
            scrontab_lines.append("")  # insert space if there were lines before
        scrontab_lines.append(SCRON_SECTION_START)
        scrontab_lines.append("#SCRON --time=05:00")
        scrontab_lines.append("#SCRON --job-name=funcxpersist")
        scrontab_lines.append(f"#SCRON --account={self._account}")
        scrontab_lines.append("#SCRON --mem=128")
        scrontab_lines.append(f"30 0-14,16-23 * * * {self._script_path} >> {self._persist_log_path} 2>&1")  # times are in UTC
        scrontab_lines.append("")
        scrontab_lines.append("#SCRON --time=05:00")
        scrontab_lines.append("#SCRON --job-name=funcxrestart")
        scrontab_lines.append(f"#SCRON --account={self._account}")
        scrontab_lines.append("#SCRON --mem=128")
        scrontab_lines.append(f"30 15 * * * env ENDPOINT_RESTART=1 {self._script_path} >> {self._persist_log_path} 2>&1")  # times are in UTC
        scrontab_lines.append(SCRON_SECTION_END)
        scrontab_lines.append("")  # end with a newline

        # install new scrontab
        new_scrontab_text = "\n".join(scrontab_lines)
        self._write_new_scrontab(new_scrontab_text)

    def _remote_dir_create(self, path):
        """Create directory at the given path"""
        status, stdout, stderr = self.run_command(f'mkdir -p "{path}"', profile=False)
        assert status == 0, f"Creating '{path}' failed: {stdout} {stderr}"
        if not self._remote_path_exists(path):
            raise RuntimeError(f"Creating '{path}' failed: ({stdout}) ({stderr})")

    def _remote_path_writeable(self, path):
        """Return True if the path is writeable, otherwise False"""
        status, _, _ = self.run_command(f'test -w "{path}"', profile=False)

        return not status

    def _remote_path_exists(self, path):
        """Return True if the path exists on the remote, otherwise False"""
        try:
            self._sftp.stat(path)
            exists = True
        except FileNotFoundError:
            exists = False

        return exists

    def get_funcx_status(self):
        """Checks whether funcx is authorised and the default endpoint is configured and running"""
        # assume it is configured if the credentials and default endpoint files exist
        # or run status and see if we can tell from that
        # first test if funcx

    def is_globus_compute_endpoint_authenticated(self):
        """
        Check whether the globus compute endpoint is authenticated

        """
        print("Checking if Globus Compute endpoint is authenticated, please wait...")

        command = f"module load {FUNCX_MODULE} && globus-compute-endpoint whoami"
        status, output, error = self.run_command(command)
        # failure should look like: "Error: Unable to retrieve user information. Please log in again."
        if status or "Error" in output or "Unable to retrieve user information" in output:
            logger.debug(f"Globus compute endpoint is not authenticated, output follows:\n{output}\n{error}\n")
            authenticated = False
        else:
            logger.debug(f"Globus compute endpoint is authenticated:\n{output}")
            authenticated = True

        return authenticated

    def string_in_remote_file(self, file_path, string_match):
        """Return `True` if `string_match` exists in remote file `file_path`"""
        command = f'grep "{string_match}" "{file_path}"'
        status, output, error = self.run_command(command, profile=False)
        if status:
            match = False
        else:
            match = True

        return match

    def is_funcx_endpoint_configured(self):
        """
        Check whether the funcx default endpoint is configured already

        """
        # test if default endpoint config exists, if so, we assume it is configured
        if self._remote_path_exists(self._globus_compute_config_file_new):
            # test if the config refers to the deprecated HighThroughputEngine
            if self.string_in_remote_file(self._globus_compute_config_file_new, "HighThroughputEngine"):
                logger.debug("Globus Compute endpoint configuration is out of date (refers to HighThroughputEngine)")
                configured = False
            else:
                logger.debug("Assuming Globus Compute endpoint is configured as config file exists")
                configured = True
        else:
            logger.debug(f"Globus Compute config file does not exist: {self._globus_compute_config_file_new}")
            configured = False

        return configured
