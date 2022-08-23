
import os
import sys
import time
import logging
import getpass
import tempfile
import importlib.resources

import paramiko
from globus_sdk import GCSClient
from globus_sdk.services.gcs.data import GuestCollectionDocument

from rjm import utils


logger = logging.getLogger(__name__)

GATEWAY = "lander.nesi.org.nz"
LOGIN_NODE = "login.mahuika.nesi.org.nz"
FUNCX_NODES = [
    "mahuika01",
    "mahuika02",
]
FUNCX_MODULE = "funcx-endpoint/1.0.2-gimkl-2020a-Python-3.9.9"
FUNCX_ENDPOINT_NAME = "default"
GLOBUS_NESI_COLLECTION = 'cc45cfe3-21ae-4e31-bad4-5b3e7d6a2ca1'
GLOBUS_NESI_ENDPOINT = '90b0521d-ebf8-4743-a492-b07176fe103f'
GLOBUS_NESI_GCS_ADDRESS = "c61f4.bd7c.data.globus.org"
NESI_PERSIST_SCRIPT_PATH = "/home/{username}/.funcx-endpoint-persist-nesi.sh"
SCRON_SECTION_START = "# BEGIN RJM AUTOMATICALLY ADDED SECTION"
SCRON_SECTION_END = "# END RJM AUTOMATICALLY ADDED SECTION"


class NeSISetup:
    """
    Runs setup steps specific to NeSI:

    - open SSH connection to Mahuika login node
    - configure funcx endpoint (TODO: how to do auth)
    - start default funcx endpoint
    - install scrontab entry to persist default endpoint (TODO: also, restart if newer version of endpoint?)

    """
    def __init__(self, username, password, token, account):
        self._username = username
        self._password = password
        self._token = token
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
        funcx_dir = f"/home/{self._username}/.funcx"
        self._funcx_cred_file = f"{funcx_dir}/credentials/funcx_sdk_tokens.json"
        self._funcx_default_config = f"{funcx_dir}/default/config.py"

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
            sys.stderr.write("Authentication error: please check your NeSI credentials and try again!" + os.linesep)
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
        self._client.connect(GATEWAY, username=self._username, sock=self._proxy, password=self._password)

        # create an sftp client too
        self._sftp = self._client.open_sftp()

        # test run command
        status, stdout, stderr = self.run_command("echo $HOSTNAME")
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
        """auth handler that returns first and second factors for NeSI"""
        self._num_handler_requests += 1
        logger.debug(f"auth_handler called with: {prompt_list}")

        if self._num_handler_requests == 1:
            return_val = [self._password]  # first prompt asks for password
            logger.debug("returning first factor")

        elif self._num_handler_requests == 2:
            return_val = [self._token]  # second asks for token
            logger.debug("returning second factor")

        else:
            # fall back to interactive
            if len(title.strip()):
                print(title.strip())
            if len(instructions.strip()):
                print(instructions.strip())
            return_val = [echo and input(prompt) or getpass.getpass(prompt) for (prompt, echo) in prompt_list]

        return return_val

    def _run_command_handle_funcx_authentication(self, command):
        """
        Execute funcx command on NeSI

        Try to detect if funcx requests authentication before executing the command

        If so, request the code from the user and then continue

        Ensure /etc/profile is sourced prior to running the command.

        """
        full_command = f"source /etc/profile && {command}"
        logger.debug(f"Running command: '{full_command}'")
        stdin, stdout, stderr = self._client.exec_command(full_command)

        # wait until stdout channel has data ready
        while not stdout.channel.recv_ready():
            # check if the process exited without any output
            if stdout.channel.exit_status_ready():
                logger.debug("Breaking wait loop due to exit status ready")
                break
            else:
                time.sleep(5)
                logger.debug("Waiting for remote process")

        # set a timeout on the channel
        stdout.channel.settimeout(30)

        # receive any output
        logger.debug(f"Receiving stdout: {stdout.channel.recv_ready()}")
        output = ""
        while stdout.channel.recv_ready():
            output += stdout.channel.recv(1024).decode('utf-8')
            logger.debug(f"Received stdout:\n{output}\nEnd of received stdout")
            logger.debug(f"More to receive: {stdout.channel.recv_ready()}")

        # handling funcx authentication here...
        if "https://auth.globus.org/v2/oauth2/authorize" in output:
            # need to do auth
            print("="*120)
            print("Follow these instructions to authenticate funcX on NeSI:")
            print(output.strip())
            auth_code = input().strip()

            # send auth code
            logger.debug("Sending auth code to remote")
            stdin.write(auth_code)
            stdin.flush()
            stdin.channel.shutdown_write()  # send EOF

            print("="*120)
            print("Continuing with funcX setup, please wait...")

        # wait for process to complete
        logger.debug("Reading remaining stdout")
        output = (output + stdout.read().decode('utf-8')).strip()
        logger.debug("Reading stderr")
        error = stderr.read().decode('utf-8').strip()
        logger.debug("Waiting for process to finish")
        status = stdout.channel.recv_exit_status()
        logger.debug("Returning")

        return status, output, error

    def run_command(self, command, input_text=None):
        """
        Execute command on NeSI and return stdout and stderr.

        Ensure /etc/profile is sourced prior to running the command.

        If input_text is specified, write that to stdin

        """
        full_command = f"source /etc/profile && {command}"
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

    def setup_funcx(self, restart=False):
        """
        Sets up the funcX endpoint on NeSI.

        If restart is True, then restart the endpoint if it is already running

        """
        print("Setting up funcX, please wait...")

        # remove existing scrontab to avoid interference
        self._remove_funcx_scrontab()

        # configure funcx endpoint
        if not self.is_funcx_endpoint_configured():
            logger.info(f"Configuring funcX '{FUNCX_ENDPOINT_NAME}' endpoint")
            print("Configuring funcX, please wait...")

            command = f"module load {FUNCX_MODULE} && funcx-endpoint configure {FUNCX_ENDPOINT_NAME}"
            status, stdout, stderr = self._run_command_handle_funcx_authentication(command)
            assert status == 0, f"Configuring endpoint failed: {stdout} {stderr}"

            assert self.is_funcx_endpoint_configured(), "funcX endpoint configuration failed"
            logger.info("funcX endpoint configuration complete")

        # make sure the worker_logs directory exists
        worker_logs_dir = f"/home/{self._username}/.funcx/{FUNCX_ENDPOINT_NAME}/HighThroughputExecutor/worker_logs"
        logger.debug(f"Making sure worker_logs dir exists: {worker_logs_dir}")
        self._remote_dir_create(worker_logs_dir)

        # start the funcX endpoint
        endpoint_running, endpoint_id = self.is_funcx_endpoint_running(stop=restart)
        if not endpoint_running:
            logger.info(f"Starting funcx '{FUNCX_ENDPOINT_NAME}' endpoint")
            print("Starting funcX endpoint, please wait...")

            command = f"module load {FUNCX_MODULE} && funcx-endpoint start {FUNCX_ENDPOINT_NAME}"
            status, stdout, stderr = self._run_command_handle_funcx_authentication(command)
            assert status == 0, f"Starting endpoint failed: {stdout} {stderr}"
            endpoint_running, endpoint_id = self.is_funcx_endpoint_running()
            assert endpoint_running, f'Starting funcX endpoint failed: {stdout} {stderr}'

        # report endpoint id for configuring rjm
        print("="*120)
        print(f"funcX endpoint is running and has id: '{endpoint_id}'")
        print("="*120)

        # store endpoint id
        self._funcx_id = endpoint_id

        # install scrontab if not already installed
        self._setup_funcx_scrontab()
        logger.info("Installed scrontab entry to ensure funcx endpoint keeps running (run 'scrontab -l' on mahuika to view)")
        print("A scrontab entry has been added to periodically check the status of the funcx endpoint and restart it if needed")
        print("On mahuika, run 'scrontab -l' to view it")
        print("You may also notice a Slurm job has been created with name 'funcxcheck', please do not cancel it!")
        print("="*120)

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
        print("Removing existing scrontab to avoid interferences, please wait...")

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

    def _setup_funcx_scrontab(self):
        """
        Create a scrontab job for keeping funcx endpoint running

        """
        print("Setting up funcX scrontab entry, please wait...")

        # write script to NeSI
        with importlib.resources.path('rjm.setup', 'funcx-endpoint-persist-nesi.sh') as p:
            # upload the script to NeSI
            script_path = NESI_PERSIST_SCRIPT_PATH.format(username=self._username)
            assert os.path.exists(p), "Problem finding shell script resource ({p})"
            logger.debug(f"Uploading persist script '{p}' to '{script_path}'")
            self._sftp.put(p, script_path)
        assert self._remote_path_exists(script_path), f"Failed to upload persist script: '{script_path}'"

        # make sure the script is executable
        status, stdout, stderr = self.run_command(f"chmod +x {script_path}")
        assert status == 0, f"Failed to make script executable: {stdout} {stderr}"

        # make sure it has unix line endings
        status, stdout, stderr = self.run_command(f"dos2unix {script_path}")
        assert status == 0, f"Failed to convert convert script to unix format: {stdout} {stderr}"

        # retrieve current scrontab
        current_scrontab = self._retrieve_current_scrontab()

        # for storing new scrontab
        scrontab_lines = current_scrontab.splitlines()

        # add new rjm section (assume existing section was already removed)
        if len(scrontab_lines) > 0 and len(scrontab_lines[-1].strip()) > 0:
            scrontab_lines.append("")  # insert space if there were lines before
        scrontab_lines.append(SCRON_SECTION_START)
        scrontab_lines.append("#SCRON --time=08:00")
        scrontab_lines.append("#SCRON --job-name=funcxcheck")
        scrontab_lines.append(f"#SCRON --account={self._account}")
        scrontab_lines.append("#SCRON --mem=128")
        scrontab_lines.append(f"@hourly {script_path}")
        scrontab_lines.append(SCRON_SECTION_END)
        scrontab_lines.append("")  # end with a newline

        # install new scrontab
        new_scrontab_text = "\n".join(scrontab_lines)
        self._write_new_scrontab(new_scrontab_text)

    def _remote_dir_create(self, path):
        """Create directory at the given path"""
        status, stdout, stderr = self.run_command(f'mkdir -p "{path}"')
        assert status == 0, f"Creating '{path}' failed: {stdout} {stderr}"
        if not self._remote_path_exists(path):
            raise RuntimeError(f"Creating '{path}' failed: ({stdout}) ({stderr})")

    def _remote_path_writeable(self, path):
        """Return True if the path is writeable, otherwise False"""
        status, _, _ = self.run_command(f'test -w "{path}"')

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

    def is_funcx_endpoint_configured(self):
        """
        Check whether the funcx default endpoint is configured already

        """
        # test if default endpoint config exists, if so, we assume it is configured
        if self._remote_path_exists(self._funcx_default_config):
            logger.debug("Assuming funcx default endpoint is configured as config file exists")
            configured = True
        else:
            logger.debug("funcX default endpoint config file does not exist")
            configured = False

        return configured

    def is_funcx_endpoint_running(self, stop=False):
        """
        Check whether the funcx default endpoint is running already

        If stop is True, then stop the endpoint if it is running

        """
        print("Checking if a funcX endpoint is running, please wait...")

        # test whether funcx endpoint is actually running
        # loop over nodes where funcx could be running
        funcx_running_nodes = []
        funcx_endpoint_id = None
        for node in FUNCX_NODES:
            command = f"ssh -oStrictHostKeyChecking=no {node} 'source /etc/profile && module load {FUNCX_MODULE} && funcx-endpoint list'"
            status, stdout, stderr = self._run_command_handle_funcx_authentication(command)
            assert status == 0, f"listing endpoints on '{node}' failed: {stdout} {stderr}"

            for line in stdout.splitlines():
                if FUNCX_ENDPOINT_NAME in line:
                    funcx_endpoint_id = line.split('|')[3].strip()
                    if "Running" in line:
                        funcx_running_nodes.append(node)
                        break

        # report which nodes the endpoint is running on
        if len(funcx_running_nodes) > 0:
            logger.debug(f"funcX '{FUNCX_ENDPOINT_NAME}' endpoint (id: {funcx_endpoint_id}) is running on: {funcx_running_nodes}")
        else:
            logger.debug("funcX endpoint is not running")

        # stop the endpoint if multiple are running or we're specfically asked to
        if len(funcx_running_nodes) > 1 or stop:
            if len(funcx_running_nodes) > 1:
                logger.warning(f'funcX endpoint running on multiple nodes -> attempting to stop them all: {funcx_running_nodes}')
            else:
                print("Stopping funcX endpoint for restart, please wait...")

            for node in funcx_running_nodes:
                command = f"ssh -oStrictHostKeyChecking=no {node} 'source /etc/profile && module load {FUNCX_MODULE} && funcx-endpoint stop {FUNCX_ENDPOINT_NAME}'"
                status, stdout, stderr = self._run_command_handle_funcx_authentication(command)
                if status:
                    raise RuntimeError(f"Failed to stop funcX endpoint on '{node}':\n\n{stdout}\n\n{stderr}")
            funcx_running_nodes = []

        return len(funcx_running_nodes) != 0, funcx_endpoint_id
