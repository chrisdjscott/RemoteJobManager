
import os
import logging
import getpass
import tempfile
import importlib.resources

import paramiko
from globus_sdk import GCSClient
from globus_sdk import GCSAPIError
from globus_sdk.services.gcs.data import GuestCollectionDocument
from funcx import FuncXClient

from rjm import utils
from rjm.runners.funcx_slurm_runner import FUNCX_SCOPE


logger = logging.getLogger(__name__)

GATEWAY = "lander.nesi.org.nz"
LOGIN_NODE = "login.mahuika.nesi.org.nz"
FUNCX_NODES = [
    "mahuika01",
    "mahuika02",
]
FUNCX_MODULE = "funcx-endpoint/0.3.6-gimkl-2020a-Python-3.9.9"
FUNCX_ENDPOINT_NAME = "default"
GLOBUS_NESI_COLLECTION = 'cc45cfe3-21ae-4e31-bad4-5b3e7d6a2ca1'
GLOBUS_NESI_ENDPOINT = '90b0521d-ebf8-4743-a492-b07176fe103f'
GLOBUS_NESI_GCS_ADDRESS = "c61f4.bd7c.data.globus.org"
NESI_PERSIST_SCRIPT_PATH = "/home/{username}/.funcx-endpoint-persist-nesi.sh"


class NeSISetup:
    """
    Runs setup steps specific to NeSI:

    - open SSH connection to Mahuika login node
    - configure funcx endpoint (TODO: how to do auth)
    - start default funcx endpoint
    - install scrontab entry to persist default endpoint (TODO: also, restart if newer version of endpoint?)

    """
    def __init__(self, username, password, token):
        self._username = username
        self._password = password
        self._token = token
        self._num_handler_requests = 0
        self._client = None
        self._sftp = None
        self._lander_client = None
        self._funcx_authorised = None
        self._funcx_configured = None
        self._funcx_running = None

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
        self._lander_client.get_transport().auth_interactive(username=self._username, handler=self._auth_handler)

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
        print("Setting up Globus...")

        # select directory for sharing
        print("Enter your NeSI project code below or press enter to select the default (note: you must be a member of the project)")
        account = input("Enter NeSI project code [uoa00106]: ").strip() or "uoa00106"
        guest_collection_dir = f"/nesi/nobackup/{account}/{self._username}/rjm-jobs"
        print("="*120)
        print(f"Creating Globus guest collection at:\n    {guest_collection_dir}")
        response = input("Press enter to accept the above location or specify an alternative here: ").strip()
        if len(response):
            if response.startswith("/nesi/nobackup"):
                guest_collection_dir = response
            else:
                raise ValueError("Valid guest collection directories must start with '/nesi/nobackup'")
        logger.info(f"Guest collection directory: {guest_collection_dir}")

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
            print("NOTE: You may be asked for a linked identity with the NeSI Wellington OIDC Server")
            print("      If you already have a linked identity it should appear in the list like: '{self._username}@wlg-dtn-oidc.nesi.org.nz'")
            print("      If so, please select it and follow the instructions to authenticate with your NeSI credentials")
            print("      Otherwise, choose the option to 'Link an identity from NeSI Wellington OIDC Server'")
            print("")
            print("="*120)

            # globus auth
            tmp_token_file = os.path.join(tmpdir, "tokens.json")
            globus_cli = utils.handle_globus_auth(
                [required_scope],
                token_file=tmp_token_file,
            )
            authorisers = globus_cli.get_authorizers_by_scope(endpoint_scope)
            print("Authentication done. Continuing...")

            # GCS client
            client = GCSClient(GLOBUS_NESI_GCS_ADDRESS, authorizer=authorisers[endpoint_scope])

            # user credentials
            cred = client.get("/user_credentials")
            assert cred["code"] == "success", "Error getting user_credentials"
            user_cred = cred["data"][0]

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
            try:
                response = client.create_collection(doc)
            except GCSAPIError as exc:
                # first attempt might result in authentication error if they haven't
                # authenticated with their NeSI credentials recently?
                logger.debug("Initial attempt to create collection failed with error {exc.http_status}. {exc.code}, {exc.message}")
                if exc.http_status == 403 and exc.code == "permission_denied" and exc.message.startswith("You must reauthenticate one of your identities"):
                    logger.warning(exc.message)

                    # ask them to login to the NeSI endpoint via Globus Web App, then try again
                    print("="*120)
                    print("You must activate the NeSI Globus Endpoint at the following URL, which should")
                    print("require entering your NeSI credentials:")
                    print(f"    https://app.globus.org/file-manager?origin_id={GLOBUS_NESI_COLLECTION}")
                    print("")
                    print("NOTE: Please confirm you can see your files on NeSI via the above link before continuing")
                    print("")
                    input("Once you can access your NeSI files at the above link, press enter to continue... ")
                    # TODO: add link to some documentation...

                    # now try to create the collection again
                    response = client.create_collection(doc)

                else:
                    # otherwise raise the original error
                    raise exc

            endpoint_id = response.data["id"]
            logger.debug(f"Created Globus Guest Collection with Endpoint ID: {endpoint_id}")

        # report endpoint id for configuring rjm
        print("="*120)
        print(f"Globus guest collection endpoint id: '{endpoint_id}'")
        print(f"Globus guest collection endpoint path: '{guest_collection_dir}'")
        print("The above values will be required when configuring RJM")
        print(f"You can manage the endpoint online at: https://app.globus.org/file-manager/collections/{endpoint_id}/overview")
        print("="*120)

        # also store the endpoint id and path
        self._globus_id = endpoint_id
        self._globus_path = guest_collection_dir

    def setup_funcx(self):
        """
        Sets up funcx:

        1. Check if funcx is configured, if not configure it (default endpoint)
           - TODO: handle auth centrally and copy tokens across, if possible
        2. Check if funcx endpoint is running, if not start it
           - TODO: option to restart it (or maybe make that the default)

        1. Check if funcx is authorised
        2. If not, get required scopes including globus
        3. ...

        """
        print("Setting up funcX...")

        # make sure funcx is authorised
        if not self.is_funcx_authorised():
            logger.info("Authorising funcX")
            print("="*120)
            print("Authorising funcX - this should open a browser where you need to authenticate with Globus and approve access")
            print("="*120)

            required_scopes = [
                utils.OPENID_SCOPE,
                utils.SEARCH_SCOPE,
                FUNCX_SCOPE,
            ]

            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_token_file = os.path.join(tmpdir, "tokens.json")
                logger.debug(f"Requesting funcX tokens and storing in tmpfile: {tmp_token_file}")
                logger.debug(f"Using funcx client id: {FuncXClient.FUNCX_SDK_CLIENT_ID}")
                utils.handle_globus_auth(
                    required_scopes,
                    token_file=tmp_token_file,
                    client_id=FuncXClient.FUNCX_SDK_CLIENT_ID,  # using their client id so refreshing works
                    name="RJM on behalf of FuncX Endpoint",
                )
                assert os.path.exists(tmp_token_file)
                print("Authentication done. Continuing...")

                # upload funcx tokens to correct place if needed
                logger.debug("Transferring token file to NeSI")

                # create the credentials directory
                self._remote_dir_create(f'/home/{self._username}/.funcx/credentials')

                # transfer the token file we just created
                self._sftp.put(tmp_token_file, self._funcx_cred_file)

            assert self.is_funcx_authorised(), "funcX authorisation failed"
            logger.info("funcX authorisation complete")

        # configure funcx endpoint
        if not self.is_funcx_endpoint_configured():
            logger.info(f"Configuring funcX '{FUNCX_ENDPOINT_NAME}' endpoint")

            status, stdout, stderr = self.run_command(f"module load {FUNCX_MODULE} && funcx-endpoint configure {FUNCX_ENDPOINT_NAME}")
            assert status == 0, f"Configuring endpoint failed: {stdout} {stderr}"

            assert self.is_funcx_endpoint_configured(), "funcX endpoint configuration failed"
            logger.info("funcX endpoint configuration complete")

        # start the funcX endpoint
        endpoint_running, endpoint_id = self.is_funcx_endpoint_running()
        if not endpoint_running:
            logger.info(f"Starting funcx '{FUNCX_ENDPOINT_NAME}' endpoint")
            status, stdout, stderr = self.run_command(f"module load {FUNCX_MODULE} && funcx-endpoint start {FUNCX_ENDPOINT_NAME}")
            assert status == 0, f"Starting endpoint failed: {stdout} {stderr}"
            endpoint_running, endpoint_id = self.is_funcx_endpoint_running()
            assert endpoint_running, f'Starting funcX endpoint failed: {stdout} {stderr}'

        # report endpoint id for configuring rjm
        print("="*120)
        print(f"funcX endpoint is running and has id: '{endpoint_id}'")
        print("The above value will be required when configuring RJM")
        print("="*120)

        # store endpoint id
        self._funcx_id = endpoint_id

        # install scrontab if not already installed
        self._setup_funcx_scrontab()
        logger.info("Installed scrontab entry to ensure funcx endpoint keeps ruuning (run 'scrontab -l' on mahuika to view)")
        print("A scrontab entry has been added to periodically check the status of the funcx endpoint and restart it if needed")
        print("On mahuika, run 'scrontab -l' to view it")
        print("="*120)

    def _setup_funcx_scrontab(self):
        """
        Create a scrontab job for keeping funcx endpoint running

        """
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

        # retrieve current scrontab
        status, stdout, stderr = self.run_command('scrontab -l')
        if status != 0:
            if "no crontab for" in stdout or "no crontab for" in stderr:
                # blank scrontab
                current_scrontab = ""
            else:
                raise RuntimeError(f"Failed to retrieve current scrontab contents: '{stdout}' '{stderr}'")
        else:
            current_scrontab = stdout

        # for storing new scrontab
        new_scrontab_lines = []

        # remove rjm section from current scrontab, if any
        in_rjm_section = False
        rjm_section_start = "# BEGIN RJM AUTOMATICALLY ADDED SECTION"
        rjm_section_end = "# END RJM AUTOMATICALLY ADDED SECTION"
        for line in current_scrontab.splitlines():
            if rjm_section_start in line:
                in_rjm_section = True
                logger.debug("Found beginning of rjm section in existing scrontab")
            elif in_rjm_section and rjm_section_end in line:
                in_rjm_section = False
                logger.debug("Found end of rjm section in existing scrontab")
            elif in_rjm_section:
                logger.debug(f"Removing current rjm section ({line})")
            else:
                new_scrontab_lines.append(line)
                logger.debug(f"Keeping existing scrontab line ({line})")

        # add new rjm section
        if len(new_scrontab_lines) > 0 and len(new_scrontab_lines[-1].strip()) > 0:
            new_scrontab_lines.append("")  # insert space if there were lines before
        new_scrontab_lines.append(rjm_section_start)
        new_scrontab_lines.append("#SCRON -t 08:00")
        new_scrontab_lines.append("#SCRON -J funcxcheck")
        new_scrontab_lines.append("#SCRON --mem=128")
        new_scrontab_lines.append(f"@hourly {script_path}")
        new_scrontab_lines.append(rjm_section_end)
        new_scrontab_lines.append("")  # end with a newline

        # install new scrontab
        status, stdout, stderr = self.run_command("scrontab -", input_text="\n".join(new_scrontab_lines))
        assert status == 0, f"Setting scrontab failed: {stdout} {stderr}"

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

    def is_funcx_authorised(self):
        """
        Check whether funcx is authorised already

        """
        # test if credentials file exists, if so, we assume it is authorised
        if self._remote_path_exists(self._funcx_cred_file):
            logger.debug("Assuming funcx is authorised as credentials file exists")
            authorised = True
        else:
            logger.debug("funcX credentials file does not exist")
            authorised = False

        return authorised

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

    def is_funcx_endpoint_running(self):
        """
        Check whether the funcx default endpoint is running already

        """
        # test whether funcx endpoint is actually running
        if self.is_funcx_authorised():
            # loop over nodes where funcx could be running
            funcx_running_nodes = []
            funcx_endpoint_id = None
            for node in FUNCX_NODES:
                status, stdout, stderr = self.run_command(f"ssh -oStrictHostKeyChecking=no {node} 'source /etc/profile && module load {FUNCX_MODULE} && funcx-endpoint list'")
                assert status == 0, f"listing endpoints on '{node}' failed: {stdout} {stderr}"

                for line in stdout.splitlines():
                    if FUNCX_ENDPOINT_NAME in line:
                        funcx_endpoint_id = line.split('|')[3].strip()
                        if "Running" in line:
                            funcx_running_nodes.append(node)
                            break

            if len(funcx_running_nodes) == 1:
                logger.debug(f"funcX '{FUNCX_ENDPOINT_NAME}' endpoint is running on '{funcx_running_nodes[0]}' with endpoint id '{funcx_endpoint_id}'")
            elif len(funcx_running_nodes) > 1:
                logger.warning(f'funcX endpoint running on multiple nodes -> attempting to stop them all: {funcx_running_nodes}')
                for node in funcx_running_nodes:
                    status, stdout, stderr = self.run_command(f"ssh {node} 'source /etc/profile && module load {FUNCX_MODULE} && funcx-endpoint stop {FUNCX_ENDPOINT_NAME}'")
                    if status:
                        raise RuntimeError(f"Failed to stop funcX endpoint on '{node}': {stdout} {stderr}")
                funcx_running_nodes = []
            else:
                logger.debug("funcX endpoint is not running")

            return len(funcx_running_nodes) != 0, funcx_endpoint_id

        else:
            raise RuntimeError("ensure funcX is authorised before checking whether the endpoint is running")
