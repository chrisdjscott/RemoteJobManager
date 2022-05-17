
import os
import logging
import getpass
import tempfile

import paramiko

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
GLOBUS_CREATE_COLLECTION_SCOPE = f"{GLOBUS_NESI_ENDPOINT}[*{GLOBUS_NESI_COLLECTION}]"


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

        funcx_dir = f"/home/{self._username}/.funcx"
        self._funcx_cred_file = f"{funcx_dir}/credentials/funcx_sdk_tokens.json"
        self._funcx_default_config = f"{funcx_dir}/default/config.py"

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

    def run_command(self, command):
        """
        Execute command on NeSI and return stdout and stderr.

        Ensure /etc/profile is sourced prior to running the command.

        """
        full_command = f"source /etc/profile && {command}"
        logger.debug(f"Running command: '{full_command}'")
        stdin, stdout, stderr = self._client.exec_command(full_command)
        output = stdout.read().decode('utf-8').strip()
        error = stderr.read().decode('utf-8').strip()
        status = stdout.channel.recv_exit_status()

        return status, output, error

    def setup_globus(self):
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
        # select directory for sharing
        account = input("Enter NeSI project code: ").strip()
        guest_collection_dir = f"/nesi/nobackup/{account}/{self._username}/rjm-jobs"
        print("="*120)
        print("Creating Globus guest collection at:\n    {guest_collection_dir}")
        response = ("Press enter to continue or enter a different location: ").strip()
        if len(response):
            if response.startswith("/nesi/nobackup"):
                # TODO: check have write access too...
                guest_collection_dir = response
            else:
                raise ValueError("Valid guest collection directories must start with '/nesi/nobackup'")
        logger.info(f"Guest collection directory: {guest_collection_dir}")

        # create the directory if it doesn't exist
        if self._remote_path_exists(guest_collection_dir):
            logger.debug("Guest collection directory already exists")
        else:
            logger.debug("Creating guest collection directory...")
            status, stdout, stderr = self.run_command(f"mkdir -p {guest_collection_dir}")
            assert status == 0, f"Creating '{guest_collection_dir}' failed: {stdout} {stderr}"
            if not self._remote_path_exists(guest_collection_dir):
                raise RuntimeError(f"Creating guest collection directory failed ({stdout}) ({stderr})")

        # do Globus auth
        required_scopes = [GLOBUS_CREATE_COLLECTION_SCOPE]
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_token_file = os.path.join(tmpdir, "tokens.json")
            globus_cli = utils.handle_globus_auth(
                required_scopes,
                token_file=tmp_token_file,
            )

        # create Globus collection, report back endpoint id for config


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
        # make sure funcx is authorised
        if not self.is_funcx_authorised():
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
                utils.handle_globus_auth(
                    required_scopes,
                    token_file=tmp_token_file,
                )
                assert os.path.exists(tmp_token_file)

                # upload funcx tokens to correct place if needed
                logger.debug("Transferring token file to NeSI")

                # create the credentials directory
                status, stdout, stderr = self.run_command(f"mkdir -p /home/{self._username}/.funcx/credentials")
                assert status == 0, "Create credentials file failed: {stdout} {stderr}"

                # transfer the token file we just created
                self._sftp.put(tmp_token_file, self._funcx_cred_file)

            assert self.is_funcx_authorised(), "funcX authorisation failed"
            logger.info("funcX authorisatiion complete")

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
        print("the above value will be required when configuring RJM")
        print("="*120)

        # TODO: install scrontab if not already installed


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
                status, stdout, stderr = self.run_command(f"ssh {node} 'source /etc/profile && module load {FUNCX_MODULE} && funcx-endpoint list'")
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
                        logger.warning(f"Failed to stop funcX endpoint on '{node}': {stdout} {stderr}")
            else:
                logger.debug("funcX endpoint is not running")

            return len(funcx_running_nodes) != 0, funcx_endpoint_id

        else:
            raise RuntimeError("ensure funcX is authorised before checking whether the endpoint is running")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    logging.getLogger("globus_sdk").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("fair_research_login").setLevel(logging.WARNING)
    nesi = NeSISetup(
        username=input(f"Enter NeSI username or press enter to accept default [{getpass.getuser()}]: ") or getpass.getuser(),
        password=getpass.getpass("Enter NeSI Login Password (First Factor): "),
        token=input("Enter NeSI Authenticator Code (Second Factor with >5 seconds remaining): "),
    )
    #stdout, stderr = nesi_ssh.run_command("scrontab -l")
    #print(stdout)
    #print(stderr)

    nesi.setup_funcx()
#    nesi.setup_globus()

