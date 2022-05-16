
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
LOGIN_NODES = [
    "mahuika01",
    "mahuika02",
]
FUNCX_MODULE = "funcx-endpoint/0.3.6-gimkl-2020a-Python-3.9.9"
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
        stdin, stdout, stderr = self._client.exec_command("echo $HOSTNAME")
        logger.info(f"Successfully opened connection to {stdout.read().strip().decode('utf-8')}")

    def __del__(self):
        # close connection
        if self._client is not None:
            self._client.close()
        if self._sftp is not None:
            self._sftp.close()
        if self._lander_client is not None:
            self._lander_client.close()

    def _auth_handler(self, title, instructions, prompt_list):
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
        full_command = f"source /etc/profile; module load {FUNCX_MODULE}; {command}"
        logger.debug(f"Running command: '{full_command}'")
        stdin, stdout, stderr = self._client.exec_command(full_command)
        return stdout.read().decode('utf-8').strip(), stderr.read().decode('utf-8').strip()

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
        required_scopes = []

        # select directory for sharing
        account = input("Enter NeSI project code: ").strip()
        guest_collection_dir = f"/nesi/nobackup/{account}/{self._username}/rjm-jobs"
        print("="*100)
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
            stdout, stderr = self.run_command(f"mkdir -p {guest_collection_dir}")
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
        self.get_funcx_status()

        if not self._funcx_authorised:
            required_scopes = [
                utils.OPENID_SCOPE,
                utils.SEARCH_SCOPE,
                FUNCX_SCOPE,
            ]

            # do Globus auth
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
                self.run_command("mkdir -p /home/{username}/.funcx/credentials")

                # transfer the token file we just created
                self._sftp.put(tmp_token_file, self._funcx_cred_file)

        # TODO: configure and start funcx endpoint, report back endpoint id for config


        # TODO: confirm list command works


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
        """Returns whether funcX is configured or not"""
        # assume it is configured if the credentials and default endpoint files exist
        # or run status and see if we can tell from that
        # first test if funcx

        # test if credentials file exists
        if self._remote_path_exists(self._funcx_cred_file):
            logger.debug("Assuming funcx is authorised as credentials file exists")
            self._funcx_authorised = True
        else:
            logger.debug("funcX credentials file does not exist")
            self._funcx_authorised = False

        # test if default endpoint config exists
        if self._remote_path_exists(self._funcx_default_config):
            logger.debug("Assuming funcx default endpoint is configured as config file exists")
            self._funcx_configured = True
        else:
            logger.debug("funcX default endpoint config file does not exist")
            self._funcx_configured = False

        # TODO: test "funcx-endpoint list" shows default endpoint and whether running??
        # TODO: need to loop over login nodes
        if self._funcx_authorised and self._funcx_configured:
            stdout, stderr = self.run_command("funcx-endpoint list")
            print("out")
            print(stdout)
            print("err")
            print(stderr)





if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    nesi = NeSISetup(
        username=input(f"Username [{getpass.getuser()}]: ") or getpass.getuser(),
        password=getpass.getpass("NeSI 1st factor (password): "),
        token=input("NeSI 2nd factor (WITH AT LEAST 10 SECS REMAINING): "),
    )
    #stdout, stderr = nesi_ssh.run_command("scrontab -l")
    #print(stdout)
    #print(stderr)

    nesi.setup_funcx()
#    nesi.setup_globus()

