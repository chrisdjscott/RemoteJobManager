"""
This is an interactive script to set up Globus Transfer and Globus Compute on NeSI for use with RJM. It will create a new Globus guest collection
(shared directory) on NeSI for each NeSI project and will configure RJM to use the Globus Compute endpoint run by NeSI.
Finally, all configuration values will be written to the RJM config file on the local
machine and the authentication steps will be run.

While running this script, you will need to enter your NeSI username and your NeSI project code(s) and
will need to use a web browser to carry out NeSI and Globus authentication as required.

With --add-project, a guest collection is created for one additional project and added to the
existing configuration.

"""
import os
import re
import sys
import copy
import argparse
import logging
import getpass

from rjm import utils
from rjm import config as config_helper
from rjm import __version__
from rjm.setup.nesi import NeSISetup
from rjm.auth import do_authentication
from rjm.errors import RemoteJobConfigError


logger = logging.getLogger(__name__)

# NeSI project codes are lowercase letters followed by digits, e.g. uoa00106
PROJECT_CODE_RE = re.compile(r"^[a-z]+[0-9]+$")
DEFAULT_PROJECT_CODE = "uoa00106"


def parse_project_codes(text):
    """
    Return the list of project codes in a comma separated string, without duplicates

    :raises ValueError: if any project code is not valid

    """
    codes = [code.strip() for code in text.split(",") if code.strip()]
    if not codes:
        raise ValueError("No project codes were entered")
    for code in codes:
        if not PROJECT_CODE_RE.match(code):
            raise ValueError(f"Invalid NeSI project code: '{code}' (expected lowercase letters followed by digits, e.g. uoa00106)")

    return list(dict.fromkeys(codes))


def prompt_project_codes():
    """Ask the user for their NeSI project codes until a valid list is entered"""
    while True:
        text = input(
            "Enter NeSI project code(s), separated by commas, or press enter to accept default "
            f"(you must belong to them) [{DEFAULT_PROJECT_CODE}]: "
        ).strip() or DEFAULT_PROJECT_CODE
        try:
            return parse_project_codes(text)
        except ValueError as exc:
            print(exc)


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Set up RJM for use with NeSI")
    parser.add_argument('-l', '--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-ll', '--loglevel', required=False,
                        help="level of log verbosity (setting the level here overrides the config file)",
                        choices=['debug', 'info', 'warn', 'error', 'critical'])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('-s', '--ssh', action='store_true',
                      help='Generate an SSH key pair (stored under ~/.rjm) for use with the Paramiko runner. When this option is chosen, Globus Transfer/Compute setup is skipped.')
    mode.add_argument('-a', '--add-project', metavar='PROJECTCODE',
                      help='Create a Globus guest collection for an additional NeSI project and add it to the existing configuration')
    parser.add_argument('-w', '--where-config', action="store_true", help="Print location of the config file and exit")
    parser.add_argument('-v', '--version', action="version", version='%(prog)s ' + __version__)

    return parser


def add_project(project):
    """
    Create a guest collection for one more project and add it to the existing config

    """
    if not PROJECT_CODE_RE.match(project):
        sys.exit(f"ERROR: invalid NeSI project code: '{project}' (expected lowercase letters followed by digits, e.g. uoa00106)")

    try:
        config = config_helper.load_config()
    except RemoteJobConfigError as exc:
        sys.exit(f"ERROR: {exc}")
    if config_helper.is_paramiko_stack(config):
        sys.exit("ERROR: projects can only be added when RJM is configured to use Globus")
    if project in config_helper.get_projects(config):
        sys.exit(f"ERROR: project '{project}' is already configured")

    username = input(f"Enter NeSI username or press enter to accept default [{getpass.getuser()}]: ").strip() or getpass.getuser()
    print("="*120)

    # create the guest collection
    nesi = NeSISetup(username, [project])
    nesi.setup_globus_transfer()
    remote_endpoint, remote_path = nesi.get_globus_transfer_config()[project]

    # backup the current config and add the project
    bkp_file = utils.backup_file(config_helper.CONFIG_FILE_LOCATION)
    print(f"Backed up current config file to: {bkp_file}")
    config_helper.add_project(config, project, remote_endpoint, remote_path)
    config_helper.save_config(config)

    print("="*120)
    print(f"Added project {project} to the configuration file")
    print("="*120)

    # authenticate, only logging in again if the token file lacks the new collection's scope
    print("Running authenticate next...")
    do_authentication(verbose=True)

    print("="*120)
    print(f"You should be ready to start using rjm with project {project} now")
    print("="*120)


def nesi_setup(args=None):
    """
    Interactively configure RJM for NeSI

    """
    # command line args
    parser = make_parser()
    args = parser.parse_args(args)
    # Determine whether Globus setup should be performed.
    # If SSH option is chosen, we skip Globus (mutually exclusive behavior).
    no_globus = args.ssh

    if args.where_config:
        # print location of config file and exit
        print(f"RJM config file location: {config_helper.CONFIG_FILE_LOCATION}")
        sys.exit(0)

    if args.add_project:
        utils.setup_logging(log_name="rjm_config", log_file=args.logfile, log_level=args.loglevel)
        logger.info(f"Running rjm_config v{__version__}")
        add_project(args.add_project)
        return

    # check if config already exists and ask for confirmation before proceeding
    if os.path.exists(config_helper.CONFIG_FILE_LOCATION):
        print("Detected a current RJM config file at the following location:")
        print(f"    {config_helper.CONFIG_FILE_LOCATION}")
        print("On successful completion the existing configuration will be backed up and replaced")
        proceed = input("Enter 'yes' if you wish to proceed: ")
        if proceed.strip() != 'yes':
            sys.exit('Stopping')

    # setup logging
    utils.setup_logging(log_name="rjm_config", log_file=args.logfile, log_level=args.loglevel)

    # report version
    logger.info(f"Running rjm_config v{__version__}")

    if not no_globus:
        print()
        print("="*120)
        print()
        print("This is an interactive script to configure RJM for accessing NeSI. "
              "You will be required to enter information along the way, including your NeSI username and project code(s).")
        print()
        print("="*120)
        print()
        print("At times either a browser window will be automatically opened, or you will be asked to copy a link and open it "
              "in a browser, where you will be asked to authenticate and allow RJM to have access. "
              "Please ensure the default browser on your system is set to a modern and reasonably up to date browser.")
        print()
        print("="*120)
        print()
        print("In some situations a new link will be opened in your browser immediately after you authenticated the last one, "
              "which can be easy to miss, so if it looks like nothing is happening, please check your browser window for a pending authentication.")
        print()
        print("="*120)
        print()

        # get extra info from user
        username = input(f"Enter NeSI username or press enter to accept default [{getpass.getuser()}]: ").strip() or getpass.getuser()
        projects = prompt_project_codes()
        print("="*120)

    else:
        print()
        print("="*120)
        print()
        print("This is an interactive script to configure RJM for accessing a remote machine via SSH. "
              "You will be required to enter information along the way, including your username on the remote machine.")
        print()
        print("="*120)
        print()

        # get extra info from user
        username = input(f"Enter remote username or press enter to accept default [{getpass.getuser()}]: ").strip() or getpass.getuser()
        projects = None
        print("="*120)

    # create the setup object
    nesi = NeSISetup(username, projects)

    # do the globus setup unless the user asked to skip it
    if not no_globus:
        # This step is interactive and may open a browser
        nesi.setup_globus_transfer()

    # If the user asked for an SSH key‑pair, generate it now via the new helper
    if args.ssh:
        # This will prompt for the remote base path and create the key pair
        nesi.setup_paramiko()
        paramiko_cfg = nesi.get_paramiko_config()

    # write values to config file
    req_opts = copy.deepcopy(config_helper.CONFIG_OPTIONS)

    # get config values (only if globus setup was performed)
    project_collections = None
    if not no_globus:
        funcx_ep = nesi.get_globus_compute_config()
        project_collections = nesi.get_globus_transfer_config()

    # modify dict to set values as defaults
    done_funcx_ep = False

    # Populate overrides – Globus overrides are applied only when Globus was run.
    # Paramiko overrides are applied only when the SSH option was chosen.
    for optd in req_opts:
        # ----- Globus overrides (only when Globus setup was run) -----
        if not no_globus:
            if optd["section"] == "GLOBUS_COMPUTE" and optd["name"] == "remote_endpoint":
                optd["override"] = funcx_ep
                done_funcx_ep = True

            # ----- set the transferer and runner based on whether ssh option was chosen or not
            if optd["section"] == "COMPONENTS":
                if optd["name"] == "runner":
                    optd["override"] = "globus_compute_slurm_runner"
                elif optd["name"] == "transferer":
                    optd["override"] = "globus_https_transferer"

        # ----- Paramiko overrides (only when SSH key pair was generated) -----
        if args.ssh:
            if optd["section"] == "PARAMIKO":
                if optd["name"] == "private_key_file":
                    optd["override"] = paramiko_cfg["private_key_file"]
                elif optd["name"] == "remote_user":
                    optd["override"] = paramiko_cfg["remote_user"]
                elif optd["name"] == "remote_base_path":
                    optd["override"] = paramiko_cfg["remote_base_path"]
                elif optd["name"] == "remote_address":
                    # Store the remote machine address entered during Paramiko setup
                    optd["override"] = paramiko_cfg["remote_address"]

            # ----- set the transferer and runner based on whether ssh option was chosen or not
            if optd["section"] == "COMPONENTS":
                if optd["name"] == "runner":
                    optd["override"] = "paramiko_ssh_runner"
                elif optd["name"] == "transferer":
                    optd["override"] = "paramiko_sftp_transferer"

    # sanity checks – only required when Globus overrides were attempted
    if not no_globus:
        assert done_funcx_ep

    # backup current config if any
    if os.path.exists(config_helper.CONFIG_FILE_LOCATION):
        bkp_file = utils.backup_file(config_helper.CONFIG_FILE_LOCATION)
        print(f"Backed up current config file to: {bkp_file}")
        print("="*120)

    # call method to set config file
    config_helper.do_configuration(config_options=req_opts, projects=project_collections)

    print("="*120)
    print("Configuration file has been updated")
    print("="*120)

    # Run authentication only if Globus steps were not skipped
    if not no_globus:
        print("Running authenticate next...")
        # force fresh authentication
        do_authentication(force=True, verbose=True)

    print("="*120)
    print("You should be ready to start using rjm now")
    print("="*120)


if __name__ == "__main__":
    nesi_setup()
