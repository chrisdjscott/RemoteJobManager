"""
This is an interactive script to set up Globus Transfer and Globus Compute on NeSI for use with RJM. It will create a new Globus guest collection
(shared directory) on NeSI and will configure RJM to use the Globus Compute endpoint run by NeSI.
Finally, all configuration values will be written to the RJM config file on the local
machine and the authentication steps will be run.

While running this script, you will need to enter your NeSI username and your NeSI project code and
will need to use a web browser to carry out NeSI and Globus authentication as required.

"""
import os
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


logger = logging.getLogger(__name__)


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Set up RJM for use with NeSI")
    parser.add_argument('-l', '--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-ll', '--loglevel', required=False,
                        help="level of log verbosity (setting the level here overrides the config file)",
                        choices=['debug', 'info', 'warn', 'error', 'critical'])
    parser.add_argument('-w', '--where-config', action="store_true", help="Print location of the config file and exit")
    parser.add_argument('-v', '--version', action="version", version='%(prog)s ' + __version__)

    return parser


def nesi_setup():
    """
    Upload files and start running for the given local directory

    """
    # command line args
    parser = make_parser()
    args = parser.parse_args()

    if args.where_config:
        # print location of config file and exit
        print(f"RJM config file location: {config_helper.CONFIG_FILE_LOCATION}")
        sys.exit(0)

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
    logger = logging.getLogger(__name__)
    logger.info(f"Running rjm_config v{__version__}")

    print()
    print("="*120)
    print()
    print("This is an interactive script to configure RJM for accessing NeSI. "
          "You will be required to enter information along the way, including your NeSI username and project code.")
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
    account = input("Enter NeSI project code or press enter to accept default (you must belong to it) [uoa00106]: ").strip() or "uoa00106"
    print("="*120)

    # create the setup object
    nesi = NeSISetup(username, account)

    # do the globus setup first because it is more interactive
    nesi.setup_globus_transfer()

    # write values to config file
    req_opts = copy.deepcopy(config_helper.CONFIG_OPTIONS_REQUIRED)

    # get config values
    globus_ep, globus_path = nesi.get_globus_transfer_config()
    funcx_ep = nesi.get_globus_compute_config()

    # modify dict to set values as defaults
    done_globus_ep = False
    done_globus_path = False
    done_funcx_ep = False
    for optd in req_opts:
        if optd["section"] == "GLOBUS" and optd["name"] == "remote_endpoint":
            optd["override"] = globus_ep
            done_globus_ep = True
        elif optd["section"] == "GLOBUS" and optd["name"] == "remote_path":
            optd["override"] = globus_path
            done_globus_path = True
        elif optd["section"] == "FUNCX" and optd["name"] == "remote_endpoint":
            optd["override"] = funcx_ep
            done_funcx_ep = True
    assert done_globus_ep
    assert done_globus_path
    assert done_funcx_ep

    # backup current config if any
    if os.path.exists(config_helper.CONFIG_FILE_LOCATION):
        bkp_file = utils.backup_file(config_helper.CONFIG_FILE_LOCATION)
        print(f"Backed up current config file to: {bkp_file}")
        print("="*120)

    # call method to set config file
    config_helper.do_configuration(required_options=req_opts, accept_defaults=True)

    print("="*120)
    print("Configuration file has been updated")
    print("="*120)
    print("Running authenticate next...")

    # force fresh authentication
    do_authentication(force=True, verbose=True)

    print("="*120)
    print("You should be ready to start using rjm now")
    print("="*120)


if __name__ == "__main__":
    nesi_setup()
