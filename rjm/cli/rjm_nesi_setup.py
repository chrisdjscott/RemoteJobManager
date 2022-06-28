
import os
import copy
import argparse
import logging
import getpass

import pwinput

from rjm import utils
from rjm import config as config_helper
from rjm import __version__
from rjm.setup.nesi import NeSISetup


logger = logging.getLogger(__name__)


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Upload files and start jobs")

    parser.add_argument('--funcx', action="store_true", help="Set up funcX on NeSI")
    parser.add_argument('--globus', action="store_true", help="Set up Globus for NeSI")
    parser.add_argument('--config', action="store_true", help="Write config values to config file (--config implies --funcx and --globus)")

    parser.add_argument('-l', '--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-ll', '--loglevel', required=False,
                        help="level of log verbosity (setting the level here overrides the config file)",
                        choices=['debug', 'info', 'warn', 'error', 'critical'])

    parser.add_argument('-v', '--version', action="version", version='%(prog)s ' + __version__)

    return parser


def nesi_setup():
    """
    Upload files and start running for the given local directory

    """
    # command line args
    parser = make_parser()
    args = parser.parse_args()

    if not args.funcx and not args.globus and not args.config:
        print("Neither '--funcx', '--globus' nor '--config' specified; nothing to do")

    else:
        # setup logging
        utils.setup_logging(log_name="nesi_setup", log_file=args.logfile, log_level=args.loglevel)

        print("="*120)
        print("This is an interactive script to setup RJM for accessing NeSI")
        print("You will be required to enter information along the way, including NeSI credentials and to")
        print("authenticate with Globus in a browser when asked to do so")
        print("="*120)
        print("At times a browser window will be automatically opened and you will be asked to authenticate")
        print("and allow RJM to have access. Please ensure the default browser on your system is set to a modern")
        print("and reasonably up to date browser.")
        print("="*120)
        print("Please be prepared to enter you NeSI password and second factor below")
        print("Also, please ensure the second factor has at least 5 seconds remaining before it refreshes")
        print()

        # get extra info from user
        username = input(f"Enter NeSI username or press enter to accept default [{getpass.getuser()}]: ").strip() or getpass.getuser()
        account = input("Enter NeSI project code or press enter to accept default (you must belong to it) [uoa00106]: ").strip() or "uoa00106"
        password = pwinput.pwinput(prompt="Enter NeSI Login Password (First Factor): ")
        token = input("Enter NeSI Authenticator Code (Second Factor with at least 5s before it refreshes): ")

        # create the setup object
        nesi = NeSISetup(username, password, token, account)

        # do the funcx setup
        if args.funcx or args.config:
            nesi.setup_funcx()

        # do the globus setup
        if args.globus or args.config:
            nesi.setup_globus()

        if args.config:
            # write values to config file
            req_opts = copy.deepcopy(config_helper.CONFIG_OPTIONS_REQUIRED)

            # get config values
            globus_ep, globus_path = nesi.get_globus_config()
            funcx_ep = nesi.get_funcx_config()

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
            print("Please run rjm_authenticate to finish setup")
            print("="*120)

        else:
            # just report the values
            print("="*120)
            print("Configuration values for use with rjm_configure:")
            if args.funcx:
                funcx_ep = nesi.get_funcx_config()
                print(f"- funcX endpoint id: {funcx_ep}")
            if args.globus:
                globus_ep, globus_path = nesi.get_globus_config()
                print(f"- Globus endpoint id: {globus_ep}")
                print(f"- Globus endpoint path: {globus_path}")
            print("Please run rjm_configure to continue with the configuration")
            print("="*120)


if __name__ == "__main__":
    nesi_setup()
