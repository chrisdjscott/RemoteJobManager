
import sys
import argparse
import logging
import getpass

from rjm import utils
from rjm import __version__
from rjm.setup.nesi import NeSISetup


logger = logging.getLogger(__name__)


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Upload files and start jobs")

    parser.add_argument('--funcx', action="store_true", help="Set up funcX on NeSI")
    parser.add_argument('--globus', action="store_true", help="Set up Globus for NeSI")

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

    if not args.funcx and not args.globus:
        print("Neither '--funcx' nor '--globus' specified; nothing to do")

    else:
        # setup logging
        utils.setup_logging(log_name="nesi_setup", log_file=args.logfile, log_level=args.loglevel)

        print("="*120)
        print("This is an interactive script to setup NeSI for using RJM")
        print("You will be required to enter information along the way, including NeSI credentials and to")
        print("authenticate with Globus in a browser when asked to do so")
        print("="*120)

        # get extra info from user
        username = input(f"Enter NeSI username or press enter to accept default [{getpass.getuser()}]: ").strip() or getpass.getuser()
        password = getpass.getpass("Enter NeSI Login Password (First Factor): ")
        token = input("Enter NeSI Authenticator Code (Second Factor with >5 seconds remaining): ")

        # create the setup object
        nesi = NeSISetup(username, password, token)

        # do the funcx setup
        if args.funcx:
            nesi.setup_funcx()

        # do the globus setup
        if args.globus:
            nesi.setup_globus()

        # report
        print("="*120)
        print("Configuration values:")
        if args.funcx:
            funcx_ep = nesi.get_funcx_config()
            print(f"- funcX endpoint id: {funcx_ep}")
        if args.globus:
            globus_ep, globus_path = nesi.get_globus_config()
            print(f"- Globus endpoint id: {globus_ep}")
            print(f"- Globus endpoint path: {globus_path}")
        print("="*120)


if __name__ == "__main__":
    nesi_setup()
