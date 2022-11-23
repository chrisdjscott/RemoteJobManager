"""
This is an interactive script that will restart the funcX endpoint running on NeSI. Sometimes the endpoint gets into a
bad state due to network, file system, etc issues on NeSI and this script will attempt to fix it.

While running this script, you will need to enter your NeSI username, password and second factor, your NeSI project code and
will need to use a web browser to carry out the Globus authentication as required.

"""
import os
import sys
import copy
import argparse
import logging
import getpass

import pwinput

from rjm import utils
from rjm import config as config_helper
from rjm import __version__
from rjm.setup.nesi import NeSISetup
from rjm.auth import do_authentication


logger = logging.getLogger(__name__)


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Restart funcX on NeSI for use with RJM")
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

    # check if config already exists and ask for confirmation before proceeding
    if not os.path.exists(config_helper.CONFIG_FILE_LOCATION):
        print("Error: RJM config file does not exist, you must run `rjm_config` before running this program!")
        sys.exit(1)

    # setup logging
    utils.setup_logging(log_name="rjm_restart", log_file=args.logfile, log_level=args.loglevel)

    print("="*120)
    print("This is an interactive script to restart funcX on NeSI for use with RJM")
    print("You will be required to enter information along the way, including NeSI credentials and to")
    print("authenticate with Globus in a browser when asked to do so")
    print("="*120)
    print("At times a browser window will be automatically opened and you will be asked to authenticate")
    print("and allow RJM to have access. Please ensure the default browser on your system is set to a modern")
    print("and reasonably up to date browser.")
    print("="*120)
    print("It is quite normal for there to be gaps of up to a few minutes between output, as the setup is")
    print("happening in the background.")
    print("="*120)
    print("Please be prepared to enter you NeSI password and second factor below")
    print("Also, please ensure the second factor has at least 5 seconds remaining before it refreshes")
    print()

    # get extra info from user
    username = input(f"Enter NeSI username or press enter to accept default [{getpass.getuser()}]: ").strip() or getpass.getuser()
    account = input("Enter NeSI project code or press enter to accept default (you must belong to it) [uoa00106]: ").strip() or "uoa00106"
    password = pwinput.pwinput(prompt="Enter NeSI Login Password (First Factor): ")
    token = input("Enter NeSI Authenticator Code (Second Factor with at least 5s before it refreshes): ")
    print("="*120)

    # create the setup object
    nesi = NeSISetup(username, password, token, account)

    # restart funcx
    nesi.setup_funcx(restart=True)


if __name__ == "__main__":
    nesi_setup()
