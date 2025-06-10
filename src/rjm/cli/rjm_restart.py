"""
This is a legacy script that is no longer required as we move to using a Multi-User endpoint on NeSI. You can still run it but
it won't do anything.

This is an interactive script that will restart your Globus Compute endpoint running on NeSI. Sometimes the endpoint gets into a
bad state due to network, file system, etc issues on NeSI and this script will attempt to fix it.

While running this script, you will need to enter your NeSI username and your NeSI project code and
will need to use a web browser to carry out NeSI and Globus authentication as required.

"""
import os
import sys
import argparse
import logging
import getpass

from rjm import utils
from rjm import config as config_helper
from rjm import __version__
from rjm.setup.nesi import NeSISetup


logger = logging.getLogger(__name__)


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Restart Globus Compute on NeSI for use with RJM")
    parser.add_argument('-l', '--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-ll', '--loglevel', required=False,
                        help="level of log verbosity (setting the level here overrides the config file)",
                        choices=['debug', 'info', 'warn', 'error', 'critical'])
    parser.add_argument('-r', '--reauth', action="store_true", help="Force reauthentication of globus-compute-endpoint")
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

    # report version
    logger = logging.getLogger(__name__)
    logger.info(f"Running rjm_restart v{__version__}")

    print("="*120)
    print("This is an interactive script to restart Globus Compute on NeSI for use with RJM")
    print("You will be required to enter information along the way, including opening a link to enter your NeSI credentials and to")
    print("authenticate with Globus in a browser when asked to do so")
    print("="*120)
    print("At times a browser window will be automatically opened and you will be asked to authenticate")
    print("and allow RJM to have access. Please ensure the default browser on your system is set to a modern")
    print("and reasonably up to date browser.")
    print("="*120)
    print("It is quite normal for there to be gaps of up to a few minutes between output, as the setup is")
    print("happening in the background.")
    print()

    # get extra info from user
    username = input(f"Enter NeSI username or press enter to accept default [{getpass.getuser()}]: ").strip() or getpass.getuser()
    account = input("Enter NeSI project code or press enter to accept default (you must belong to it) [uoa00106]: ").strip() or "uoa00106"
    print("="*120)

    # create the setup object
    nesi = NeSISetup(username, account)

    # restart funcx
    nesi.setup_globus_compute(restart=True, reauthenticate=args.reauth)


if __name__ == "__main__":
    nesi_setup()
