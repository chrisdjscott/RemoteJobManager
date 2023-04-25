
import os
import argparse

from rjm import utils
from rjm import __version__
from rjm.auth import do_authentication


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Perform required authentication (if any)")
    parser.add_argument('--force', action="store_true",
                        help="Delete any stored tokens and force reauthentication")
    parser.add_argument("--verbose", action="store_true", help="Enable print statements")
    parser.add_argument('-l', '--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-ll', '--loglevel', default="critical",
                        help="level of log verbosity (default: %(default)s)",
                        choices=['debug', 'info', 'warn', 'error', 'critical'])
    parser.add_argument("-v", '--version', action='version', version='%(prog)s ' + __version__)

    return parser


def authenticate():
    """
    Handle authentication

    """
    # command line args
    parser = make_parser()
    args = parser.parse_args()

    # setup logging
    utils.setup_logging(log_file=args.logfile, log_level=args.loglevel)

    # check if the token file already exists
    initial_run = not os.path.isfile(utils.TOKEN_FILE_LOCATION)

    # run the authentication
    do_authentication(force=args.force, verbose=(args.verbose or initial_run))


if __name__ == "__main__":
    authenticate()
