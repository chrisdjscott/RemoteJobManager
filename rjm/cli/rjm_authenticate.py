
import os
import sys
import argparse

from rjm.remote_job import RemoteJob
from rjm import config as config_helper
from rjm import utils
from rjm import __version__


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Perform required authentication (if any)")
    parser.add_argument('--force', action="store_true",
                        help="Delete any stored tokens and force reauthentication")
    parser.add_argument("--verbose", action="store_true", help="Enable print statements")
    parser.add_argument("-v", '--version', action='version', version='%(prog)s ' + __version__)

    return parser


def authenticate():
    """
    Handle authentication

    """
    # command line args
    parser = make_parser()
    args = parser.parse_args()

    if args.verbose:
        print("Authenticating RJM...")

    # check config file exists (configure should have been done already)
    if not os.path.isfile(config_helper.CONFIG_FILE_LOCATION):
        sys.stderr.write("ERROR: configuration file must be create with rjm_configure before running rjm_authenticate" + os.linesep)
        sys.exit(1)

    # delete token file if exists
    if args.force:
        if os.path.isfile(utils.TOKEN_FILE_LOCATION):
            if args.verbose:
                print(f"Deleting existing token file to force reauthentication: {utils.TOKEN_FILE_LOCATION}")
            os.unlink(utils.TOKEN_FILE_LOCATION)

    # just for first directory should be ok
    rj = RemoteJob()
    globus_scopes = rj.get_required_globus_scopes()
    if args.verbose:
        print("Requesting authentication - will open link in web browser if required...")
    try:
        utils.handle_globus_auth(globus_scopes)
    except Exception as exc:
        sys.stderr.write(f"ERROR: failed to do Globus auth: {exc}" + os.linesep)
        sys.exit(1)
    else:
        if args.verbose:
            print("RJM authentication completed")


if __name__ == "__main__":
    authenticate()
