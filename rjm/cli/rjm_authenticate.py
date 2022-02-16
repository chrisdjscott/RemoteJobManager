
import os
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
    parser.add_argument("-v", '--version', action='version', version='%(prog)s ' + __version__)

    return parser


def authenticate():
    """
    Handle authentication

    """
    print("Authenticating RJM...")

    # command line args
    parser = make_parser()
    args = parser.parse_args()

    # check config file exists (configure should have been done already)
    if not os.path.isfile(config_helper.CONFIG_FILE_LOCATION):
        raise RuntimeError("rjm configure must be run before authenticate")

    # delete token file if exists
    if args.force:
        if os.path.isfile(utils.TOKEN_FILE_LOCATION):
            print(f"Deleting existing token file to force reauthentication: {utils.TOKEN_FILE_LOCATION}")
            os.unlink(utils.TOKEN_FILE_LOCATION)

    # just for first directory should be ok
    rj = RemoteJob()
    globus_scopes = rj.get_required_globus_scopes()
    print("Requesting authentication - will open link in web browser if required...")
    utils.handle_globus_auth(globus_scopes)
    print("RJM authentication completed")


if __name__ == "__main__":
    authenticate()
