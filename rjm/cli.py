
import os
import logging
import argparse
from datetime import datetime

from . import __version__
from . import utils
from . import config as config_helper
from .remote_job import RemoteJob


logger = logging.getLogger(__name__)


def _load_local_dirs(dirsfile):
    with open(dirsfile) as fh:
        local_dirs = fh.readlines()
    local_dirs = [d.strip() for d in local_dirs]

    # get list of local directories
    local_dirs_exist = []
    for local_dir in local_dirs:
        if os.path.isdir(local_dir):
            local_dirs_exist.append(local_dir)
        else:
            logger.warning(f'Local directory does not exist: "{local_dir}" (skipping)')
    logger.debug(f"Local directories: {local_dirs_exist}")

    return local_dirs_exist


def _batch_arg_parser(*args, **kwargs):
    """
    Create arg parser for batch_* commands

    """
    parser = argparse.ArgumentParser(*args, **kwargs)
    parser.add_argument('-f', '--localjobdirfile', required=True,
                        help="file that contains the names of the local job directories, one name per line")
    parser.add_argument('-l', '--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-ll', '--loglevel', required=False,
                        help="level of log verbosity (setting the level here overrides the config file)",
                        choices=['debug', 'info', 'warn', 'error', 'critical'])
    parser.add_argument('-v', '--version', action="version", version='%(prog)s ' + __version__)

    return parser


def configure():
    """
    Run through configuration steps

    """
    parser = argparse.ArgumentParser(description="Walk through the configuration of RJM")
    parser.add_argument("-v", '--version', action='version', version='%(prog)s ' + __version__)
    parser.parse_args()

    config_helper.do_configuration()


def authenticate():
    """
    Handle authentication

    """
    print("Authenticating RJM...")

    # command line args
    parser = argparse.ArgumentParser(description="Perform required authentication (if any)")
    parser.add_argument('--force', action="store_true",
                        help="Delete any stored tokens and force reauthentication")
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


def batch_submit():
    """
    Upload files and start running for the given local directory

    """
    # command line args
    parser = _batch_arg_parser(description="Upload files and start jobs")
    parser.add_argument('--force', action="store_true",
                        help="Ignore progress from previous runs stored in job directory, i.e. start from scratch")
    args = parser.parse_args()

    # setup
    utils.setup_logging(log_file=args.logfile, log_level=args.loglevel)
    local_dirs = _load_local_dirs(args.localjobdirfile)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

    # loop over local directories
    for local_dir in local_dirs:
        rj = RemoteJob(timestamp=timestamp)
        rj.setup(local_dir, force=args.force)
        rj.upload_and_start()


def batch_wait():
    """
    Wait for run completion and download files for all remote jobs.

    """
    # command line args
    parser = _batch_arg_parser(description="Wait for jobs to complete and download files")
    args = parser.parse_args()

    # setup
    utils.setup_logging(log_file=args.logfile, log_level=args.loglevel)
    local_dirs = _load_local_dirs(args.localjobdirfile)

    # loop over local directories
    for local_dir in local_dirs:
        rj = RemoteJob()
        rj.setup(local_dir)
        rj.wait_and_download()


if __name__ == "__main__":
    print(">>> Running batch_submit")
    batch_submit()
    print(">>> Running batch_wait")
    batch_wait()
    print(">>> done")
