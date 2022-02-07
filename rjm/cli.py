
import os
import logging
import argparse
from datetime import datetime

from . import __version__
from . import utils
from .remote_job import RemoteJob


logger = logging.getLogger(__name__)


def load_local_dirs(dirsfile):
    with open(dirsfile) as fh:
        local_dirs = fh.readlines()
    local_dirs = [d.strip() for d in local_dirs]

    # list of RemoteJob objects based on local directories
    local_dirs_exist = []
    for local_dir in local_dirs:
        if os.path.isdir(local_dir):
            local_dirs_exist.append(local_dir)
        else:
            logger.warning(f'Local directory does not exist: "{local_dir}" (skipping)')
    logger.debug(f"Local directories: {local_dirs_exist}")

    return local_dirs_exist


def batch_submit():
    """
    Upload files and start running for the given local directory

    """
    # command line args
    parser = argparse.ArgumentParser(description="Upload files and start jobs")
    parser.add_argument('-f', '--localjobdirfile', required=True,
                        help="file that contains the names of the local job directories, one name per line")
    parser.add_argument('-l','--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-v', '--version', action="version", version='%(prog)s ' + __version__)
    args = parser.parse_args()

    # setup
    utils.setup_logging(log_file=args.logfile)
    local_dirs = load_local_dirs(args.localjobdirfile)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

    # loop over local directories
    for local_dir in local_dirs:
        rj = RemoteJob(local_dir, timestamp=timestamp)
        rj.upload_and_start()


def batch_wait():
    """
    Wait for run completion and download files for all remote jobs.

    """
    # command line args
    parser = argparse.ArgumentParser(description="Wait for jobs to complete and download files")
    parser.add_argument('-f', '--localjobdirfile', required=True, type=str,
                        help="file that contains the names of the local job directories, one name per line")
    parser.add_argument('-l','--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-v', '--version', action="version", version='%(prog)s ' + __version__)
    args = parser.parse_args()

    # setup
    utils.setup_logging(log_file=args.logfile)
    local_dirs = load_local_dirs(args.localjobdirfile)

    # loop over local directories
    for local_dir in local_dirs:
        rj = RemoteJob(local_dir)
        rj.wait_and_download()


if __name__ == "__main__":
    print(">>> Running batch_submit")
    batch_submit()
    print(">>> Running batch_wait")
    batch_wait()
    print(">>> done")
