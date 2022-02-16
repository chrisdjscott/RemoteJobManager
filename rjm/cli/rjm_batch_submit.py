
import argparse
from datetime import datetime

from rjm.remote_job import RemoteJob
from rjm import utils
from rjm import __version__
from rjm.cli import read_local_dirs_file


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Upload files and start jobs")
    parser.add_argument('-f', '--localjobdirfile', required=True,
                        help="file that contains the names of the local job directories, one name per line")
    parser.add_argument('-l', '--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-ll', '--loglevel', required=False,
                        help="level of log verbosity (setting the level here overrides the config file)",
                        choices=['debug', 'info', 'warn', 'error', 'critical'])
    parser.add_argument('--force', action="store_true",
                        help="Ignore progress from previous runs stored in job directory, i.e. start from scratch")
    parser.add_argument('-v', '--version', action="version", version='%(prog)s ' + __version__)

    return parser


def batch_submit():
    """
    Upload files and start running for the given local directory

    """
    # command line args
    parser = make_parser()
    args = parser.parse_args()

    # setup
    utils.setup_logging(log_file=args.logfile, log_level=args.loglevel)
    local_dirs = read_local_dirs_file(args.localjobdirfile)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

    # loop over local directories
    for local_dir in local_dirs:
        rj = RemoteJob(timestamp=timestamp)
        rj.setup(local_dir, force=args.force)
        rj.upload_and_start()
