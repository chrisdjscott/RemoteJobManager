
import argparse

from rjm import __version__
from rjm import utils
from rjm.remote_job import RemoteJob
from rjm.cli import read_local_dirs_file


def make_parser():
    """
    Create arg parser for batch_* commands

    """
    parser = argparse.ArgumentParser(description="Cancel any running jobs")
    parser.add_argument('-f', '--localjobdirfile', required=True,
                        help="file that contains the names of the local job directories, one name per line")
    parser.add_argument('-l', '--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-ll', '--loglevel', required=False,
                        help="level of log verbosity (setting the level here overrides the config file)",
                        choices=['debug', 'info', 'warn', 'error', 'critical'])

    not_used_help = "not used; provided for compatibility with previous versions"
    parser.add_argument('-z', '--pollingintervalsec', help=not_used_help, action='append')

    parser.add_argument('-v', '--version', action="version", version='%(prog)s ' + __version__)

    return parser


def batch_cancel():
    """
    Cancel all jobs

    """
    # command line args
    parser = make_parser()
    args = parser.parse_args()

    # setup
    utils.setup_logging(log_name="batch_cancel", log_file=args.logfile, log_level=args.loglevel)
    local_dirs = read_local_dirs_file(args.localjobdirfile)

    # loop over local directories
    for local_dir in local_dirs:
        rj = RemoteJob()
        rj.setup(local_dir)
        rj.run_cancel()


if __name__ == "__main__":
    batch_cancel()
