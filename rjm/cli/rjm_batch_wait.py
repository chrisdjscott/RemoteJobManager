
import sys
import logging
import argparse

from rjm import __version__
from rjm import utils
from rjm.remote_job import RemoteJob
from rjm.cli import read_local_dirs_file


logger = logging.getLogger(__name__)


def make_parser():
    """
    Create arg parser for batch_* commands

    """
    parser = argparse.ArgumentParser(description="Wait for the jobs to complete and download files")
    parser.add_argument('-f', '--localjobdirfile', required=True,
                        help="file that contains the names of the local job directories, one name per line")
    parser.add_argument('-l', '--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-ll', '--loglevel', required=False,
                        help="level of log verbosity (setting the level here overrides the config file)",
                        choices=['debug', 'info', 'warn', 'error', 'critical'])
    parser.add_argument('-z', '--pollingintervalsec', type=int,
                        help="number of seconds to wait between attempts to poll for job status")
    parser.add_argument('-v', '--version', action="version", version='%(prog)s ' + __version__)

    return parser


def batch_wait():
    """
    Wait for run completion and download files for all remote jobs.

    """
    # command line args
    parser = make_parser()
    args = parser.parse_args()

    # setup
    utils.setup_logging(log_file=args.logfile, log_level=args.loglevel)
    local_dirs = read_local_dirs_file(args.localjobdirfile)

    # loop over local directories
    failures = []
    for local_dir in local_dirs:
        try:
            rj = RemoteJob()
            rj.setup(local_dir)
            rj.wait_and_download(polling_interval=args.pollingintervalsec)
        except Exception as exc:
            # append string to list of failures
            failures.append(f"[{local_dir}]: {exc}")

    # print any errors
    if len(failures):
        logger.error(f"{len(failures)} local directories failed, errors listed below:")
        for msg in failures:
            logger.error(msg)
        sys.exit(1)


if __name__ == "__main__":
    batch_wait()
