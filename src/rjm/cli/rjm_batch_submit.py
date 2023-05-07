
import argparse
import logging

from rjm.remote_job_batch import RemoteJobBatch
from rjm import utils
from rjm import __version__


logger = logging.getLogger(__name__)


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Upload files and start jobs")
    parser.add_argument('-f', '--localjobdirfile', required=True,
                        help="file that contains the names of the local job directories, one name per line")
    parser.add_argument('-l', '--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-ll', '--loglevel', required=False,
                        help="level of log verbosity (setting the level here overrides the config file)",
                        choices=['debug', 'info', 'warn', 'error', 'critical'])
    parser.add_argument('-le', '--logextra', action='store_true', help='Also log funcx and globus at the chosen loglevel')
    parser.add_argument('--force', action="store_true",
                        help="ignore progress from previous runs stored in job directory, i.e. start from scratch")
    parser.add_argument('-n', '--defaultlogname', action='store_true', help='Use default log name instead of "batch_submit"')

    not_used_help = "not used; provided for compatibility with previous versions"
    parser.add_argument('-c', '--cmd', help=not_used_help, action='append')
    parser.add_argument('-d', '--remotedir', help=not_used_help)
    parser.add_argument('-j', '--jobtype', help=not_used_help)
    parser.add_argument('-m', '--mem', help=not_used_help)
    parser.add_argument('-p', '--projectcode', help=not_used_help)
    parser.add_argument('-w', '--walltime', help=not_used_help)

    parser.add_argument('-v', '--version', action="version", version='%(prog)s ' + __version__)

    return parser


def batch_submit():
    """
    Upload files and start running for the given local directory

    """
    # command line args
    parser = make_parser()
    args = parser.parse_args()

    # setup logging
    log_name = None if args.defaultlogname else "batch_submit"
    utils.setup_logging(log_name=log_name, log_file=args.logfile, log_level=args.loglevel, cli_extra=args.logextra)

    # report version
    logger = logging.getLogger(__name__)
    logger.info(f"Running rjm_batch_submit v{__version__}")

    # create the object for managing a batch of remote jobs
    rjb = RemoteJobBatch()
    rjb.setup(args.localjobdirfile, force=args.force)

    # upload files and start
    rjb.upload_and_start()


if __name__ == "__main__":
    batch_submit()
