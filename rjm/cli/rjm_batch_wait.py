
import logging
import argparse

from rjm import __version__
from rjm import utils
from rjm.remote_job_batch import RemoteJobBatch


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

    # setup logging
    utils.setup_logging(log_name="batch_wait", log_file=args.logfile, log_level=args.loglevel)

    # create the object for managing a batch of remote jobs
    rjb = RemoteJobBatch()
    rjb.setup(args.localjobdirfile)

    # wait for jobs to complete
    try:
        rjb.wait_and_download(polling_interval=args.pollingintervalsec)
    except BaseException as exc:
        # writing an stderr.txt file into the directory of unfinished jobs, for wfn
        rjb.write_stderr_for_unfinshed_jobs(f"RJM exited with the following error (check logs for more info): {repr(exc)}")
        raise exc


if __name__ == "__main__":
    batch_wait()
