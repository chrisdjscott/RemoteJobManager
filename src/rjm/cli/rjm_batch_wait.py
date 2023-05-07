"""
Script to wait for completion of a batch of jobs and download the output files.

A file containing a list of local directories is passed as an argument. Each
directory should contain an *rjm_uploads.txt* file, with the list of files to be
uploaded, and a *run.sl* Slurm script, which will be submitted to Slurm on the
remote machine.

"""
import logging
import argparse
import traceback

from rjm import __version__
from rjm import utils
from rjm.remote_job_batch import RemoteJobBatch
from rjm.runners.globus_compute_slurm_runner import MIN_POLLING_INTERVAL


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
    parser.add_argument('-le', '--logextra', action='store_true', help='Also log funcx and globus at the chosen loglevel')
    parser.add_argument('-z', '--pollingintervalsec', type=int,
                        help=f"job status polling interval in seconds (minimum is {MIN_POLLING_INTERVAL} unless `-o` specified too)")
    parser.add_argument('-o', '--min-polling-override', action='store_true',
                        help=f'override minimum polling interval of {MIN_POLLING_INTERVAL} s')
    parser.add_argument('-n', '--defaultlogname', action='store_true', help='Use default log name instead of "batch_wait"')
    parser.add_argument('-v', '--version', action="version", version='%(prog)s ' + __version__)

    return parser


def batch_wait(args=None):
    """
    Wait for run completion and download files for all remote jobs.

    """
    # command line args
    parser = make_parser()
    args = parser.parse_args(args)

    # setup logging
    log_name = None if args.defaultlogname else "batch_wait"
    utils.setup_logging(log_name=log_name, log_file=args.logfile, log_level=args.loglevel, cli_extra=args.logextra)

    # report version
    logger = logging.getLogger(__name__)
    logger.info(f"Running rjm_batch_wait v{__version__}")

    # create the object for managing a batch of remote jobs
    rjb = RemoteJobBatch()
    rjb.setup(args.localjobdirfile)

    # wait for jobs to complete
    try:
        rjb.wait_and_download(polling_interval=args.pollingintervalsec, min_polling_override=args.min_polling_override)
    except BaseException as exc:
        # writing an stderr.txt file into the directory of unfinished jobs, for wfn
        rjb.write_stderr_for_unfinshed_jobs(traceback.format_exc())
        raise exc


if __name__ == "__main__":
    batch_wait()
