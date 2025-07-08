"""
Script to upload input files, run jobs and download output files.

A file containing a list of local directories is passed as an argument. Each
directory should contain an *rjm_uploads.txt* file, with the list of files to be
uploaded, a *run.sl* Slurm script, which will be submitted to Slurm on the
remote machine, and an *rjm_downloads.txt* file, with the list of files to be
downloaded on completion of the Slurm job.

"""
import argparse
import logging
import traceback

from rjm.remote_job_batch import RemoteJobBatch
from rjm import utils
from rjm import __version__
from rjm.runners.globus_compute_slurm_runner import MIN_POLLING_INTERVAL, MIN_WARMUP_POLLING_INTERVAL, MAX_WARMUP_DURATION


logger = logging.getLogger(__name__)


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Upload files, run the job and download results")
    parser.add_argument('-f', '--localjobdirfile', required=True,
                        help="file that contains the names of the local job directories, one name per line")
    parser.add_argument('-l', '--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-ll', '--loglevel', required=False,
                        help="level of log verbosity (setting the level here overrides the config file)",
                        choices=['debug', 'info', 'warn', 'error', 'critical'])
    parser.add_argument('--force', action="store_true",
                        help="ignore progress from previous runs stored in job directory, i.e. start from scratch")
    parser.add_argument('-z', '--pollingintervalsec', type=int,
                        help=f"job status polling interval in seconds (minimum is {MIN_POLLING_INTERVAL})")
    parser.add_argument('-w', '--warmuppollingintervalsec', type=int,
                        help=f"job status polling interval in seconds during the warmup period (minimum is {MIN_WARMUP_POLLING_INTERVAL})")
    parser.add_argument('-d', '--warmupdurationsec', type=int,
                        help=f"Warmup period duration for job status polling (maximum is {MAX_WARMUP_DURATION})")
    parser.add_argument('-v', '--version', action="version", version='%(prog)s ' + __version__)

    return parser


def batch_run():
    """
    Upload files and start running for the given local directory

    """
    # command line args
    parser = make_parser()
    args = parser.parse_args()

    # setup logging
    utils.setup_logging(log_name="batch_run", log_file=args.logfile, log_level=args.loglevel)

    # report version
    logger = logging.getLogger(__name__)
    logger.info(f"Running rjm_batch_run v{__version__}")

    # create the object for managing a batch of remote jobs
    rjb = RemoteJobBatch()
    rjb.setup(args.localjobdirfile, force=args.force)

    # upload files and start
    rjb.upload_and_start()

    # wait for jobs to complete and download files
    try:
        rjb.wait_and_download(
            polling_interval=args.pollingintervalsec,
            warmup_polling_interval=args.warmuppollingintervalsec,
            warmup_duration=args.warmupdurationsec,
        )
    except BaseException as exc:
        # writing an stderr.txt file into the directory of unfinished jobs, for wfn
        rjb.write_stderr_for_unfinshed_jobs(traceback.format_exc())
        raise exc


if __name__ == "__main__":
    batch_run()
