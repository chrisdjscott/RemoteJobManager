
import os
import argparse
from datetime import datetime
import tempfile

from rjm import __version__
from rjm.remote_job import RemoteJob
from rjm import utils


def make_parser():
    """Return ArgumentParser"""
    parser = argparse.ArgumentParser(description="Perform basic checks of the interface to the remote machine")
    parser.add_argument('-l', '--logfile', help="logfile. if not specified, all messages will be printed to the terminal.")
    parser.add_argument('-ll', '--loglevel', default="critical",
                        help="level of log verbosity (default: %(default)s)",
                        choices=['debug', 'info', 'warn', 'error', 'critical'])
    parser.add_argument("-v", '--version', action='version', version='%(prog)s ' + __version__)

    return parser


def _remote_health_check(remote_root, remote_relpath, remote_file):
    """Check the directory and file exist"""
    import os.path

    full_path_dir = os.path.join(remote_root, remote_relpath)
    assert os.path.isdir(full_path_dir), f"Remote directory does not exist: '{full_path_dir}'"

    full_path_file = os.path.join(full_path_dir, remote_file)
    assert os.path.isfile(full_path_file), f"Remote file does not exist: '{full_path_file}'"


def health_check():
    # command line arguments
    parser = make_parser()
    args = parser.parse_args()

    # setup logging
    utils.setup_logging(log_file=args.logfile, log_level=args.loglevel)

    print("Running RJM health check...")

    # create remote job object
    rj = RemoteJob()
    rj.do_globus_auth()
    t = rj.get_transferer()
    r = rj.get_runner()

    # use transferer to make a directory on the remote machine (tests transfer client)
    prefix = f"health-check-{datetime.now().strftime('%Y%m%dT%H%M%S')}"
    print()
    print("Testing creation of unique remote directory...")
    remote_root, remote_relpath = t.make_unique_directory(prefix)
    print(f'Created remote directory "{remote_relpath}" in "{remote_root}"')

    # use transferer to upload a file to the directory (tests https upload)
    with tempfile.TemporaryDirectory() as tmpdir:
        t.set_local_directory(tmpdir)

        # write file to local directory
        with open(os.path.join(tmpdir, "test.txt"), "w") as fh:
            fh.write("Testing")

        # upload that file
        print()
        print("Testing uploading a file...")
        t.upload_files(["test.txt"])
        print("Finished testing uploading a file")

    # use runner to check the directory and file exists (tests funcx)
    print()
    print("Using runner to check directory and file exist...")
    r.run_function(_remote_health_check, remote_root, remote_relpath, "test.txt")
    print("Finished checking directory and file exist")

    # cleanup remote directory

    print()
    print("If there were no errors above it looks like basic functionality is good")
    print()


if __name__ == "__main__":
    health_check()
