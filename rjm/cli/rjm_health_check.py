
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


def _remote_health_check(remote_dir, remote_file):
    """Check the directory and file exist"""
    import os

    # test remote directory exists
    if not os.path.isdir(remote_dir):
        return f"Remote directory does not exist: '{remote_dir}'"

    # test uploaded file exists in remote directory
    full_path_file = os.path.join(remote_dir, remote_file)
    if not os.path.isfile(full_path_file):
        return f"Remote file does not exist: '{full_path_file}'"

    # everything worked if we got this far, so delete the remote file and directory
    os.unlink(full_path_file)
    os.rmdir(remote_dir)


def health_check():
    # command line arguments
    parser = make_parser()
    args = parser.parse_args()

    # setup logging
    utils.setup_logging(log_file=args.logfile, log_level=args.loglevel)

    print("Running RJM health check...")

    with tempfile.TemporaryDirectory() as tmpdir:
        # create remote job object
        rj = RemoteJob()
        rj.setup(tmpdir)
        t = rj.get_transferer()
        r = rj.get_runner()

        # use transferer to make a directory on the remote machine (tests funcx)
        prefix = f"health-check-{datetime.now().strftime('%Y%m%dT%H%M%S')}"
        print()
        print("Testing creation of unique remote directory...")
        rj.make_remote_directory(prefix=prefix, retries=False)
        remote_dir = rj.get_remote_directory()
        print(f'Created remote directory: "{remote_dir}"')

        # write file to local directory
        test_file_name = "test.txt"
        test_file_local = os.path.join(tmpdir, test_file_name)
        with open(test_file_local, "w") as fh:
            fh.write("Testing")

        # upload that file
        print()
        print("Testing uploading a file...")
        t.upload_files([test_file_local])
        print("Finished testing uploading a file")

        # use runner to check the directory and file exists (tests funcx)
        print()
        print("Using runner to check directory and file exist...")
        result = r.run_function(_remote_health_check, remote_dir, test_file_name)
        if result is None:
            print("Finished checking directory and file exist")
        else:
            raise RuntimeError("Error checking directory and file exist: {result}")

    print()
    print("If there were no errors above it looks like basic functionality is good")
    print()


if __name__ == "__main__":
    health_check()
