
import os
import logging
from datetime import datetime

from . import utils
from .remote_job import RemoteJob


logger = logging.getLogger(__name__)


class RemoteJobManager:
    """
    Manages remote jobs.

    """
    def __init__(self, local_dirs):
        self._config = utils.load_config()

        # get a timestamp for identifying remote jobs
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

        # list of RemoteJob objects based on local directories
        self._remote_jobs = []
        for local_dir in local_dirs:
            if os.path.isdir(local_dir):
                self._remote_jobs.append(local_dir)
                self._remote_jobs.append(RemoteJob(local_dir, timestamp=timestamp))
            else:
                logger.warning(f'Local directory does not exist: "{local_dir}" (skipping)')
        logger.debug(f"Remote jobs: {self._remote_jobs}")

    def run(self):
        """Run all remote jobs from start to finish"""



if __name__ == "__main__":
    import sys

    utils.setup_logging()

    dirsfile = sys.argv[1]
    with open(dirsfile) as fh:
        local_dirs = fh.readlines()
    local_dirs = [d.strip() for d in local_dirs]
    print(">>>", local_dirs)

    rjm = RemoteJobManager(local_dirs)
    print(">>>", rjm)
