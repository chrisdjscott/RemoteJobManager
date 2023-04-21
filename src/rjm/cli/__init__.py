
import os
import logging


def read_local_dirs_file(dirsfile):
    logger = logging.getLogger(__name__)

    with open(dirsfile) as fh:
        local_dirs = fh.readlines()
    local_dirs = [d.strip() for d in local_dirs]

    # get list of local directories
    local_dirs_exist = []
    for local_dir in local_dirs:
        if os.path.isdir(local_dir):
            local_dirs_exist.append(local_dir)
        else:
            logger.warning(f'Local directory does not exist: "{local_dir}" (skipping)')

    return local_dirs_exist
