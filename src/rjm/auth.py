
import os
import sys
import logging

from rjm import utils
from rjm.remote_job import RemoteJob
from rjm import config as config_helper


logger = logging.getLogger(__name__)


def do_authentication(force=False, verbose=False, retry=True):
    """
    Run through the steps to authenticate RJM

    """
    # check config file exists (configure should have been done already)
    if not os.path.isfile(config_helper.CONFIG_FILE_LOCATION):
        sys.stderr.write("ERROR: configuration file must be created with rjm_configure before running rjm_authenticate" + os.linesep)
        sys.exit(1)

    # delete token file if exists
    if force:
        if os.path.isfile(utils.TOKEN_FILE_LOCATION):
            if verbose:
                print(f"Deleting existing token file to force reauthentication: {utils.TOKEN_FILE_LOCATION}")
            os.unlink(utils.TOKEN_FILE_LOCATION)

    # create the remote job object and get Globus scopes
    try:
        rj = RemoteJob()
    except Exception as exc:
        logger.exception("Failed to create RemoteJob")
        sys.stderr.write(f"ERROR: failed to create RemoteJob: {exc}" + os.linesep)
        sys.exit(1)

    if verbose:
        print("===============================================================================")
        print("Requesting Globus authentication - will open link in web browser if required...")
        print("===============================================================================")

    # TODO: run this in separate thread with timeout and fail if not completed in time
    try:
        rj.do_globus_auth()
    except Exception as exc:
        if retry and os.path.isfile(utils.TOKEN_FILE_LOCATION):
            logger.exception("Authentication failed, retrying")
            print("===========================================================")
            print("Authentication failed; attempting to force reauthentication")
            print("===========================================================")
            do_authentication(force=True, verbose=verbose, retry=False)
        else:
            logger.exception("Failed to do Globus auth")
            sys.stderr.write(f"ERROR: failed to do Globus auth: {exc}" + os.linesep)
            sys.exit(1)

    if verbose:
        print("RJM authentication completed")
