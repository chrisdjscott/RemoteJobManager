
import os
import sys

from rjm import utils
from rjm.remote_job import RemoteJob
from rjm import config as config_helper


def do_authentication(force=False, verbose=False):
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
        globus_scopes = rj.get_runner().get_globus_scopes()
        globus_scopes.extend(rj.get_transferer().get_globus_scopes())
    except Exception as exc:
        sys.stderr.write(f"ERROR: failed to create RemoteJob: {exc}" + os.linesep)
        sys.exit(1)

    # do the Globus authentication
    if verbose:
        print("===============================================================================")
        print("Requesting Globus authentication - will open link in web browser if required...")
        print("===============================================================================")
    # TODO: run this in separate thread with timeout and fail if not completed in time
    try:
        utils.handle_globus_auth(globus_scopes)
    except Exception as exc:
        sys.stderr.write(f"ERROR: failed to do Globus auth: {exc}" + os.linesep)
        sys.exit(1)
    else:
        if verbose:
            print("RJM authentication completed")
