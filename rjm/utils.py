
import os
import math
import logging
import configparser

from fair_research_login import NativeClient, JSONTokenStorage
from funcx.sdk.client import FuncXClient


# default file locations
CONFIG_FILE_LOCATION = os.path.expanduser("~/.rjm/rjm_config.ini")
TOKEN_FILE_LOCATION = os.path.expanduser("~/.rjm/rjm_tokens.json")

# Globus client id for this app
CLIENT_ID = "b7f9ff16-4094-4d2a-8183-6dfd9362096a"

# some Globus auth scopes
SEARCH_SCOPE = "urn:globus:auth:scope:search.api.globus.org:all"
FUNCX_SCOPE = FuncXClient.FUNCX_SCOPE
OPENID_SCOPE = "openid"
TRANSFER_SCOPE = "urn:globus:auth:scope:transfer.api.globus.org:all"
HTTPS_SCOPE = "https://auth.globus.org/scopes/{endpoint_id}/https"

# default logging levels
LOG_LEVEL_RJM = logging.INFO
LOG_LEVEL_OTHER = logging.WARNING


def setup_logging():
    # set the default levels
    logging.basicConfig(level=LOG_LEVEL_OTHER)
    logging.getLogger("rjm").setLevel(LOG_LEVEL_RJM)

    # check if specific levels are set in log file
    config = load_config()
    if "LOGGING" in config:
        for logger_name, level_name in config.items("LOGGING"):
            level = getattr(logging, level_name, None)
            if level is not None:
                logging.getLogger(logger_name).setLevel(level)

def load_config(config_file=CONFIG_FILE_LOCATION):
    """Load the config file and return the configparser object"""
    # load the config
    if os.path.exists(config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
    else:
        raise RuntimeError(f"Config file does not exist: {config_file}")

    return config


def handle_globus_auth(scopes, token_file=TOKEN_FILE_LOCATION):
    """Load the globus auth that should have already been configured"""
    # TODO: make open browser tab optional

    cli = NativeClient(
        client_id=CLIENT_ID,
        token_storage=JSONTokenStorage(token_file),  # save/load tokens here
        app_name="RemoteJobManager",
    )

    # get the requested scopes (load tokens from file if available, otherwise request new tokens)
    cli.login(refresh_tokens=True, requested_scopes=scopes)

    return cli


def pretty_size_from_bytes(size_bytes):
    """Scale size in bytes"""
    if size_bytes == 0:
        return 0, "B"
    size_name = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return s, size_name[i]
