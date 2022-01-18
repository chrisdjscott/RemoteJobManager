
import os
import math
import configparser

from fair_research_login import NativeClient, JSONTokenStorage


# default file locations
CONFIG_FILE_LOCATION = os.path.expanduser("~/.rjm/rjm_config.ini")
TOKEN_FILE_LOCATION = os.path.expanduser("~/.rjm/rjm_tokens.json")

# Globus client id for this app
CLIENT_ID = "6ffc9c02-cf62-4268-a695-d9d100181962"

# some Globus auth scopes
SEARCH_SCOPE = "urn:globus:auth:scope:search.api.globus.org:all"
FUNCX_SCOPE = "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"
OPENID_SCOPE = "openid"
TRANSFER_SCOPE = "urn:globus:auth:scope:transfer.api.globus.org:all"
HTTPS_SCOPE = "https://auth.globus.org/scopes/{endpoint_id}/https"


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
    # TODO: detect if not already configured and exit with error

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
