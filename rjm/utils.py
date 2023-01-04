
import os
import math
import logging
import shutil
from datetime import datetime

from fair_research_login import NativeClient, JSONTokenStorage

from rjm import config as config_helper


# default file locations
TOKEN_FILE_LOCATION = os.path.join(
    os.path.expanduser("~"),
    ".rjm",
    "rjm_tokens.json"
)

# Globus client id for this app
CLIENT_ID = "b7f9ff16-4094-4d2a-8183-6dfd9362096a"

# some Globus auth scopes
SEARCH_SCOPE = "urn:globus:auth:scope:search.api.globus.org:all"
OPENID_SCOPE = "openid"
TRANSFER_SCOPE = "urn:globus:auth:scope:transfer.api.globus.org:all"
HTTPS_SCOPE = "https://auth.globus.org/scopes/{endpoint_id}/https"

# default logging levels
LOG_LEVEL_RJM = logging.INFO
LOG_LEVEL_OTHER = logging.WARNING

# defaults for retries
DEFAULT_RETRY_TRIES = 12  # number of times to retry
DEFAULT_RETRY_BACKOFF = 2  # factor to increase delay by each time
DEFAULT_RETRY_DELAY = 5  # initial delay

logger = logging.getLogger(__name__)


def setup_logging(log_name=None, log_file=None, log_level=None, cli_extra=False):
    # name
    if log_name is None:
        log_name = "%(name)s"

    # set the default levels
    logging.basicConfig(
        level=LOG_LEVEL_OTHER,
        filename=log_file,
        format=f'%(asctime)s|{log_name}|%(levelname)s|%(message)s',
    )
    logging.getLogger("rjm").setLevel(LOG_LEVEL_RJM)

    # check if specific levels are set in log file
    if os.path.exists(config_helper.CONFIG_FILE_LOCATION):
        config = config_helper.load_config()
        if "LOGGING" in config:
            for logger_name, level_name in config.items("LOGGING"):
                level = getattr(logging, level_name, None)
                if level is not None:
                    logging.getLogger(logger_name).setLevel(level)

    # command line overrides rjm log level
    if log_level is not None:
        if log_level == "warn":  # "warn" is deprecated
            log_level = "warning"
        level = getattr(logging, log_level.upper(), None)
        if level is not None:
            logging.getLogger("rjm").setLevel(level)
            if cli_extra:
                # same level for globus and funcx
                logging.getLogger("globus").setLevel(level)
                logging.getLogger("funcx").setLevel(level)


def handle_globus_auth(scopes, token_file=TOKEN_FILE_LOCATION,
                       client_id=CLIENT_ID, name="RemoteJobManager"):
    """Load the globus auth that should have already been configured"""
    # TODO: make open browser tab optional

    cli = NativeClient(
        client_id=client_id,
        token_storage=JSONTokenStorage(token_file),  # save/load tokens here
        app_name=name,
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


def backup_file(file_path):
    """Backup the given file"""
    if os.path.exists(file_path):
        now = datetime.now().strftime("%Y%m%dT%H%M%S")
        bkp_file = file_path + f'-{now}'
        bkp_file_base = bkp_file
        count = 1
        while os.path.exists(bkp_file):
            bkp_file = f"{bkp_file_base}-{count}"
            count += 1
        shutil.copy(file_path, bkp_file)

        return bkp_file


def get_retry_values_from_config(config):
    """
    Return retry values (tries, backoff and delay)

    """
    use_config_vals = config.getboolean("RETRY", "override_defaults", fallback=False)

    if use_config_vals:
        retry_tries = config.getint("RETRY", "tries", fallback=DEFAULT_RETRY_TRIES)
        retry_backoff = config.getint("RETRY", "backoff", fallback=DEFAULT_RETRY_BACKOFF)
        retry_delay = config.getint("RETRY", "delay", fallback=DEFAULT_RETRY_DELAY)
        logger.debug(f"Using retry values from config: tries={retry_tries}, backoff={retry_backoff}, delay={retry_delay}")
    else:
        retry_tries = DEFAULT_RETRY_TRIES
        retry_backoff = DEFAULT_RETRY_BACKOFF
        retry_delay = DEFAULT_RETRY_DELAY
        logger.debug(f"Using default retry values: tries={retry_tries}, backoff={retry_backoff}, delay={retry_delay}")

    # report how long we will wait for
    total = 0
    current = retry_delay
    for i in range(retry_tries):
        total += current
        current *= retry_backoff
    logger.debug(f"Will retry for a total of {total} seconds")

    return retry_tries, retry_backoff, retry_delay
