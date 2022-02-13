
import os
import logging
import configparser


logger = logging.getLogger(__name__)


# settings
CONFIG_FILE_LOCATION = os.path.expanduser("~/.rjm/rjm_config.ini")
CONFIG_OPTIONS = [
    {
        "section": "GLOBUS",
        "name": "endpoint_id",
        "default": None,
        "help": "",
    }
]


def load_config(config_file=CONFIG_FILE_LOCATION):
    """Load the config file and return the configparser object"""
    # load the config
    if os.path.exists(config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
    else:
        raise RuntimeError(f"Config file does not exist: {config_file}")

    return config


def do_configuration():
    """Run through configuration steps"""
    # load config file if it already exists
    if os.path.isfile(CONFIG_FILE_LOCATION):
        logger.debug(f"Loading current config: {CONFIG_FILE_LOCATION}")
        config = load_config()
    else:
        if not os.path.isdir(os.path.dirname(CONFIG_FILE_LOCATION)):
            logger.debug(f"Creating rjm directory: {os.path.dirname(CONFIG_FILE_LOCATION)}")
            os.makedirs(os.path.dirname(CONFIG_FILE_LOCATION))

        # create empty config object
        config = config.ConfigParser()

    # Globus options


    # funcx options


    # ...
