
import os
import logging
import configparser


logger = logging.getLogger(__name__)


# settings
CONFIG_FILE_LOCATION = os.path.expanduser("~/.rjm/rjm_config.ini")
CONFIG_OPTIONS = [
    {
        "section": "GLOBUS",
        "name": "remote_endpoint",
        "default": None,
        "help": "The endpoint id of the Globus guest collection on the remote machine",
    },
    {
        "section": "GLOBUS",
        "name": "remote_path",
        "default": None,
        "help": "Absolute path to the root of the Globus guest collection on the remote machine",
    },
    {
        "section": "FUNCX",
        "name": "remote_endpoint",
        "default": None,
        "help": "The endpoint id of the funcX endpoint running on the remote machine",
    },
    {
        "section": "SLURM",
        "name": "slurm_script",
        "default": "run.sl",
        "help": "Name of the Slurm script that will be included in the uploaded files",
    },
    {
        "section": "SLURM",
        "name": "poll_interval",
        "default": 10,
        "help": "Interval (in seconds) between checking whether the Slurm job has completed",
    },
    {
        "section": "FILES",
        "name": "uploads_file",
        "default": "rjm_uploads.txt",
        "help": "Name of the file in the local directory that lists files to be uploaded",
    },
    {
        "section": "FILES",
        "name": "downloads_file",
        "default": "rjm_downloads.txt",
        "help": "Name of the file in the local directory that lists files to be downloaded",
    },
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
    logger.info("Doing configuration...")

    # load config file if it already exists
    if os.path.isfile(CONFIG_FILE_LOCATION):
        logger.info(f"Loading current config: {CONFIG_FILE_LOCATION}")
        config = load_config()
    else:
        if not os.path.isdir(os.path.dirname(CONFIG_FILE_LOCATION)):
            logger.debug(f"Creating rjm directory: {os.path.dirname(CONFIG_FILE_LOCATION)}")
            os.makedirs(os.path.dirname(CONFIG_FILE_LOCATION))

        # create empty config object
        config = configparser.ConfigParser()

    logger.debug(f"Current config sections: {config.sections()}")

    # loop over the options, asking user for input
    for optd in CONFIG_OPTIONS:
        section = optd["section"]
        name = optd["name"]
        default = optd["default"]
        text = optd["help"]

        # current value if any
        try:
            logger.debug(f"looking for '{section}':'{name}' in config")
            value = config[section][name]
            logger.debug(f"  found '{value}'")
        except KeyError:
            value = default
            logger.debug(f"  not found, using default if any: {value}")

        # user input
        print()
        msg = f"{text} [{value if value is not None else ''}]: "
        new_value = input(msg).strip()
        while value is None and not len(new_value):
            new_value = input(msg).strip()
        if len(new_value):
            value = new_value

        # store
        logger.debug(f"Storing value for {section}:{name} = {value}")
        if not config.has_section(section):
            config.add_section(section)
        config[section][name] = value

    # store configuration
    logger.info(f"Writing config file to: {CONFIG_FILE_LOCATION}")
    logger.info("Check that file to adjust other options directly")
    with open(CONFIG_FILE_LOCATION, 'w') as cf:
        config.write(cf)
