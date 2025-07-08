
import os
import logging
import configparser

from rjm.errors import RemoteJobConfigError


logger = logging.getLogger(__name__)


# settings
CONFIG_FILE_LOCATION = os.path.join(
    os.path.expanduser("~"),
    ".rjm",
    "rjm_config.ini"
)
CONFIG_OPTIONS_REQUIRED = [
    {
        "section": "GLOBUS",
        "name": "remote_endpoint",
        "default": None,
        "help": "Enter the endpoint id of the Globus guest collection on the remote machine",
    },
    {
        "section": "GLOBUS",
        "name": "remote_path",
        "default": None,
        "help": "Enter the absolute path to the root of the Globus guest collection on the remote machine",
    },
    {
        "section": "FUNCX",
        "name": "remote_endpoint",
        "default": None,
        "help": "Enter the endpoint id of the Globus Compute endpoint running on the remote machine",
    },
]
CONFIG_OPTIONS_OPTIONAL = [  # default values must be strings
    {
        "section": "SLURM",
        "name": "slurm_script",
        "default": "run.sl",
        "help": "Name of the Slurm script that will be included in the uploaded files",
    },
    {
        "section": "SLURM",
        "name": "poll_interval",
        "default": "60",
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
        raise RemoteJobConfigError(f"Config file does not exist: {config_file}")

    return config


def load_or_make_config(config_file=CONFIG_FILE_LOCATION):
    """Load the config file if it exists or create an empty config object"""
    try:
        config = load_config(config_file=config_file)
        logger.debug(f"Loaded config from: {config_file}")

    except RemoteJobConfigError:
        if not os.path.isdir(os.path.dirname(CONFIG_FILE_LOCATION)):
            os.makedirs(os.path.dirname(CONFIG_FILE_LOCATION))

        # create empty config object
        config = configparser.ConfigParser()
        logger.debug("Created empty config object")

    return config


def _process_option(config, optd, ask=True):
    section = optd["section"]
    name = optd["name"]
    default = optd["default"]
    text = optd["help"]
    logger.debug(f"Processing option: {section}:{name} ({text}) : {default}")

    # current value if any
    try:
        value = config[section][name]
        logger.debug(f"Found existing config value: {value}")

        # special case - we want to make the default the minimum
        if section == "SLURM" and name == "poll_interval":
            if value < default:
                value = default
                logger.debug(f"Forcing default minimum value for SLURM:poll_interval: {value}")

    except KeyError:
        value = default
        logger.debug(f"Using default config value: {value}")

    # override selected value
    override = False
    if "override" in optd and optd["override"] is not None:
        value = optd["override"]
        override = True
        logger.debug(f"Overriding config value with: {value}")

    # user input
    if ask and not override:
        print()
        msg = f"{text} [{value if value is not None else ''}]: "
        new_value = input(msg).strip()
        while value is None and not len(new_value):
            new_value = input(msg).strip()
        if len(new_value):
            value = new_value
            logger.debug(f"Got new value from user input: {value}")

    # check we got a value
    if value is None:
        raise RuntimeError(f"No value provided for '{section}:{name}' (ask={ask}; override={override})")

    # store
    if not config.has_section(section):
        config.add_section(section)
    config[section][name] = value


def do_configuration(required_options=CONFIG_OPTIONS_REQUIRED,
                     optional_options=CONFIG_OPTIONS_OPTIONAL, accept_defaults=False):
    """
    Run through configuration steps

    :param required_options: optional, the list of required options, see config.CONFIG_OPTIONS_REQUIRED
        for the expected format and default
    :param optional_options: optional, the list of required options, see config.CONFIG_OPTIONS_OPTIONAL
        for the expected format and default
    :param accept_defaults: optional, if True then accept the default values
        without requesting confirmation, defaults to False

    """
    logger.debug("Configuring RJM...")
    print("Configuring RJM...")
    if not accept_defaults:
        print("Please enter configuration values below or accept the defaults (in square brackets)")

    # load config file if it already exists
    logger.debug("Load existing config or make a new config object")
    config = load_or_make_config()

    # loop over the options, asking user for input
    logger.debug("Processing required options")
    for optd in required_options:
        _process_option(config, optd)

    # do they want to configure the rest?
    print()
    use_defaults = "y" if accept_defaults else ""
    while use_defaults not in ("y", "n"):
        use_defaults = input("Do you wish to use default values for the remaining options (y/n)? ")
    if use_defaults == "y":
        ask = False
    else:
        ask = True

    logger.debug("Processing optional options")
    for optd in optional_options:
        _process_option(config, optd, ask=ask)

    # store configuration
    with open(CONFIG_FILE_LOCATION, 'w') as cf:
        config.write(cf)

    logger.debug(f"Written config file to: {CONFIG_FILE_LOCATION}")
    print(f"Written config file to: {CONFIG_FILE_LOCATION}")
