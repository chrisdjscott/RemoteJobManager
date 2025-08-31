
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

CONFIG_OPTIONS = [  # default values must be strings or None
    {
        "section": "COMPONENTS",
        "name": "runner",
        "default": "globus_compute_slurm_runner",
        "help": "Enter the runner implementation that should be used",
    },
    {
        "section": "COMPONENTS",
        "name": "transferer",
        "default": "globus_https_transferer",
        "help": "Enter the runner implementation that should be used",
    },
    {
        "section": "GLOBUS_TRANSFER",
        "name": "remote_endpoint",
        "default": None,
        "help": "Enter the endpoint id of the Globus guest collection on the remote machine",
    },
    {
        "section": "GLOBUS_TRANSFER",
        "name": "remote_path",
        "default": None,
        "help": "Enter the absolute path to the root of the Globus guest collection on the remote machine",
    },
    {
        "section": "GLOBUS_COMPUTE",
        "name": "remote_endpoint",
        "default": None,
        "help": "Enter the endpoint id of the Globus Compute endpoint running on the remote machine",
    },
    {
        "section": "SLURM",
        "name": "slurm_script",
        "default": "run.sl",
        "help": "Name of the Slurm script that will be included in the uploaded files",
    },
    {
        "section": "POLLING",
        "name": "warmup_poll_interval",
        "default": "10",
        "help": "Interval (in seconds) between checking whether the Slurm job has completed during the initial phase",
    },
    {
        "section": "POLLING",
        "name": "warmup_duration",
        "default": "120",
        "help": "Duration (in seconds) during which we apply the `warmup_poll_interval` before switching to `poll_interval`",
    },
    {
        "section": "POLLING",
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
    {
        "section": "PARAMIKO",
        "name": "private_key_file",
        "default": os.path.join(os.path.expanduser("~"), ".rjm", "paramiko_private_key"),
        "help": "Path to the file containing the private key that paramiko should use to connect to the remote system",
    },
    {
        "section": "PARAMIKO",
        "name": "remote_address",
        "default": None,
        "help": "Address of the remote machine",
    },
    {
        "section": "PARAMIKO",
        "name": "remote_user",
        "default": None,
        "help": "User to connect to the remote machine as",
    },
    {
        "section": "PARAMIKO",
        "name": "remote_base_path",
        "default": None,
        "help": "Base directory that paramiko should work under on the remote machine",
    },
    {
        "section": "PARAMIKO",
        "name": "job_script",
        "default": "run.sl",
        "help": "Name of the script to execute on the remote machine when starting a job",
    },
]


def load_config(config_file=CONFIG_FILE_LOCATION):
    """Load the config file and return the configparser object"""
    # load the config
    if os.path.exists(config_file):
        config = configparser.ConfigParser()
        config.read(config_file)

        # check if the config file is old and raise error if so
        old_format = False
        if not "COMPONENTS" in config:
            old_format = True
            logger.debug("Old format config file detected -- no COMPONENTS section -- defaulting to Globus")
            config["COMPONENTS"] = {
                "runner": "globus_compute_slurm_runner",
                "transferer": "globus_https_transferer",
            }

        if not "GLOBUS_TRANSFER" in config:
            old_format = True
            logger.debug("Old format config file detected -- no GLOBUS_TRANSFER section -- attempting to fix")
            if "GLOBUS" in config:
                logger.debug("Using GLOBUS config for GLOBUS_TRANSFER")
                config["GLOBUS_TRANSFER"] = config["GLOBUS"]

        if not "GLOBUS_COMPUTE" in config:
            old_format = True
            logger.debug("Old format config file detected -- no GLOBUS_COMPUTE section -- attempting to fix")
            if "GLOBUS_COMPUTE" in config:
                logger.debug("Using FUNCX config for GLOBUS_COMPUTE")
                config["GLOBUS_COMPUTE"] = config["FUNCX"]

        if not "POLLING" in config:
            old_format = True
            logger.debug("Old format config file detected -- no POLLING section -- attempting to fix")
            if "POLLING" in config:
                logger.debug("Using SLURM config for POLLING")
                config["POLLING"] = config["SLURM"]

        if old_format:
            logger.warning("Attempted to automatically update your old config file -- rerun `rjm_config` to avoid this")

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


def _process_option(config, optd):
    section = optd["section"]
    name = optd["name"]
    default = optd["default"]
    text = optd["help"]
    logger.debug(f"Processing option: {section}:{name} ({text}) : {default}")

    not_required = False
    if default is None:
        not_required = True

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

    # check we got a value
    if value is None and not not_required:
        raise RuntimeError(f"No value provided for '{section}:{name}' (ask={ask}; override={override})")

    # store
    if value is not None:
        if not config.has_section(section):
            config.add_section(section)
        config[section][name] = value


def do_configuration(config_options=CONFIG_OPTIONS):
    """
    Run through configuration steps

    :param config_options: optional, the list of config options, see config.CONFIG_OPTIONS
        for the expected format and defaults

    """
    logger.debug("Configuring RJM...")
    print("Configuring RJM...")

    # load config file if it already exists
    logger.debug("Load existing config or make a new config object")
    config = load_or_make_config()

    # loop over the options, asking user for input
    logger.debug("Processing config options")
    for optd in config_options:
        _process_option(config, optd)

    # store configuration
    with open(CONFIG_FILE_LOCATION, 'w') as cf:
        config.write(cf)

    logger.debug(f"Written config file to: {CONFIG_FILE_LOCATION}")
    print(f"Written config file to: {CONFIG_FILE_LOCATION}")
