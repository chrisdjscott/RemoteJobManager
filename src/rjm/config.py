
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

# per-project sections are named [PROJECT:<code>]
PROJECT_SECTION_PREFIX = "PROJECT:"

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
    if not os.path.exists(config_file):
        raise RemoteJobConfigError(f"Config file does not exist: {config_file}")

    config = configparser.ConfigParser()
    config.read(config_file)

    # the Globus stack needs at least one project section; the paramiko stack
    # is single-project and does not use them
    if not is_paramiko_stack(config) and not get_projects(config):
        raise RemoteJobConfigError(
            "No [PROJECT:<code>] sections found in the config file -- "
            "rerun `rjm_config` to configure the NeSI project code(s) to use"
        )

    return config


def is_paramiko_stack(config):
    """Return True when the config selects the paramiko runner and transferer"""
    runner = config.get("COMPONENTS", "runner", fallback="globus_compute_slurm_runner")
    transferer = config.get("COMPONENTS", "transferer", fallback="globus_https_transferer")
    return runner == "paramiko_ssh_runner" and transferer == "paramiko_sftp_transferer"


def get_project_sections(config):
    """Return the names of the [PROJECT:<code>] sections, in config file order"""
    return [section for section in config.sections() if section.startswith(PROJECT_SECTION_PREFIX)]


def get_projects(config):
    """
    Return the project codes configured in [PROJECT:<code>] sections, in config file order

    """
    return [section[len(PROJECT_SECTION_PREFIX):] for section in get_project_sections(config)]


def resolve_project(config, project):
    """
    Return the project code that a job should run under

    :param config: the loaded config object
    :param project: project code requested by the caller, or None to use the
        only configured project

    """
    projects = get_projects(config)
    if not projects:
        raise RemoteJobConfigError("No projects are configured -- rerun `rjm_config`")
    if project is None:
        if len(projects) == 1:
            return projects[0]
        raise RemoteJobConfigError(
            f"Multiple projects are configured, select one: {', '.join(projects)}"
        )
    if project not in projects:
        raise RemoteJobConfigError(
            f"Project '{project}' is not configured (configured projects: {', '.join(projects)})"
        )
    return project


def load_or_make_config(config_file=CONFIG_FILE_LOCATION):
    """
    Load the config file if it exists or create an empty config object

    The file is read without validation so that existing values are kept when
    reconfiguring from an outdated config file.

    """
    config = configparser.ConfigParser()
    if os.path.exists(config_file):
        config.read(config_file)
        logger.debug(f"Loaded config from: {config_file}")
    else:
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        logger.debug("Created empty config object")

    return config


def save_config(config, config_file=CONFIG_FILE_LOCATION):
    """Write the config object to the config file"""
    with open(config_file, 'w') as cf:
        config.write(cf)
    logger.debug(f"Written config file to: {config_file}")


def add_project(config, project, remote_endpoint, remote_path):
    """Add a [PROJECT:<code>] section for the given project's guest collection"""
    config[f"{PROJECT_SECTION_PREFIX}{project}"] = {
        "remote_endpoint": remote_endpoint,
        "remote_path": remote_path,
    }


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


def do_configuration(config_options=CONFIG_OPTIONS, projects=None, config_file=CONFIG_FILE_LOCATION):
    """
    Run through configuration steps

    :param config_options: optional, the list of config options, see config.CONFIG_OPTIONS
        for the expected format and defaults
    :param projects: optional, mapping of project code to (remote_endpoint, remote_path);
        when given, these replace any existing project sections
    :param config_file: optional, the config file to write

    """
    logger.debug("Configuring RJM...")
    print("Configuring RJM...")

    # load config file if it already exists
    logger.debug("Load existing config or make a new config object")
    config = load_or_make_config(config_file=config_file)

    # loop over the options, asking user for input
    logger.debug("Processing config options")
    for optd in config_options:
        _process_option(config, optd)

    # replace the project sections; [GLOBUS_TRANSFER] is superseded by them
    if projects is not None:
        for section in get_project_sections(config) + ["GLOBUS_TRANSFER"]:
            config.remove_section(section)
        for project, (remote_endpoint, remote_path) in projects.items():
            add_project(config, project, remote_endpoint, remote_path)

    # store configuration
    save_config(config, config_file=config_file)
    print(f"Written config file to: {config_file}")
