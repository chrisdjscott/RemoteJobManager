import configparser

import pytest

from rjm import config as config_helper
from rjm.errors import RemoteJobConfigError


CONFIG_FILE_TEST = """[PROJECT:uoa12345]
remote_endpoint = abcdefg
remote_path = /remote/path

[GLOBUS_COMPUTE]
remote_endpoint = abcdefg

[SLURM]
slurm_script = run.sl

[POLLING]
poll_interval = 10

[FILES]
uploads_file = rjm_uploads.txt
downloads_file = rjm_downloads.txt

[COMPONENTS]
runner = globus_compute_slurm_runner
transferer = globus_https_transferer
"""

CONFIG_FILE_LEGACY_TEST = """[GLOBUS_TRANSFER]
remote_endpoint = abcdefg
remote_path = /remote/path

[GLOBUS_COMPUTE]
remote_endpoint = abcdefg

[COMPONENTS]
runner = globus_compute_slurm_runner
transferer = globus_https_transferer
"""

CONFIG_FILE_PARAMIKO_TEST = """[COMPONENTS]
runner = paramiko_ssh_runner
transferer = paramiko_sftp_transferer

[PARAMIKO]
remote_address = remote.example.com
remote_user = user
remote_base_path = /remote
"""


@pytest.fixture
def config_file(tmp_path):
    c = tmp_path / "rjm_config.ini"
    c.write_text(CONFIG_FILE_TEST)
    return c


def make_config(projects):
    config = configparser.ConfigParser()
    for code in projects:
        config[f"{config_helper.PROJECT_SECTION_PREFIX}{code}"] = {
            "remote_endpoint": "abcdefg",
            "remote_path": f"/remote/{code}",
        }
    return config


def test_load_config(config_file):
    config = config_helper.load_config(config_file=str(config_file))
    assert config_helper.get_projects(config) == ["uoa12345"]
    assert config.get("PROJECT:uoa12345", "remote_endpoint") == "abcdefg"
    assert config.get("PROJECT:uoa12345", "remote_path") == "/remote/path"
    assert config.get("GLOBUS_COMPUTE", "remote_endpoint") == "abcdefg"
    assert config.get("SLURM", "slurm_script") == "run.sl"
    assert config.getint("POLLING", "poll_interval") == 10
    assert config.get("FILES", "uploads_file") == "rjm_uploads.txt"
    assert config.get("FILES", "downloads_file") == "rjm_downloads.txt"


def test_load_config_exception(tmp_path):
    config_file = tmp_path / "does_not_exist.ini"
    with pytest.raises(RemoteJobConfigError):
        config_helper.load_config(config_file=str(config_file))


def test_load_config_legacy(tmp_path):
    """A config with no [PROJECT:*] sections must be rejected with instructions to rerun rjm_config"""
    config_file = tmp_path / "rjm_config.ini"
    config_file.write_text(CONFIG_FILE_LEGACY_TEST)
    with pytest.raises(RemoteJobConfigError, match="rjm_config"):
        config_helper.load_config(config_file=str(config_file))


def test_load_config_paramiko(tmp_path):
    """The paramiko stack does not use project sections and must still load"""
    config_file = tmp_path / "rjm_config.ini"
    config_file.write_text(CONFIG_FILE_PARAMIKO_TEST)
    config = config_helper.load_config(config_file=str(config_file))
    assert config_helper.get_projects(config) == []


def test_get_projects():
    config = make_config(["uoa12345", "uoa00106"])
    assert config_helper.get_projects(config) == ["uoa12345", "uoa00106"]


def test_get_projects_empty():
    config = configparser.ConfigParser()
    assert config_helper.get_projects(config) == []


def test_resolve_project_explicit():
    config = make_config(["uoa12345", "uoa00106"])
    assert config_helper.resolve_project(config, "uoa00106") == "uoa00106"


def test_resolve_project_unknown():
    config = make_config(["uoa12345", "uoa00106"])
    with pytest.raises(RemoteJobConfigError, match="uoa12345, uoa00106"):
        config_helper.resolve_project(config, "uoa99999")


def test_resolve_project_single_default():
    config = make_config(["uoa12345"])
    assert config_helper.resolve_project(config, None) == "uoa12345"


def test_resolve_project_multiple_requires_selection():
    config = make_config(["uoa12345", "uoa00106"])
    with pytest.raises(RemoteJobConfigError, match="uoa12345, uoa00106"):
        config_helper.resolve_project(config, None)


def test_resolve_project_none_configured():
    config = configparser.ConfigParser()
    with pytest.raises(RemoteJobConfigError, match="No projects are configured"):
        config_helper.resolve_project(config, "uoa12345")
