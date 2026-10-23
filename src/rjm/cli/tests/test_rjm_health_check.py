import configparser

import pytest

from rjm.cli import rjm_health_check
from rjm.errors import RemoteJobConfigError


@pytest.fixture(autouse=True)
def no_logging_setup(mocker):
    # the health check defaults to the critical log level, which would leak into other tests
    mocker.patch("rjm.utils.setup_logging")


@pytest.fixture
def configobj():
    config = configparser.ConfigParser()
    config["PROJECT:uoa12345"] = {
        "remote_endpoint": "qwerty",
        "remote_path": "asdfg",
    }
    config["PROJECT:nesi00001"] = {
        "remote_endpoint": "zxcvb",
        "remote_path": "hjkl",
    }
    config["GLOBUS_COMPUTE"] = {
        "remote_endpoint": "abcdefg",
    }
    config["COMPONENTS"] = {
        "runner": "globus_compute_slurm_runner",
        "transferer": "globus_https_transferer",
    }
    return config


def test_health_check_all_projects(mocker, configobj, capsys):
    mocker.patch("rjm.config.load_config", return_value=configobj)
    mocked_check = mocker.patch("rjm.cli.rjm_health_check._check_project")

    rjm_health_check.health_check([])

    assert [c.args[0] for c in mocked_check.call_args_list] == ["uoa12345", "nesi00001"]
    output = capsys.readouterr().out
    assert "uoa12345: OK" in output
    assert "nesi00001: OK" in output


def test_health_check_continues_after_failure(mocker, configobj, capsys):
    mocker.patch("rjm.config.load_config", return_value=configobj)
    mocked_check = mocker.patch(
        "rjm.cli.rjm_health_check._check_project",
        side_effect=[RuntimeError("upload failed"), None],
    )

    with pytest.raises(SystemExit) as excinfo:
        rjm_health_check.health_check([])

    assert excinfo.value.code == 1
    assert mocked_check.call_count == 2
    output = capsys.readouterr().out
    assert "uoa12345: FAILED" in output
    assert "nesi00001: OK" in output


def test_health_check_single_project(mocker, configobj):
    mocker.patch("rjm.config.load_config", return_value=configobj)
    mocked_check = mocker.patch("rjm.cli.rjm_health_check._check_project")

    rjm_health_check.health_check(["-p", "nesi00001"])

    mocked_check.assert_called_once()
    assert mocked_check.call_args.args[0] == "nesi00001"


def test_health_check_unknown_project(mocker, configobj):
    mocker.patch("rjm.config.load_config", return_value=configobj)
    mocked_check = mocker.patch("rjm.cli.rjm_health_check._check_project")

    with pytest.raises(RemoteJobConfigError, match="not configured"):
        rjm_health_check.health_check(["-p", "uoa99999"])

    mocked_check.assert_not_called()


def test_health_check_paramiko(mocker, configobj, capsys):
    configobj["COMPONENTS"] = {
        "runner": "paramiko_ssh_runner",
        "transferer": "paramiko_sftp_transferer",
    }
    mocker.patch("rjm.config.load_config", return_value=configobj)
    mocked_check = mocker.patch("rjm.cli.rjm_health_check._check_project")

    rjm_health_check.health_check([])

    mocked_check.assert_called_once()
    assert mocked_check.call_args.args[0] is None
    assert "Summary" not in capsys.readouterr().out


def test_check_project_uses_project(mocker):
    mocked_remote_job = mocker.patch("rjm.cli.rjm_health_check.RemoteJob")
    rj = mocked_remote_job.return_value
    rj.get_remote_directory.return_value = "/remote/health-check"
    rj.get_runner.return_value.run_function.return_value = None
    args = rjm_health_check.make_parser().parse_args([])

    rjm_health_check._check_project("nesi00001", args)

    mocked_remote_job.assert_called_once_with(project="nesi00001")
    rj.get_transferer.return_value.upload_files.assert_called_once()
    rj.get_runner.return_value.run_function.assert_called_once()
