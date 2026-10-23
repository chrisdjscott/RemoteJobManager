import configparser

import pytest

from rjm.cli import rjm_config


@pytest.fixture
def configobj():
    config = configparser.ConfigParser()
    config["PROJECT:uoa12345"] = {
        "remote_endpoint": "qwerty",
        "remote_path": "asdfg",
    }
    config["GLOBUS_COMPUTE"] = {
        "remote_endpoint": "abcdefg",
    }
    config["COMPONENTS"] = {
        "runner": "globus_compute_slurm_runner",
        "transferer": "globus_https_transferer",
    }
    return config


def test_parse_project_codes():
    assert rjm_config.parse_project_codes(" uoa00106, nesi12345 ,uoa00106,") == ["uoa00106", "nesi12345"]


@pytest.mark.parametrize("text", ["", " , ", "uoa00106, UOA12345", "uoa 00106", "00106"])
def test_parse_project_codes_invalid(text):
    with pytest.raises(ValueError):
        rjm_config.parse_project_codes(text)


def test_prompt_project_codes_retries_until_valid(mocker):
    mocker.patch("builtins.input", side_effect=["bad code", "uoa00106,nesi12345"])

    assert rjm_config.prompt_project_codes() == ["uoa00106", "nesi12345"]


def test_prompt_project_codes_default(mocker):
    mocker.patch("builtins.input", return_value="")

    assert rjm_config.prompt_project_codes() == [rjm_config.DEFAULT_PROJECT_CODE]


def test_nesi_setup_writes_projects(mocker, tmp_path):
    mocker.patch("rjm.config.CONFIG_FILE_LOCATION", str(tmp_path / "rjm_config.ini"))
    mocker.patch("builtins.input", side_effect=["user", "uoa00106,nesi12345"])
    mocked_nesi = mocker.patch("rjm.cli.rjm_config.NeSISetup")
    collections = {"uoa00106": ("ep1", "/path1"), "nesi12345": ("ep2", "/path2")}
    mocked_nesi.return_value.get_globus_transfer_config.return_value = collections
    mocked_nesi.return_value.get_globus_compute_config.return_value = "compute-ep"
    mocked_do_configuration = mocker.patch("rjm.config.do_configuration")
    mocked_auth = mocker.patch("rjm.cli.rjm_config.do_authentication")

    rjm_config.nesi_setup([])

    mocked_nesi.assert_called_once_with("user", ["uoa00106", "nesi12345"])
    mocked_nesi.return_value.setup_globus_transfer.assert_called_once()
    assert mocked_do_configuration.call_args.kwargs["projects"] == collections
    mocked_auth.assert_called_once_with(force=True, verbose=True)


def test_add_project(mocker, configobj):
    mocker.patch("rjm.config.load_config", return_value=configobj)
    mocker.patch("builtins.input", return_value="user")
    mocked_nesi = mocker.patch("rjm.cli.rjm_config.NeSISetup")
    mocked_nesi.return_value.get_globus_transfer_config.return_value = {"nesi12345": ("ep2", "/path2")}
    mocked_backup = mocker.patch("rjm.utils.backup_file")
    mocked_save = mocker.patch("rjm.config.save_config")
    mocked_auth = mocker.patch("rjm.cli.rjm_config.do_authentication")

    rjm_config.nesi_setup(["--add-project", "nesi12345"])

    mocked_nesi.assert_called_once_with("user", ["nesi12345"])
    mocked_backup.assert_called_once()
    saved_config = mocked_save.call_args.args[0]
    assert saved_config.get("PROJECT:nesi12345", "remote_endpoint") == "ep2"
    assert saved_config.get("PROJECT:nesi12345", "remote_path") == "/path2"
    assert saved_config.has_section("PROJECT:uoa12345")
    mocked_auth.assert_called_once_with(verbose=True)


def test_add_project_already_configured(mocker, configobj):
    mocker.patch("rjm.config.load_config", return_value=configobj)
    mocked_nesi = mocker.patch("rjm.cli.rjm_config.NeSISetup")
    mocked_save = mocker.patch("rjm.config.save_config")

    with pytest.raises(SystemExit, match="already configured"):
        rjm_config.nesi_setup(["--add-project", "uoa12345"])

    mocked_nesi.assert_not_called()
    mocked_save.assert_not_called()


def test_add_project_invalid_code(mocker, configobj):
    mocker.patch("rjm.config.load_config", return_value=configobj)
    mocked_nesi = mocker.patch("rjm.cli.rjm_config.NeSISetup")

    with pytest.raises(SystemExit, match="invalid NeSI project code"):
        rjm_config.nesi_setup(["--add-project", "uoa00106,nesi12345"])

    mocked_nesi.assert_not_called()


def test_add_project_paramiko(mocker, configobj):
    configobj["COMPONENTS"] = {
        "runner": "paramiko_ssh_runner",
        "transferer": "paramiko_sftp_transferer",
    }
    mocker.patch("rjm.config.load_config", return_value=configobj)

    with pytest.raises(SystemExit, match="only be added when RJM is configured to use Globus"):
        rjm_config.nesi_setup(["--add-project", "nesi12345"])


def test_add_project_and_ssh_exclusive():
    with pytest.raises(SystemExit):
        rjm_config.make_parser().parse_args(["--ssh", "--add-project", "nesi12345"])
