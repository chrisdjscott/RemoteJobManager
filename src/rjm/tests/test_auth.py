import configparser

import pytest

from rjm import auth
from rjm import utils


@pytest.fixture
def configobj():
    config = configparser.ConfigParser()
    config["PROJECT:uoa12345"] = {"remote_endpoint": "qwerty", "remote_path": "asdfg"}
    config["PROJECT:uoa00106"] = {"remote_endpoint": "zxcvb", "remote_path": "other"}
    config["GLOBUS_COMPUTE"] = {"remote_endpoint": "abcdefg"}
    config["COMPONENTS"] = {"runner": "globus_compute_slurm_runner", "transferer": "globus_https_transferer"}
    return config


def test_do_authentication_requests_all_project_scopes(mocker, configobj):
    mocker.patch('os.path.isfile', return_value=True)
    mocker.patch('rjm.config.load_config', return_value=configobj)
    mocked_auth = mocker.patch('rjm.utils.handle_globus_auth')

    auth.do_authentication()

    scopes = mocked_auth.call_args.args[0]
    assert utils.TRANSFER_SCOPE in scopes
    assert utils.HTTPS_SCOPE.format(endpoint_id="qwerty") in scopes
    assert utils.HTTPS_SCOPE.format(endpoint_id="zxcvb") in scopes


def test_do_authentication_paramiko_skipped(mocker, configobj):
    configobj["COMPONENTS"] = {"runner": "paramiko_ssh_runner", "transferer": "paramiko_sftp_transferer"}
    mocker.patch('os.path.isfile', return_value=True)
    mocker.patch('rjm.config.load_config', return_value=configobj)
    mocked_auth = mocker.patch('rjm.utils.handle_globus_auth')

    auth.do_authentication(verbose=True)

    mocked_auth.assert_not_called()
