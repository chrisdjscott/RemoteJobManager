import configparser

import pytest

from rjm.cli import rjm_batch_submit


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


def test_batch_submit_passes_project(mocker, configobj, tmp_path):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    mocked_setup = mocker.patch('rjm.remote_job_batch.RemoteJobBatch.setup')
    mocker.patch('rjm.remote_job_batch.RemoteJobBatch.upload_and_start')

    dirsfile = tmp_path / "dirs.txt"
    dirsfile.write_text("")

    rjm_batch_submit.batch_submit(['-f', str(dirsfile), '-p', 'uoa00106'])

    mocked_setup.assert_called_once_with(str(dirsfile), force=False, project='uoa00106')


def test_batch_submit_default_project(mocker, configobj, tmp_path):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    mocked_setup = mocker.patch('rjm.remote_job_batch.RemoteJobBatch.setup')
    mocker.patch('rjm.remote_job_batch.RemoteJobBatch.upload_and_start')

    dirsfile = tmp_path / "dirs.txt"
    dirsfile.write_text("")

    rjm_batch_submit.batch_submit(['-f', str(dirsfile)])

    mocked_setup.assert_called_once_with(str(dirsfile), force=False, project=None)
