import os
import json
import configparser

import pytest

from rjm.cli import rjm_batch_cancel


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


def test_batch_cancel_uses_saved_project(mocker, configobj, tmp_path):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    mocked_remote_job = mocker.patch('rjm.cli.rjm_batch_cancel.RemoteJob')
    mocked_remote_job.STATE_FILE = "remote_job.json"

    localdir = tmp_path / "jobdir"
    localdir.mkdir()
    state = {
        "project": "uoa00106",
        "remote_directory": "/remote/dir",
        "remote_basename": "dir",
        "uploaded": True,
        "run_started": True,
        "run_succeeded": False,
        "run_failed": False,
        "downloaded": False,
        "cancelled": False,
    }
    (localdir / "remote_job.json").write_text(json.dumps(state))
    dirsfile = tmp_path / "dirs.txt"
    dirsfile.write_text(str(localdir) + os.linesep)

    rjm_batch_cancel.batch_cancel(['-f', str(dirsfile)])

    mocked_remote_job.assert_called_once_with(project='uoa00106')
    mocked_remote_job.return_value.setup.assert_called_once_with(str(localdir))
    mocked_remote_job.return_value.run_cancel.assert_called_once()


def test_batch_cancel_no_saved_project(mocker, configobj, tmp_path):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    mocked_remote_job = mocker.patch('rjm.cli.rjm_batch_cancel.RemoteJob')
    mocked_remote_job.STATE_FILE = "remote_job.json"

    localdir = tmp_path / "jobdir"
    localdir.mkdir()
    state = {
        "remote_directory": "/remote/dir",
        "remote_basename": "dir",
        "uploaded": True,
        "run_started": True,
        "run_succeeded": False,
        "run_failed": False,
        "downloaded": False,
        "cancelled": False,
    }
    (localdir / "remote_job.json").write_text(json.dumps(state))
    dirsfile = tmp_path / "dirs.txt"
    dirsfile.write_text(str(localdir) + os.linesep)

    rjm_batch_cancel.batch_cancel(['-f', str(dirsfile)])

    mocked_remote_job.assert_called_once_with(project=None)


def test_batch_cancel_skips_job_without_state(mocker, configobj, tmp_path):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    mocked_remote_job = mocker.patch('rjm.cli.rjm_batch_cancel.RemoteJob')
    mocked_remote_job.STATE_FILE = "remote_job.json"

    localdir = tmp_path / "jobdir"
    localdir.mkdir()
    dirsfile = tmp_path / "dirs.txt"
    dirsfile.write_text(str(localdir) + os.linesep)

    rjm_batch_cancel.batch_cancel(['-f', str(dirsfile)])

    mocked_remote_job.assert_not_called()
