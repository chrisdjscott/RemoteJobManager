
import configparser

import pytest

from rjm.remote_job import RemoteJob
from rjm.errors import RemoteJobRunnerError


@pytest.fixture
def configobj():
    config = configparser.ConfigParser()
    config["GLOBUS"] = {
        "remote_endpoint": "qwerty",
        "remote_path": "asdfg",
    }
    config["FUNCX"] = {
        "remote_endpoint": "abcdefg",
    }
    config["SLURM"] = {
        "slurm_script": "run.sl",
        "poll_interval": "20",
    }
    config["RETRY"] = {
        "delay": "1",
        "backoff": "1",
        "tries": "4",
    }
    config["FILES"] = {
        "uploads_file": "uploads.txt",
        "downloads_file": "downloads.txt",
    }

    return config


@pytest.fixture
def rj(mocker, configobj):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    rj = RemoteJob()

    return rj


def test_run_start_restarts_fail(rj, mocker):
    rj._uploaded = True
    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.run_function',
        return_value=(1, "mocking failure")
    )
    with pytest.raises(RemoteJobRunnerError):
        rj.run_start()
    assert mocked.call_count == 4
    assert rj._run_started is False


def test_run_start_restarts_succeed(rj, mocker):
    rj._uploaded = True
    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.run_function',
        side_effect=[
            (1, "mocking failure"),
            (2, "mocking failure"),
            (0, "Submitted batch job 1234567"),
        ],
    )

    rj.run_start()

    assert mocked.call_count == 3
    assert rj._run_started is True
    assert rj._runner._jobid == '1234567'
