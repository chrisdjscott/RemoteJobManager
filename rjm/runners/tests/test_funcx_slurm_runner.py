
import configparser

import pytest

from rjm.runners import funcx_slurm_runner
from rjm.errors import RemoteJobRunnerError


@pytest.fixture
def configobj():
    config = configparser.ConfigParser()
    config["FUNCX"] = {
        "remote_endpoint": "abcdefg",
    }
    config["SLURM"] = {
        "slurm_script": "run.sl",
        "poll_interval": "20",
    }

    return config


@pytest.fixture
def runner(mocker, configobj):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    runner = funcx_slurm_runner.FuncxSlurmRunner()

    return runner


def test_start_restarts_fail(runner, mocker):
    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.run_function',
        return_value=(1, "mocking failure")
    )
    with pytest.raises(RemoteJobRunnerError):
        runner.start()
    assert mocked.call_count == 1


def test_start_restarts_succeed(runner, mocker):
    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.run_function',
        return_value=(0, "Submitted batch job 1234567"),
    )

    started = runner.start()

    assert mocked.called_once()
    assert started is True
    assert runner._jobid == '1234567'
