
import os
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
        "poll_interval": "1",
    }

    return config


@pytest.fixture
def runner(mocker, configobj):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    runner = funcx_slurm_runner.FuncxSlurmRunner()

    return runner


def mocked_run_function(function, *args, **kwargs):
    return function(*args, **kwargs)


def test_make_remote_directory_single(runner, tmpdir):
    runner.run_function = mocked_run_function
    remote_base_path = str(tmpdir)
    prefix = "my-remote-dir"

    full_path, basename = runner.make_remote_directory(remote_base_path, prefix)

    assert os.path.basename(full_path) == basename
    assert os.path.join(remote_base_path, basename) == full_path
    assert os.path.isdir(full_path)
    assert basename.startswith(prefix)


def test_make_remote_directory_list(runner, tmpdir):
    runner.run_function = mocked_run_function
    remote_base_path = str(tmpdir)
    prefixes = ["my-remote-dir", "another-remote-dir"]

    remote_dirs = runner.make_remote_directory(remote_base_path, prefixes)

    assert type(remote_dirs) is list
    assert len(remote_dirs) == len(prefixes)
    for prefix, (full_path, basename) in zip(prefixes, remote_dirs):
        assert os.path.basename(full_path) == basename
        assert os.path.join(remote_base_path, basename) == full_path
        assert os.path.isdir(full_path)
        assert basename.startswith(prefix)


def test_start_fail(runner, mocker):
    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.run_function',
        return_value=(1, "mocking failure")
    )
    with pytest.raises(RemoteJobRunnerError):
        runner.start()
    assert mocked.call_count == 1


def test_start_succeed(runner, mocker):
    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.run_function',
        return_value=(0, "Submitted batch job 1234567"),
    )

    started = runner.start()

    assert mocked.called_once()
    assert started is True
    assert runner._jobid == '1234567'


def test_wait_fail(runner, mocker):
    runner._jobid = '123456'
    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.run_function',
        return_value=(1, "mocking failure")
    )
    with pytest.raises(RemoteJobRunnerError):
        runner.wait()
    assert mocked.call_count == 1


def test_wait_succeed(runner, mocker):
    mocked_sleep = mocker.patch('time.sleep')
    runner._jobid = '123456'
    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.run_function',
        side_effect=[
            (0, "PENDING"),
            (0, "RUNNING"),
            (0, "COMPLETED"),
        ],
    )

    completed = runner.wait()

    assert mocked.call_count == 3
    assert completed is True
    assert mocked_sleep.call_count == 2