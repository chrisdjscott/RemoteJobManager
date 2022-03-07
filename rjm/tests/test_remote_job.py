
import os
import configparser
import json

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
        "poll_interval": "1",
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


def test_run_wait_restarts_fail(rj, mocker):
    rj._run_started = True
    rj._runner._jobid = '1234567'
    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.run_function',
        return_value=(1, "mocking failure")
    )
    with pytest.raises(RemoteJobRunnerError):
        rj.run_wait()
    assert mocked.call_count == 4
    assert rj._run_completed is False


def test_run_wait_restarts_succeed(rj, mocker):
    rj._run_started = True
    rj._runner._jobid = '1234567'
    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.run_function',
        side_effect=[
            (1, "mocking failure"),
            (0, "RUNNING"),
            (0, "COMPLETED"),
        ],
    )

    rj.run_wait()

    assert mocked.call_count == 3
    assert rj._run_completed is True


def test_save_state(rj, tmpdir, mocker):
    rj._state_file = tmpdir / "test_state.json"
    rj._uploaded = True
    rj._run_started = False
    rj._run_completed = False
    rj._downloaded = True
    rj._cancelled = False
    runner_state = {"jobid": "12345"}
    transferer_state = {"something": "else"}

    mocked_transfer_save_state = mocker.patch(
        'rjm.transferers.globus_https_transferer.GlobusHttpsTransferer.save_state',
        return_value=transferer_state,
    )
    mocked_runner_save_state = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.save_state',
        return_value=runner_state,
    )

    rj._save_state()

    assert mocked_transfer_save_state.called_once()
    assert mocked_runner_save_state.called_once()
    assert os.path.exists(rj._state_file)

    with open(rj._state_file) as fh:
        state_dict = json.load(fh)
    assert rj._uploaded == state_dict["uploaded"]
    assert rj._run_started == state_dict["started_run"]
    assert rj._run_completed == state_dict["finished_run"]
    assert rj._downloaded == state_dict["downloaded"]
    assert rj._cancelled == state_dict["cancelled"]
    assert state_dict["runner"] == runner_state
    assert state_dict["transfer"] == transferer_state


def test_save_state_no_state_file(rj):
    rj._state_file = None
    rj._save_state()


@pytest.mark.parametrize("force", [(True,), (False,)])
def test_load_state(rj, mocker, tmpdir, force):
    state_file = tmpdir / "state.json"
    state_dict = {
        "uploaded": True,
        "started_run": True,
        "finished_run": False,
        "downloaded": False,
        "cancelled": False,
        "runner": {
            "jobid": "123456",
        },
        "transfer": {
            "some": "thing",
        },
    }
    with open(state_file, 'w') as fh:
        json.dump(state_dict, fh, indent=4)
    rj._state_file = state_file

    mocked_transfer_load_state = mocker.patch(
        'rjm.transferers.globus_https_transferer.GlobusHttpsTransferer.load_state',
        return_value=None,
    )
    mocked_runner_load_state = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.load_state',
        return_value=None,
    )

    rj._load_state(force)

    if force:
        assert mocked_transfer_load_state.call_count == 0
        assert mocked_runner_load_state.call_count == 0
        assert rj._uploaded is False
        assert rj._run_started is False
        assert rj._run_completed is False
        assert rj._downloaded is False
        assert rj._cancelled is False
    else:
        assert mocked_transfer_load_state.called_once_with(state_dict["transfer"])
        assert mocked_runner_load_state.called_once_with(state_dict["runner"])
        assert rj._uploaded == state_dict["uploaded"]
        assert rj._run_started == state_dict["started_run"]
        assert rj._run_completed == state_dict["finished_run"]
        assert rj._downloaded == state_dict["downloaded"]
        assert rj._cancelled == state_dict["cancelled"]
