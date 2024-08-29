
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
        "override_defaults": "1",
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
    mocker.patch('time.sleep')
    rj._uploaded = True
    mocked = mocker.patch(
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.run_function',
        return_value=(1, "mocking failure")
    )
    with pytest.raises(RemoteJobRunnerError):
        rj.run_start()
    assert mocked.call_count == 4
    assert rj._run_started is False


def test_run_start_restarts_succeed(rj, mocker):
    mocker.patch('time.sleep')
    rj._uploaded = True
    mocked = mocker.patch(
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.run_function',
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
    mocker.patch('time.sleep')
    rj._run_started = True
    rj._runner._jobid = '1234567'
    mocked = mocker.patch(
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.run_function',
        return_value=(None, "mocking failure")
    )
    with pytest.raises(RemoteJobRunnerError):
        rj.run_wait()
    assert mocked.call_count == 4
    assert rj.run_completed() is False


def test_run_wait_restarts_succeed(rj, mocker):
    mocker.patch('time.sleep')
    rj._run_started = True
    rj._runner._jobid = '1234567'
    mocked = mocker.patch(
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.run_function',
        side_effect=[
            (None, "mocking failure"),
            ({rj._runner._jobid: "RUNNING"}, "no msg"),
            ({rj._runner._jobid: "COMPLETED"}, "no msg"),
        ],
    )

    rj.run_wait()

    assert mocked.call_count == 3
    assert rj.run_completed() is True
    assert rj._run_succeeded is True


def test_save_state(rj, tmpdir, mocker):
    rj._local_path = tmpdir
    rj._state_file = tmpdir / "test_state.json"
    rj._uploaded = True
    rj._run_started = False
    rj._run_succeeded = True
    rj._run_failed = False
    rj._downloaded = True
    rj._cancelled = False
    runner_state = {"jobid": "12345"}
    transferer_state = {"something": "else"}

    mocked_transfer_save_state = mocker.patch(
        'rjm.transferers.globus_https_transferer.GlobusHttpsTransferer.save_state',
        return_value=transferer_state,
    )
    mocked_runner_save_state = mocker.patch(
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.save_state',
        return_value=runner_state,
    )

    rj._save_state()

    mocked_transfer_save_state.assert_called_once()
    mocked_runner_save_state.assert_called_once()
    assert os.path.exists(rj._state_file)

    with open(rj._state_file) as fh:
        state_dict = json.load(fh)
    assert rj._uploaded == state_dict["uploaded"]
    assert rj._run_started == state_dict["run_started"]
    assert rj._run_succeeded == state_dict["run_succeeded"]
    assert rj._run_failed == state_dict["run_failed"]
    assert rj._downloaded == state_dict["downloaded"]
    assert rj._cancelled == state_dict["cancelled"]
    assert state_dict["runner"] == runner_state
    assert state_dict["transfer"] == transferer_state


def test_save_state_no_job_dir(rj, tmpdir, mocker):
    rj._local_path = tmpdir / "doesnotexist"
    rj._state_file = tmpdir / "doesnotexist" / "test_state.json"
    rj._uploaded = True
    rj._run_started = False
    rj._run_succeeded = True
    rj._run_failed = False
    rj._downloaded = True
    rj._cancelled = False
    runner_state = {"jobid": "12345"}
    transferer_state = {"something": "else"}

    mocked_transfer_save_state = mocker.patch(
        'rjm.transferers.globus_https_transferer.GlobusHttpsTransferer.save_state',
        return_value=transferer_state,
    )
    mocked_runner_save_state = mocker.patch(
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.save_state',
        return_value=runner_state,
    )

    rj._save_state()

    assert mocked_transfer_save_state.call_count == 0
    assert mocked_runner_save_state.call_count == 0
    assert not os.path.exists(rj._state_file)


def test_save_state_no_state_file(rj):
    rj._state_file = None
    rj._save_state()


@pytest.mark.parametrize("force", [(True,), (False,)])
def test_load_state(rj, mocker, tmpdir, force):
    state_file = tmpdir / "state.json"
    state_dict = {
        "uploaded": True,
        "run_started": True,
        "run_succeeded": False,
        "run_failed": True,
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
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.load_state',
        return_value=None,
    )

    rj._load_state(force)

    if force:
        assert mocked_transfer_load_state.call_count == 0
        assert mocked_runner_load_state.call_count == 0
        assert rj._uploaded is False
        assert rj._run_started is False
        assert rj._run_succeeded is False
        assert rj._run_failed is False
        assert rj._downloaded is False
        assert rj._cancelled is False
    else:
        mocked_transfer_load_state.assert_called_once_with(state_dict["transfer"])
        mocked_runner_load_state.assert_called_once_with(state_dict["runner"])
        assert rj._uploaded == state_dict["uploaded"]
        assert rj._run_started == state_dict["run_started"]
        assert rj._run_succeeded == state_dict["run_succeeded"]
        assert rj._run_failed == state_dict["run_failed"]
        assert rj._downloaded == state_dict["downloaded"]
        assert rj._cancelled == state_dict["cancelled"]


def test_write_stderr_not_needed(rj, tmpdir):
    rj._downloaded = True
    rj._local_path = tmpdir

    rj.write_stderr_if_not_finished("Hello, World!")

    # stderr.txt file should not be generated in this case
    assert not os.path.exists(tmpdir / "stderr.txt")


def test_write_stderr_needed(rj, tmpdir):
    msg = "Hello, World!"
    rj._downloaded = False
    rj._local_path = tmpdir
    stderr_file = tmpdir.join("stderr.txt")

    rj.write_stderr_if_not_finished(msg)

    # stderr.txt file should be generated in this case
    assert stderr_file.exists()
    assert stderr_file.read() == msg


def test_write_stderr_already_there(rj, tmpdir):
    msg = "Hello, World!"
    rj._downloaded = False
    rj._local_path = tmpdir
    stderr_file = tmpdir.join("stderr.txt")
    with stderr_file.open(mode="w") as f:
        f.write("content of stderr")

    rj.write_stderr_if_not_finished(msg)

    # stderr.txt file should have original contents (shouldn't be overwritten)
    assert stderr_file.exists()
    assert stderr_file.read() == "content of stderr"


def test_write_stderr_no_job_dir(rj, tmpdir):
    msg = "Hello, World!"
    rj._downloaded = False
    rj._local_path = tmpdir / "myjob"
    stderr_file = tmpdir.join("stderr.txt")

    assert not rj._local_path.exists()

    rj.write_stderr_if_not_finished(msg)

    # stderr.txt file should not exist and neither should job directory
    assert not stderr_file.exists()
    assert not rj._local_path.exists()
