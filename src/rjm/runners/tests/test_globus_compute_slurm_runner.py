
import os
import configparser

import pytest

from rjm.runners import globus_compute_slurm_runner
from rjm.runners.globus_compute_slurm_runner import MIN_POLLING_INTERVAL, MIN_WARMUP_POLLING_INTERVAL, MAX_WARMUP_DURATION
from rjm.errors import RemoteJobRunnerError


@pytest.fixture
def configobj():
    config = configparser.ConfigParser()
    config["FUNCX"] = {
        "remote_endpoint": "abcdefg",
    }
    config["SLURM"] = {
        "slurm_script": "run.sl",
        "poll_interval": "2",
        "warmup_poll_interval": "1",
        "warmup_duration": "3",
    }

    return config


@pytest.fixture
def runner(mocker, configobj):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    runner = globus_compute_slurm_runner.GlobusComputeSlurmRunner()

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
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.run_function',
        return_value=(1, "mocking failure")
    )
    with pytest.raises(RemoteJobRunnerError):
        runner.start("some_path")
    assert mocked.call_count == 1


def test_start_succeed(runner, mocker):
    mocked = mocker.patch(
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.run_function',
        return_value=(0, "Submitted batch job 1234567"),
    )

    started = runner.start("some/path")

    mocked.assert_called_once()
    assert started is True
    assert runner._jobid == '1234567'


def test_wait_fail(runner, mocker):
    runner._jobid = '123456'
    mocked = mocker.patch(
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.run_function',
        return_value=(None, "mocking failure")
    )
    with pytest.raises(RemoteJobRunnerError):
        runner.wait()
    assert mocked.call_count == 1


def test_wait_completed_success(runner, mocker):
    mocked_sleep = mocker.patch('time.sleep')
    runner._jobid = '123456'
    mocked = mocker.patch(
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.run_function',
        side_effect=[
            ({runner._jobid: "PENDING"}, "no msg"),
            ({runner._jobid: "RUNNING"}, "no msg"),
            ({runner._jobid: "COMPLETED"}, "no msg"),
        ],
    )

    completed = runner.wait()

    assert mocked.call_count == 3
    assert completed is True
    assert mocked_sleep.call_count == 2


def test_wait_completed_failed(runner, mocker):
    mocked_sleep = mocker.patch('time.sleep')
    runner._jobid = '123456'
    mocked = mocker.patch(
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.run_function',
        side_effect=[
            ({runner._jobid: "PENDING"}, "no msg"),
            ({runner._jobid: "RUNNING"}, "no msg"),
            ({runner._jobid: "FAILED"}, "no msg"),
        ],
    )

    success = runner.wait()

    assert mocked.call_count == 3
    assert success is False
    assert mocked_sleep.call_count == 2


def test_calculate_checksums(runner, tmpdir):
    text = """test file with some text"""
    expected = "337de094ee88f1bc965a97e1d6767f51a06fd1e6e679664625ff68546e3d2601"
    test_file = "testchecksum.txt"
    test_file_not_exist = "notexist.txt"
    with open(os.path.join(tmpdir, test_file), "w") as fh:
        fh.write(text)

    returncode, checksums = globus_compute_slurm_runner._calculate_checksums(
        [test_file, test_file_not_exist],
        str(tmpdir),
    )

    assert returncode == 0
    assert checksums[test_file] == expected
    assert checksums[test_file_not_exist] is None


#def test_run_function_timeout(runner, mocker):
#    class DummyFuture:
#        def result(self, timeout=None):
#            raise concurrent.futures.TimeoutError
#
#    class DummyExecutor:
#        def submit(self, *args, **kwargs):
#            return DummyFuture()
#
#    runner._funcx_executor = DummyExecutor()
#
#    mocked = mocker.patch(
#        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.reset_funcx_client',
#    )
#
#    with pytest.raises(concurrent.futures.TimeoutError):
#        runner.run_function(lambda x: print(x), "Hello, World!")
#
#    assert mocked.call_count == 1


def test_reset_globus_compute_client(configobj, mocker):
    class MockedClient:
        """dummy class"""

    class DummyFuture:
        def result(self, timeout=None):
            return "dummyresult"

    class MockedExecutor:
        """dummy class"""
        def shutdown(self, *args, **kwargs):
            pass

        def submit(self, *args, **kwargs):
            return DummyFuture()

    mocked_create_client = mocker.patch(
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner._create_globus_compute_client',
    )

    exec1 = MockedExecutor()
    exec1.id = 1
    exec2 = MockedExecutor()
    exec2.id = 2
    mocked_create_executor = mocker.patch(
        'rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner._create_globus_compute_executor',
        side_effect=[
            exec1,
            exec2,
        ],
    )

    # parent runner
    mocker.patch('rjm.config.load_config', return_value=configobj)
    runner = globus_compute_slurm_runner.GlobusComputeSlurmRunner()
    runner._setup_done = True
    runner._use_offprocess_checker = True
    runner.reset_globus_compute_client()

    assert mocked_create_executor.call_count == 1
    assert mocked_create_client.call_count == 1
    assert runner._executor.id == 1

    # child runner 1
    child1 = globus_compute_slurm_runner.GlobusComputeSlurmRunner()
    child1._setup_done = True
    child1._external_runner = runner
    child1.reset_globus_compute_client()

    assert mocked_create_executor.call_count == 1
    assert mocked_create_client.call_count == 1
    assert child1._executor.id == 1

    # child runner 2
    child2 = globus_compute_slurm_runner.GlobusComputeSlurmRunner()
    child2._setup_done = True
    child2._external_runner = runner
    child2.reset_globus_compute_client()

    assert mocked_create_executor.call_count == 1
    assert mocked_create_client.call_count == 1
    assert child2._executor.id == 1

    # reset on child 1 to test it propagates to child 2...
    child1.reset_globus_compute_client(propagate=True)

    assert mocked_create_executor.call_count == 2
    assert mocked_create_client.call_count == 2
    assert runner._executor.id == 2
    assert child1._executor.id == 2

    # child 2 should get the updated executor after it calls run_function...
    child2.run_function(None)
    assert child2._executor.id == 2


class MockedSubprocessReturn:
    def __init__(self, status, output):
        self.returncode = status
        self.stdout = output


def test_check_slurm_job_statuses_squeue(mocker):
    jobids = ["01234", "56789"]

    mocked = mocker.patch(
        'subprocess.run',
        side_effect=[
            MockedSubprocessReturn(0, "01234 COMPLETED\n56789 PENDING"),
        ],
    )

    status_dict, msg = globus_compute_slurm_runner._check_slurm_job_statuses(jobids)

    assert mocked.call_count == 1
    assert "01234" in status_dict
    assert status_dict["01234"] == "COMPLETED"
    assert "56789" in status_dict
    assert status_dict["56789"] == "PENDING"
    assert "sacct" not in "\n".join(msg)
    assert "Retrieved status after squeue" in "\n".join(msg)


def test_check_slurm_job_statuses_squeue_sacct(mocker):
    jobids = ["01234", "56789"]

    mocked = mocker.patch(
        'subprocess.run',
        side_effect=[
            MockedSubprocessReturn(0, "01234 PENDING"),
            MockedSubprocessReturn(0, "56789|COMPLETED\n"),
        ],
    )

    status_dict, msg = globus_compute_slurm_runner._check_slurm_job_statuses(jobids)

    assert mocked.call_count == 2
    assert "01234" in status_dict
    assert status_dict["01234"] == "PENDING"
    assert "56789" in status_dict
    assert status_dict["56789"] == "COMPLETED"
    assert "Retrieved status after sacct" in "\n".join(msg)
    assert "Retrieved status after squeue" in "\n".join(msg)


def test_check_slurm_job_statuses_sacct(mocker):
    jobids = ["01234", "56789"]

    mocked = mocker.patch(
        'subprocess.run',
        side_effect=[
            MockedSubprocessReturn(1, "error running squeue"),
            MockedSubprocessReturn(0, "56789|PENDING\n01234|RUNNING"),
        ],
    )

    status_dict, msg = globus_compute_slurm_runner._check_slurm_job_statuses(jobids)

    assert mocked.call_count == 2
    assert "01234" in status_dict
    assert status_dict["01234"] == "RUNNING"
    assert "56789" in status_dict
    assert status_dict["56789"] == "PENDING"
    assert "Retrieved status after sacct" in "\n".join(msg)
    assert "squeue failed with status" in "\n".join(msg)
    assert "error running squeue" in "\n".join(msg)


def test_check_slurm_job_statuses_failed(mocker):
    jobids = ["01234", "56789"]

    mocked = mocker.patch(
        'subprocess.run',
        side_effect=[
            MockedSubprocessReturn(1, "error running squeue"),
            MockedSubprocessReturn(1, "error running sacct"),
        ],
    )

    status_dict, msg = globus_compute_slurm_runner._check_slurm_job_statuses(jobids)

    assert mocked.call_count == 2
    assert status_dict is None
    assert "sacct failed with status" in "\n".join(msg)
    assert "error running sacct" in "\n".join(msg)
    assert "squeue failed with status" in "\n".join(msg)
    assert "error running squeue" in "\n".join(msg)


def test_check_slurm_job_statuses_missing(mocker):
    jobids = ["01234", "56789"]

    mocked = mocker.patch(
        'subprocess.run',
        side_effect=[
            MockedSubprocessReturn(0, ""),
            MockedSubprocessReturn(0, "56789|COMPLETED"),
        ],
    )

    status_dict, msg = globus_compute_slurm_runner._check_slurm_job_statuses(jobids)

    assert mocked.call_count == 2
    assert "56789" in status_dict
    assert status_dict["56789"] == "COMPLETED"
    assert "01234" not in status_dict


@pytest.mark.parametrize("config_vals,user_vals,expected_vals", [
    ([MIN_POLLING_INTERVAL, MIN_WARMUP_POLLING_INTERVAL, MAX_WARMUP_DURATION], [None, None, None], [MIN_POLLING_INTERVAL, MIN_WARMUP_POLLING_INTERVAL, MAX_WARMUP_DURATION]),
    ([MIN_POLLING_INTERVAL+10, MIN_WARMUP_POLLING_INTERVAL+10, MAX_WARMUP_DURATION-10], [None, None, None], [MIN_POLLING_INTERVAL+10, MIN_WARMUP_POLLING_INTERVAL+10, MAX_WARMUP_DURATION-10]),
    ([MIN_POLLING_INTERVAL-1, MIN_WARMUP_POLLING_INTERVAL-1, MAX_WARMUP_DURATION+1], [None, None, None], [MIN_POLLING_INTERVAL, MIN_WARMUP_POLLING_INTERVAL, MAX_WARMUP_DURATION]),
    ([MIN_POLLING_INTERVAL, MIN_WARMUP_POLLING_INTERVAL, MAX_WARMUP_DURATION], [MIN_POLLING_INTERVAL+10, MIN_WARMUP_POLLING_INTERVAL+10, MAX_WARMUP_DURATION-10], [MIN_POLLING_INTERVAL+10, MIN_WARMUP_POLLING_INTERVAL+10, MAX_WARMUP_DURATION-10]),
    ([MIN_POLLING_INTERVAL+10, MIN_WARMUP_POLLING_INTERVAL+10, MAX_WARMUP_DURATION-10], [MIN_POLLING_INTERVAL-10, MIN_WARMUP_POLLING_INTERVAL-10, MAX_WARMUP_DURATION+10], [MIN_POLLING_INTERVAL, MIN_WARMUP_POLLING_INTERVAL, MAX_WARMUP_DURATION]),
])
def test_get_poll_interval(config_vals, user_vals, expected_vals, mocker):
    config = configparser.ConfigParser()
    config["FUNCX"] = {
        "remote_endpoint": "abcdefg",
    }
    config["SLURM"] = {
        "slurm_script": "run.sl",
        "poll_interval": str(config_vals[0]),
        "warmup_poll_interval": str(config_vals[1]),
        "warmup_duration": str(config_vals[2]),
    }

    mocker.patch('rjm.config.load_config', return_value=config)
    runner = globus_compute_slurm_runner.GlobusComputeSlurmRunner()

    polling_interval, warmup_polling_interval, warmup_duration = runner.get_poll_interval(
        user_vals[0], user_vals[1], user_vals[2]
    )

    assert polling_interval == expected_vals[0]
    assert warmup_polling_interval == expected_vals[1]
    assert warmup_duration == expected_vals[2]
