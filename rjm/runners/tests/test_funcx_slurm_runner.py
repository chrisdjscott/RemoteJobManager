
import os
import configparser
import concurrent.futures

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
        runner.start("some_path")
    assert mocked.call_count == 1


def test_start_succeed(runner, mocker):
    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.run_function',
        return_value=(0, "Submitted batch job 1234567"),
    )

    started = runner.start("some/path")

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


def test_wait_completed_success(runner, mocker):
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


def test_wait_completed_failed(runner, mocker):
    mocked_sleep = mocker.patch('time.sleep')
    runner._jobid = '123456'
    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.run_function',
        side_effect=[
            (0, "PENDING"),
            (0, "RUNNING"),
            (0, "FAILED"),
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

    returncode, checksums = funcx_slurm_runner._calculate_checksums(
        [test_file, test_file_not_exist],
        str(tmpdir),
    )

    assert returncode == 0
    assert checksums[test_file] == expected
    assert checksums[test_file_not_exist] is None


def test_run_function_timeout(runner, mocker):
    class DummyFuture:
        def result(self, timeout=None):
            raise concurrent.futures.TimeoutError

    class DummyExecutor:
        def submit(self, *args, **kwargs):
            return DummyFuture()

    runner._funcx_executor = DummyExecutor()

    mocked = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.reset_funcx_client',
    )

    with pytest.raises(concurrent.futures.TimeoutError):
        runner.run_function(lambda x: print(x), "Hello, World!")

    assert mocked.call_count == 1


def test_reset_funcx_client(configobj, mocker):
    class MockedFuncXClient:
        """dummy class"""

    class DummyFuture:
        def result(self, timeout=None):
            return "dummyresult"

    class MockedFuncXExecutor:
        """dummy class"""
        def shutdown(self):
            pass

        def submit(self, *args, **kwargs):
            return DummyFuture()

    mocked_create_client = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner._create_funcx_client',
    )

    exec1 = MockedFuncXExecutor()
    exec1.id = 1
    exec2 = MockedFuncXExecutor()
    exec2.id = 2
    mocked_create_executor = mocker.patch(
        'rjm.runners.funcx_slurm_runner.FuncxSlurmRunner._create_funcx_executor',
        side_effect=[
            exec1,
            exec2,
        ],
    )

    # parent runner
    mocker.patch('rjm.config.load_config', return_value=configobj)
    runner = funcx_slurm_runner.FuncxSlurmRunner()
    runner._setup_done = True
    runner._use_offprocess_checker = True
    runner.reset_funcx_client()

    assert mocked_create_executor.call_count == 1
    assert mocked_create_client.call_count == 1
    assert runner._funcx_executor.id == 1

    # child runner 1
    child1 = funcx_slurm_runner.FuncxSlurmRunner()
    child1._setup_done = True
    child1._external_runner = runner
    child1.reset_funcx_client()

    assert mocked_create_executor.call_count == 1
    assert mocked_create_client.call_count == 1
    assert child1._funcx_executor.id == 1

    # child runner 2
    child2 = funcx_slurm_runner.FuncxSlurmRunner()
    child2._setup_done = True
    child2._external_runner = runner
    child2.reset_funcx_client()

    assert mocked_create_executor.call_count == 1
    assert mocked_create_client.call_count == 1
    assert child2._funcx_executor.id == 1

    # reset on child 1 to test it propagates to child 2...
    child1.reset_funcx_client(propagate=True)

    assert mocked_create_executor.call_count == 2
    assert mocked_create_client.call_count == 2
    assert runner._funcx_executor.id == 2
    assert child1._funcx_executor.id == 2

    # child 2 should get the updated executor after it calls run_function...
    child2.run_function(None)
    assert child2._funcx_executor.id == 2
