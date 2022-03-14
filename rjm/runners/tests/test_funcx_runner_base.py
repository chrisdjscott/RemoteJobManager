
import os
import pytest

from rjm.runners import funcx_runner_base


@pytest.fixture
def runner():
    runner = funcx_runner_base.FuncxRunnerBase()

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
