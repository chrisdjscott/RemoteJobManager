
import os
import configparser
import time

import pytest

from rjm import remote_job_batch
from rjm.remote_job_batch import RemoteJobBatch
from rjm.remote_job import RemoteJob


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
        "poll_interval": "2",
        "warmup_poll_interval": "1",
        "warmup_duration": "3",
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
def rjb(mocker, configobj):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    rjb = RemoteJobBatch()
    rjb._timestamp = "timestamp"

    return rjb


def test_write_stderr(mocker, rjb, tmp_path):
    localdir1 = tmp_path / "testdir1"
    localdir1.mkdir()
    (localdir1 / "uploads.txt").write_text("file2upload" + os.linesep)
    (localdir1 / "downloads.txt").write_text("file2download" + os.linesep)
    (localdir1 / "file2upload").write_text("test" + os.linesep)

    localdir2 = tmp_path / "testdir2"
    localdir2.mkdir()
    (localdir2 / "uploads.txt").write_text("file2upload" + os.linesep)
    (localdir2 / "downloads.txt").write_text("file2download" + os.linesep)
    (localdir2 / "file2upload").write_text("test" + os.linesep)

    localdir3 = tmp_path / "testdir3"
    localdir3.mkdir()
    (localdir3 / "uploads.txt").write_text("file2upload" + os.linesep)
    (localdir3 / "downloads.txt").write_text("file2download" + os.linesep)
    (localdir3 / "file2upload").write_text("test" + os.linesep)

    localdir4 = tmp_path / "testdir4"
    localdir4.mkdir()
    (localdir4 / "uploads.txt").write_text("file2upload" + os.linesep)
    (localdir4 / "downloads.txt").write_text("file2download" + os.linesep)
    (localdir4 / "file2upload").write_text("test" + os.linesep)
    (localdir4 / "stderr.txt").write_text("stderr already exists")

    localdirsfile = tmp_path / "localdirs.txt"
    localdirsfile.write_text(os.linesep.join([str(localdir1), str(localdir2), str(localdir3), str(localdir4)]) + os.linesep)

    mocker.patch('rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.get_globus_scopes')
    mocker.patch('rjm.runners.globus_compute_slurm_runner.GlobusComputeSlurmRunner.setup_globus_auth')
    mocker.patch('rjm.transferers.globus_https_transferer.GlobusHttpsTransferer.get_globus_scopes')
    mocker.patch('rjm.transferers.globus_https_transferer.GlobusHttpsTransferer.setup_globus_auth')
    mocker.patch('rjm.utils.handle_globus_auth')

    rjb.setup(str(localdirsfile))

    # both marked as not completed
    rjb._remote_jobs[0]._downloaded = False
    rjb._remote_jobs[1]._downloaded = False
    rjb._remote_jobs[2]._downloaded = True
    rjb._remote_jobs[3]._downloaded = False

    # run the stderr writing function
    rjb.write_stderr_for_unfinshed_jobs("testing stderr")

    # check they were written
    assert (localdir1 / "stderr.txt").is_file()
    assert "testing stderr" in (localdir1 / "stderr.txt").read_text()
    assert (localdir2 / "stderr.txt").is_file()
    assert "testing stderr" in (localdir2 / "stderr.txt").read_text()
    assert not (localdir3 / "stderr.txt").exists()
    assert (localdir4 / "stderr.txt").read_text() == "stderr already exists"


def test_make_directories(rjb, mocker):
    mocker.patch(
        'rjm.transferers.globus_https_transferer.GlobusHttpsTransferer.get_remote_base_directory',
        return_value="/my/remote/path",
    )

    rj1 = RemoteJob()
    mocker.patch.object(rj1, 'get_remote_directory', return_value=None)
    mocker.patch.object(rj1, 'get_local_dir', return_value="mylocaldir")
    mocked_set1 = mocker.patch.object(rj1, 'set_remote_directory')
    rj2 = RemoteJob()
    mocker.patch.object(rj2, 'get_remote_directory', return_value=None)
    mocker.patch.object(rj2, 'get_local_dir', return_value=os.path.join("multi", "pathdir"))
    mocked_set2 = mocker.patch.object(rj2, 'set_remote_directory')
    rjb._remote_jobs = [rj1, rj2]

    mocked_make_dirs = mocker.patch.object(
        rjb._runner,
        'make_remote_directory',
        return_value=[
            ("/my/remote/path/mylocaldir-timestamp", "mylocaldir-timestamp"),
            ("/my/remote/path/anotherlocaldir", "anotherlocaldir"),
        ],
    )

    rjb.make_directories()

    mocked_make_dirs.assert_called_once_with("/my/remote/path", ["mylocaldir-timestamp", "pathdir-timestamp"])
    mocked_set1.assert_called_once_with("/my/remote/path/mylocaldir-timestamp", "mylocaldir-timestamp")
    mocked_set2.assert_called_once_with("/my/remote/path/anotherlocaldir", "anotherlocaldir")


def test_categorise_jobs(rjb):
    remote_jobs = []
    rj = RemoteJob()  # not downloaded
    rj._label = "1"
    rj._uploaded = True
    rj._run_started = True
    rj._run_succeeded = True
    rj._downloaded = False
    remote_jobs.append(rj)
    rj = RemoteJob()  # not uploaded
    rj._label = "2"
    rj._uploaded = False
    remote_jobs.append(rj)
    rj = RemoteJob()  # not completed
    rj._label = "3"
    rj._uploaded = True
    rj._run_started = True
    rj._run_succeded = False
    rj._runner._jobid = '123456'
    remote_jobs.append(rj)
    rj = RemoteJob()  # not started
    rj._label = "4"
    rj._uploaded = True
    rj._run_started = False
    remote_jobs.append(rj)
    rj = RemoteJob()  # all done
    rj._label = "5"
    rj._uploaded = True
    rj._run_started = True
    rj._run_failed = True
    rj._downloaded = True
    remote_jobs.append(rj)
    rjb._remote_jobs = remote_jobs

    unup, unstart, unfin, undown = rjb._categorise_jobs()

    # unuploaded
    assert len(unup) == 1
    assert unup[0]._label == "2"

    # unstarted
    assert len(unstart) == 1
    assert unstart[0]._label == "4"

    # unfinished
    assert len(unfin) == 1
    assert unfin[0].get_runner().get_jobid() == '123456'
    assert unfin[0]._label == "3"

    # undownloaded
    assert len(undown) == 1
    assert undown[0]._label == "1"


@pytest.mark.parametrize("input_vals,current_time,expected_val", [
    ([10, 1, 20, 100.0], 110.0, 1),
    ([10, 1, 20, 100.0], 130.0, 10),
])
def test_calc_wait_time(input_vals, current_time, expected_val, mocker):
    mocker.patch('time.time', return_value=current_time)
    assert time.time() == current_time

    wait_time = remote_job_batch._calc_wait_time(input_vals[0], input_vals[1], input_vals[2], input_vals[3])

    assert wait_time == expected_val
