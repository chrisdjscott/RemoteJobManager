
import os
import configparser
import json

import pytest

from rjm.remote_job_batch import RemoteJobBatch
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
def rjb(mocker, configobj, tmpdir):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    rjb = RemoteJobBatch()
    rjb._timestamp = "timestamp"

    return rjb


@pytest.fixture
def rjb_setup(mocker, rjb, tmpdir):
    mocker.patch('rjm.remote_job.RemoteJob.setup')
    localdirsfile = tmpdir / "localdirs.txt"
    localdirsfile.write_text("testdir1" + os.linesep + "testdir2" + os.linesep)
    mocker.patch('rjm.remote_job_batch.RemoteJobBatch._read_jobs_file', return_value=["testdir1", "testdir2"])
    mocker.patch('rjm.runners.funcx_slurm_batch_runner.FuncxSlurmBatchRunner.get_globus_scopes')
    mocker.patch('rjm.runners.funcx_slurm_batch_runner.FuncxSlurmBatchRunner.setup_globus_auth')
    mocker.patch('rjm.utils.handle_globus_auth')
    rjb.setup(str(localdirsfile))

    return rjb


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

    assert mocked_make_dirs.called_once_with("/my/remote/path", ["mylocaldir", "pathdir"])
    assert mocked_set1.called_once_with("/my/remote/path/mylocaldir-timestamp", "mylocaldir-timestamp")
    assert mocked_set2.called_once_with("/my/remote/path/anotherlocaldir", "anotherlocaldir")


def test_categorise_jobs(rjb):
    remote_jobs = []
    rj = RemoteJob()  # not downloaded
    rj._label = "1"
    rj._uploaded = True
    rj._run_started = True
    rj._run_completed = True
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
    rj._run_completed = False
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
    rj._run_completed = True
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
