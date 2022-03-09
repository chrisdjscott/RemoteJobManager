
import configparser

import pytest

from rjm.remote_job import RemoteJob
from rjm.runners import funcx_slurm_batch_runner
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
    config["RETRY"] = {
        "delay": "1",
        "backoff": "1",
        "tries": "4",
    }
    config["FILES"] = {
        "uploads_file": "uploads.txt",
        "downloads_file": "downloads.txt",
    }
    config["GLOBUS"] = {
        "remote_endpoint": "qwerty",
        "remote_path": "asdfg",
    }

    return config


@pytest.fixture
def runner(mocker, configobj):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    runner = funcx_slurm_batch_runner.FuncxSlurmBatchRunner()

    return runner


def test_categorise_jobs(runner, configobj):
    remote_jobs = []
    rj = RemoteJob()  # started, completed, not downloaded
    rj._run_started = True
    rj._run_completed = True
    remote_jobs.append(rj)
    rj = RemoteJob()  # not started
    remote_jobs.append(rj)
    rj = RemoteJob()  # started, not completed
    rj._run_started = True
    rj._runner._jobid = '123456'
    remote_jobs.append(rj)

    unfin, undown, err = runner._categorise_jobs(remote_jobs)

    assert len(unfin) == 1
    assert '123456' in unfin
    assert unfin['123456'].get_runner().get_jobid() == '123456'

    assert len(undown) == 1
    assert type(undown[0]) is RemoteJob

    assert len(err) == 1

