
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
