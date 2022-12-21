
import os
import configparser

import requests
import pytest
import responses
from responses import registries

from rjm.transferers.globus_https_transferer import GlobusHttpsTransferer
from rjm.errors import RemoteJobTransfererError


class AuthoriserMock:
    def get_authorization_header(self):
        pass


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
def tf(mocker, configobj):
    mocker.patch('rjm.config.load_config', return_value=configobj)
    tf = GlobusHttpsTransferer()

    return tf


@pytest.fixture
def uploads(tmpdir):
    num_files = 5
    files = []
    for fnum in range(num_files):
        fp = tmpdir / f"testfile{fnum}"
        files.append(str(fp))
        open(files[-1], "w").close()

    return files


def test_url_for_file(tf):
    tf._https_base_url = "https://my.base.url"
    tf._remote_path = "my/remote/path"
    url = tf._url_for_file("filename")
    assert url == "https://my.base.url/my/remote/path/filename"


#@responses.activate(registry=registries.OrderedRegistry)
@responses.activate()
def test_upload_files(tf, uploads, mocker):
    mocker.patch('time.sleep')
    tf._https_authoriser = AuthoriserMock()
    tf._https_base_url = "https://my.base.url"
    tf._remote_path = "my/remote/path"
    tf._local_path = os.path.dirname(uploads[0])
    tf._max_workers = 1
    urls = [tf._url_for_file(os.path.basename(fn)) for fn in uploads]
    responses.add(
        responses.PUT,
        urls[0],
        json={"msg": "not found"},
        status=403,
    )
    responses.add(
        responses.PUT,
        urls[0],
        json={"msg": "OK"},
        status=200,
    )
    responses.add(
        responses.PUT,
        urls[1],
        json={"msg": "OK"},
        status=200,
    )
    spy = mocker.spy(requests, 'put')

    tf.upload_files(uploads[:2])

    assert spy.call_count == 3  # should have tried twice, one failure then success


@responses.activate()
def test_upload_files_retries_fail(tf, uploads, mocker):
    mocker.patch('time.sleep')
    tf._https_authoriser = AuthoriserMock()
    tf._https_base_url = "https://my.base.url"
    tf._remote_path = "my/remote/path"
    tf._local_path = os.path.dirname(uploads[0])
    tf._max_workers = 1
    urls = [tf._url_for_file(os.path.basename(fn)) for fn in uploads]
    responses.add(
        responses.PUT,
        urls[0],
        json={"msg": "not found"},
        status=403,
    )
    responses.add(
        responses.PUT,
        urls[0],
        json={"msg": "some error"},
        status=404,
    )
    responses.add(
        responses.PUT,
        urls[0],
        json={"msg": "not found"},
        status=403,
    )
    responses.add(
        responses.PUT,
        urls[0],
        json={"msg": "some error"},
        status=404,
    )
    responses.add(
        responses.PUT,
        urls[1],
        json={"msg": "OK"},
        status=200,
    )
    spy = mocker.spy(requests, 'put')

    with pytest.raises(RemoteJobTransfererError):
        tf.upload_files(uploads[:2])

    assert spy.call_count == 5


def test_calculate_checksum(tf, tmpdir):
    text = """test file with some text"""
    expected = "337de094ee88f1bc965a97e1d6767f51a06fd1e6e679664625ff68546e3d2601"
    test_file = str(tmpdir / "testchecksum.txt")
    with open(test_file, "w") as fh:
        fh.write(text)

    checksum = tf._calculate_checksum(test_file)

    assert checksum == expected
