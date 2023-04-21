
import os.path

import pytest

from rjm.transferers import transferer_base


@pytest.fixture
def transferer(mocker):
    mocker.patch('rjm.config.load_config', return_value=None)  # no config required
    transferer = transferer_base.TransfererBase()
    return transferer


def test_save_state_remote_path(transferer):
    transferer._remote_path = "some_path"
    state_dict = transferer.save_state()
    assert isinstance(state_dict, dict)
    assert "remote_path" in state_dict
    assert state_dict["remote_path"] == "some_path"


def test_save_state_no_remote_path(transferer):
    transferer._remote_path = None
    state_dict = transferer.save_state()
    assert isinstance(state_dict, dict)
    assert len(state_dict) == 0


@pytest.mark.parametrize("state_dict,expected", [
    ({"remote_path": "some_path"}, "some_path"),
    ({}, None),
    ({"unrecognised": "some_path"}, None),
    ({"unknown": "ignored", "remote_path": "some_path"}, "some_path"),
])
def test_load_state(transferer, state_dict, expected):
    transferer._remote_path = None
    transferer.load_state(state_dict)
    assert transferer._remote_path == expected


@pytest.mark.parametrize("local_dir,expected_label", [
    (os.path.join(os.path.expanduser("~"), "some", "path"), "[path] "),  # absolute path
    (os.path.join("some", "path", "relative"), "[relative] "),  # relative path with multiple components
    ("simplepath", "[simplepath] "),  # relative path with single component
])
def test_set_local_path(transferer, local_dir, expected_label):
    transferer.set_local_directory(local_dir)
    assert transferer._local_path == local_dir
    assert transferer._label == expected_label


def test_get_remote_directory(transferer):
    remote_base_path = os.path.join(os.path.expanduser("~"), "base", "path")
    remote_path = "testpath"
    transferer._remote_base_path = remote_base_path
    transferer._remote_path = remote_path
    remote_dir_tuple = transferer.get_remote_directory()
    assert remote_dir_tuple[0] == remote_base_path
    assert remote_dir_tuple[1] == remote_path

    transferer._remote_path = None
    remote_dir_tuple = transferer.get_remote_directory()
    assert remote_dir_tuple is None
