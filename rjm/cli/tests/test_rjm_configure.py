
import os
import filecmp

import pytest

from rjm.cli import rjm_configure
from rjm import config as config_helper


def test_rjm_configure_exit_status(mocker):
    mocked = mocker.patch("rjm.config.do_configuration", side_effect=ValueError)
    with pytest.raises(SystemExit) as raised_exc:
        rjm_configure.configure()
    assert mocked.called_once()
    assert raised_exc.type == SystemExit
#    assert raised_exc.value.code == 1


@pytest.mark.parametrize('option', ('-e', '--export-config'))
def test_export_config(mocker, tmp_path, option):
    # create a dummy config file
    config_file = tmp_path / "test_config.ini"
    config_file.write_text('test contents')
    config_helper.CONFIG_FILE_LOCATION = str(config_file)

    # export it to a new location
    exported_config = tmp_path / "exported_config.ini"
    rjm_configure.configure([option, str(exported_config)])

    # check exported file is ok
    assert os.path.exists(exported_config)
    assert filecmp.cmp(config_file, exported_config)


@pytest.mark.parametrize('option', ('-i', '--import-config'))
def test_import_config_new(tmp_path, option):
    # create a dummy config file
    config_file = tmp_path / "test_config.ini"
    config_file.write_text('test contents')

    # import the config file
    config_dir = tmp_path / "config_dir"
    imported_config = config_dir / "imported_config.ini"
    config_helper.CONFIG_FILE_LOCATION = str(imported_config)
    rjm_configure.configure([option, str(config_file)])

    # check exported file is ok
    assert os.path.exists(imported_config)
    assert filecmp.cmp(config_file, imported_config)


@pytest.mark.parametrize('option', ('-i', '--import-config'))
def test_import_config_replace(mocker, tmp_path, option):
    # create a dummy config file
    config_file = tmp_path / "test_config.ini"
    config_file.write_text('test contents')

    # import the config file
    spy = mocker.spy(rjm_configure, 'backup_file')
    imported_config = tmp_path / "imported_config.ini"
    imported_config.write_text("something")
    config_helper.CONFIG_FILE_LOCATION = str(imported_config)
    rjm_configure.configure([option, str(config_file)])

    # check exported file is ok
    assert os.path.exists(imported_config)
    assert filecmp.cmp(config_file, imported_config)

    # check original was backed up
    assert spy.call_count == 1
    assert os.path.isfile(spy.spy_return)
    assert open(spy.spy_return).read() == "something"


def test_mutually_exclusive_args():
    args = ['-e', 'somefile', '-i', 'otherfile']
    with pytest.raises(SystemExit) as raised_exc:
        rjm_configure.configure(args)
    assert raised_exc.type == SystemExit
