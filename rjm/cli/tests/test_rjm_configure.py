
import pytest

from rjm.cli import rjm_configure


def test_rjm_configure_exit_status(mocker):
    mocked = mocker.patch("rjm.config.do_configuration", side_effect=ValueError)
    with pytest.raises(SystemExit) as raised_exc:
        rjm_configure.configure()
    assert mocked.called_once()
    assert raised_exc.type == SystemExit
#    assert raised_exc.value.code == 1
