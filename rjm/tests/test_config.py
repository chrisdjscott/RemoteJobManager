
import pytest

from rjm import config as config_helper


CONFIG_FILE_TEST = """[GLOBUS]
remote_endpoint = abcdefg
remote_path = /remote/path

[FUNCX]
remote_endpoint = abcdefg

[SLURM]
slurm_script = run.sl
poll_interval = 10

[FILES]
uploads_file = rjm_uploads.txt
downloads_file = rjm_downloads.txt
"""


@pytest.fixture
def config_file(tmp_path):
    c = tmp_path / "rjm_config.ini"
    c.write_text(CONFIG_FILE_TEST)
    return c


def test_load_config(config_file):
    config = config_helper.load_config(config_file=str(config_file))
    assert config.get("GLOBUS", "remote_endpoint") == "abcdefg"
    assert config.get("GLOBUS", "remote_path") == "/remote/path"
    assert config.get("FUNCX", "remote_endpoint") == "abcdefg"
    assert config.get("SLURM", "slurm_script") == "run.sl"
    assert config.getint("SLURM", "poll_interval") == 10
    assert config.get("FILES", "uploads_file") == "rjm_uploads.txt"
    assert config.get("FILES", "downloads_file") == "rjm_downloads.txt"


def test_load_config_exception(tmp_path):
    config_file = tmp_path / "does_not_exist.ini"
    with pytest.raises(ValueError):
        config_helper.load_config(config_file=str(config_file))
