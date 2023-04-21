
import os
import filecmp
import configparser

import pytest

from rjm import utils


def test_backup(tmp_path):
    fn = tmp_path / "file.txt"
    fn.write_text("test contents")

    bn = utils.backup_file(str(fn))

    assert os.path.exists(fn)
    assert os.path.exists(bn)
    assert filecmp.cmp(fn, bn)


@pytest.mark.parametrize("config_vals,expected_vals", [
    ([None, 7, 1, 11, 49], [utils.DEFAULT_RETRY_TRIES, utils.DEFAULT_RETRY_BACKOFF, utils.DEFAULT_RETRY_DELAY, utils.DEFAULT_RETRY_MAX_DELAY]),
    ([False, 7, 1, 11, 49], [utils.DEFAULT_RETRY_TRIES, utils.DEFAULT_RETRY_BACKOFF, utils.DEFAULT_RETRY_DELAY, utils.DEFAULT_RETRY_MAX_DELAY]),
    ([True, 7, 1, 11, 49], [7, 1, 11, 49]),
    ([True, 14, 1106, 131, 200], [14, 1106, 131, 200]),
    ([False, 17, 13, 1189, 3030], [utils.DEFAULT_RETRY_TRIES, utils.DEFAULT_RETRY_BACKOFF, utils.DEFAULT_RETRY_DELAY, utils.DEFAULT_RETRY_MAX_DELAY]),
])
def test_get_retry_values_from_config(config_vals, expected_vals):
    # setup config object
    config = configparser.ConfigParser()
    config["RETRY"] = {
        "tries": str(config_vals[1]),
        "backoff": str(config_vals[2]),
        "delay": str(config_vals[3]),
        "max_delay": str(config_vals[4])
    }
    if config_vals[0] is not None:
        config["RETRY"]["override_defaults"] = "1" if config_vals[0] else "0"

    # run function
    t, b, d, m = utils.get_retry_values_from_config(config)

    # check return values
    assert t == expected_vals[0]
    assert b == expected_vals[1]
    assert d == expected_vals[2]
