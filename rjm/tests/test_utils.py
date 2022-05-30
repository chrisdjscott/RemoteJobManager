
import os
import filecmp

from rjm import utils


def test_backup(tmp_path):
    fn = tmp_path / "file.txt"
    fn.write_text("test contents")

    bn = utils.backup_file(fn)

    assert os.path.exists(fn)
    assert os.path.exists(bn)
    assert filecmp.cmp(fn, bn)
