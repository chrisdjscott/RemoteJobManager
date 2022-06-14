
import os

import pytest

from rjm.cli import rjm_batch_wait


def test_write_stderr_on_exception(mocker, tmp_path):
    localdir1 = tmp_path / "testdir1"
    localdir1.mkdir()
    (localdir1 / "uploads.txt").write_text("file2upload" + os.linesep)
    (localdir1 / "downloads.txt").write_text("file2download" + os.linesep)
    (localdir1 / "file2upload").write_text("test" + os.linesep)

    localdirsfile = tmp_path / "localdirs.txt"
    localdirsfile.write_text(str(localdir1) + os.linesep)

    mocker.patch('rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.get_globus_scopes')
    mocker.patch('rjm.runners.funcx_slurm_runner.FuncxSlurmRunner.setup_globus_auth')
    mocker.patch('rjm.transferers.globus_https_transferer.GlobusHttpsTransferer.get_globus_scopes')
    mocker.patch('rjm.transferers.globus_https_transferer.GlobusHttpsTransferer.setup_globus_auth')
    mocker.patch('rjm.utils.handle_globus_auth')
    mocker.patch('rjm.remote_job_batch.RemoteJobBatch.wait_and_download', side_effect=SystemExit("testing exit"))

    with pytest.raises(SystemExit):
        rjm_batch_wait.batch_wait(['-f', str(localdirsfile)])

    # check they were written
    assert (localdir1 / "stderr.txt").is_file()
    assert "testing exit" in (localdir1 / "stderr.txt").read_text()
