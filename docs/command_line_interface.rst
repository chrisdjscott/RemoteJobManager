Command line interface
======================

.. contents:: Available commands
   :local:
   :depth: 1
   :backlinks: none

All ``rjm_batch_*`` commands take ``-f <localjobdirfile>``: a text file
listing local job directories, one per line. Logging is controlled with
``-l`` (logfile), ``-ll`` (loglevel), and ``-le`` (per-logger overrides).
``--force`` ignores any prior state stored in ``remote_job.json``.

rjm_config
----------

.. automodule:: rjm.cli.rjm_config
   :noindex:

By default this configures the Globus stack. Pass ``-s`` / ``--ssh`` to
configure the experimental Paramiko (SSH/SFTP) backend instead, in which
case the Globus interactive flow and Globus authentication are skipped.

.. argparse::
   :module: rjm.cli.rjm_config
   :func: make_parser
   :prog: rjm_config
   :nodescription:

rjm_authenticate
----------------

Refresh or replace the cached Globus tokens at ``~/.rjm/rjm_tokens.json``.
Only relevant for the Globus backend.

.. argparse::
   :module: rjm.cli.rjm_authenticate
   :func: make_parser
   :prog: rjm_authenticate

rjm_health_check
----------------

Round-trip check against the configured remote: uploads a small file,
runs a simple command, and cleans up. Works against both backends.

.. argparse::
   :module: rjm.cli.rjm_health_check
   :func: make_parser
   :prog: rjm_health_check

rjm_batch_submit
----------------

Upload files listed in each ``rjm_uploads.txt`` and start the remote job
for every local job directory listed in ``-f``.

.. argparse::
   :module: rjm.cli.rjm_batch_submit
   :func: make_parser
   :prog: rjm_batch_submit

rjm_batch_wait
--------------

Wait for the remote jobs to finish and download the files listed in
``rjm_downloads.txt``.

.. argparse::
   :module: rjm.cli.rjm_batch_wait
   :func: make_parser
   :prog: rjm_batch_wait

rjm_batch_run
-------------

Convenience entry point that performs ``rjm_batch_submit`` followed by
``rjm_batch_wait`` in a single invocation.

.. argparse::
   :module: rjm.cli.rjm_batch_run
   :func: make_parser
   :prog: rjm_batch_run

rjm_batch_cancel
----------------

Cancel running jobs for the listed local job directories.

.. argparse::
   :module: rjm.cli.rjm_batch_cancel
   :func: make_parser
   :prog: rjm_batch_cancel
