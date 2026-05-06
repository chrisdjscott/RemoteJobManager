Configuration
=============

Use the :code:`rjm_config` command to configure RemoteJobManager, which will walk
through the configuration options and write the configuration file.

By default :code:`rjm_config` configures the Globus stack (Globus Transfer +
Globus Compute). To configure the experimental SSH/SFTP (Paramiko) backend
instead, pass ``-s`` / ``--ssh``:

.. code-block:: bash

   rjm_config --ssh

The configuration file lives at :code:`~/.rjm/rjm_config.ini`. Its main
sections are:

* ``[COMPONENTS]`` selects the backend (``runner`` and ``transferer``).
* ``[GLOBUS_TRANSFER]`` and ``[GLOBUS_COMPUTE]`` hold the Globus endpoint ids
  and remote path for the Globus stack.
* ``[PARAMIKO]`` holds the SSH key path, remote address, remote user, remote
  base path, and job script for the SSH stack.
* ``[POLLING]`` controls how often RJM polls the remote for job state.
* ``[SLURM]`` sets the Slurm script filename (used by the Globus stack).
* ``[FILES]``, ``[RETRY]`` and ``[LOGGING]`` control upload/download lists,
  retry behaviour, and per-logger log levels.

Globus authentication tokens are cached at :code:`~/.rjm/rjm_tokens.json` and
are not used by the Paramiko backend.
