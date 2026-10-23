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
* ``[PROJECT:<code>]`` (one per NeSI project) holds the endpoint id and
  remote path of that project's Globus guest collection. See
  `NeSI projects`_ below.
* ``[GLOBUS_COMPUTE]`` holds the Globus Compute endpoint id, which is shared
  by all projects.
* ``[PARAMIKO]`` holds the SSH key path, remote address, remote user, remote
  base path, and job script for the SSH stack.
* ``[POLLING]`` controls how often RJM polls the remote for job state.
* ``[SLURM]`` sets the Slurm script filename (used by the Globus stack).
* ``[FILES]``, ``[RETRY]`` and ``[LOGGING]`` control upload/download lists,
  retry behaviour, and per-logger log levels.

Globus authentication tokens are cached at :code:`~/.rjm/rjm_tokens.json` and
are not used by the Paramiko backend.

NeSI projects
-------------

With the Globus stack, RJM can be configured for several NeSI projects, for
example when data from different research drives must stay in the matching
NeSI project. :code:`rjm_config` asks for a comma separated list of project
codes and creates a Globus guest collection for each one, by default in
``/nesi/nobackup/<code>/<username>/rjm-jobs``. Each project gets its own
section in ``rjm_config.ini``:

.. code-block:: ini

   [PROJECT:uoa00106]
   remote_endpoint = <guest collection endpoint id>
   remote_path = /nesi/nobackup/uoa00106/<username>/rjm-jobs

   [PROJECT:uoa00999]
   remote_endpoint = <guest collection endpoint id>
   remote_path = /nesi/nobackup/uoa00999/<username>/rjm-jobs

To add a project to an existing configuration without recreating the others,
run:

.. code-block:: bash

   rjm_config --add-project uoa00999

This creates the guest collection for the new project, backs up the config
file, adds the ``[PROJECT:<code>]`` section and runs authentication again if
the cached tokens do not cover the new collection.

Selecting the project for a batch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The project is chosen when the jobs are submitted, with ``-p`` /
``--projectcode`` on :code:`rjm_batch_submit` or :code:`rjm_batch_run`:

.. code-block:: bash

   rjm_batch_submit -f localdirs.txt -p uoa00999

* If only one project is configured, ``-p`` can be omitted and that project
  is used.
* If several projects are configured, ``-p`` is required; RJM stops with an
  error listing the configured projects otherwise.
* All jobs in a batch use the same project.

The project is saved in each job's ``remote_job.json``. :code:`rjm_batch_wait`
and :code:`rjm_batch_cancel` read it from there, so they do not take ``-p``.
Resubmitting a job under a different project requires ``--force``.

Slurm account
~~~~~~~~~~~~~

The Globus Slurm runner always passes the project code to ``sbatch`` as
``--account=<code>``, so jobs are charged to the selected project. Options on
the ``sbatch`` command line take precedence over ``#SBATCH`` directives, so an
account set in the Slurm script (``#SBATCH --account=<value>`` or
``#SBATCH -A <value>``) is overridden. RJM logs a warning when the script's
account differs from the project.

Upgrading from a single project configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Configuration files written by earlier versions of RJM (with a
``[GLOBUS_TRANSFER]`` section and no ``[PROJECT:<code>]`` sections) are not
accepted. Rerun :code:`rjm_config` once to create the project sections; other
settings in the existing file, such as ``[RETRY]`` and ``[LOGGING]``, are kept.
