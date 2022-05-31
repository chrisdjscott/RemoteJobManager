Getting started on NeSI
=======================

.. contents::
   :local:
   :backlinks: none

Install RJM
-----------

On Windows you could download the executables from the latest release
on the `GitHub repository`_.

Otherwise, install the *rjm* python package, for example using a virtual
environment and installing the *main* branch from the git repo:

.. code-block:: bash

    python -m venv venv
    source venv/bin/activate
    python -m pip install git+https://github.com/chrisdjscott/RemoteJobManager

.. _GitHub repository: https://github.com/chrisdjscott/RemoteJobManager/releases

Run the NeSI setup script (once)
--------------------------------

The NeSI setup script (either :code:`rjm_nesi_setup.exe` or
:code:`rjm_nesi_setup`) will do the following:

* configure and start a "funcX endpoint" on NeSI, which RJM will use to execute
  commands on NeSI
* setup a periodic task (via scrontab) to check that the funcX endpoint is
  running and restart it if it has stopped
* create a "Globus Guest Collection" in your NeSI nobackup directory, which will
  be used to transfer files to and from NeSI

When you run the setup script you will need to input values and authenticate
multiple times, which can't be avoided at this time unfortunately. In some cases
a browser window will open where you will need to do the authentication, in other
cases you will enter details directly at the terminal. You will
need to authenticate with NeSI (first and sector factor) and with Globus, which
can usually be done using your institutions credentials, e.g. University of
Auckland.

Running the script with the :code:`--config` option will write the settings to
the RJM config file:

.. code-block:: bash

   rjm_nesi_setup.exe --config

**Note:** you only need to run this script once. You can reuse the config created
in this step on multiple machines.

Transferring config to a different machine
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Export the config on the machine that you generated it on:

.. code-block:: bash

   rjm_configure.exe --export-config myrjmconfig.ini

This should write a file called *myrjmconfig.ini* into your current directory.
Copy this file to another machine and install it by running:

.. code-block:: bash

   rjm_configure.exe --import-config myrjmconfig.ini

You will still need to redo the authentication step below on the new machine.

Authenticate RJM
----------------

Run the :code:`rjm_authenticate.exe` executable to authenticate using the
configuration created in the previous step:

.. code-block:: bash

   rjm_authenticate.exe

This will open a browser window and ask you to authenticate with Globus.

You should only need to do this step once per machine because your credentials
will be cached and reused.

Run a simple test
-----------------

Health check script
~~~~~~~~~~~~~~~~~~~

The health check script tests that basic functionality is working by uploading
a file to NeSI and executing a simple command.

.. code-block:: bash

   rjm_health_check.exe

If should exit successfully with no errors if things are working.

Example simulation
~~~~~~~~~~~~~~~~~~

Clone the repository and run the example simulation using the steps below:

.. code-block:: bash

   git clone https://github.com/chrisdjscott/RemoteJobManager.git
   cd RemoteJobManager/examples/nonmem
   rjm_batch_submit.exe -f localdirs.txt -ll info
   rjm_batch_wait.exe -f localdirs.txt -ll info

**Note:** for the above to work you need to either make sure the RJM executables
are in your PATH or provide the full path to them. Remove the ".exe" suffix if
you are using the pip installed python package rather than the Windows
executables.

If the above worked there should be a file created at *files/output.zip*.

You should also notice the file *files/remote_job.json*. This file records the
progress of the remote job. If you try to run a simulation again in the same
directory, RJM should detect that it already ran before and won't repeat the
same steps again. You can override this behaviour by passing the :code:`--force`
option to :code:`rjm_batch_submit.exe` or delete the *remote_job.json* file.
