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

Prerequisites for running the setup script
------------------------------------------

In order to proceed with the setup you will need the following:

* NeSI credentials (username, first and second factor passwords)
* NeSI project code that you belong to (e.g. uoa00106)
* Globus account (https://app.globus.org/)

  - You can usually sign up with your institutional credentials (e.g.
    *The University of Auckland*) for convenience but it is not essential

**Note:** during the setup you may be asked to authenticate multiple times and
in some cases a browser window may be opened automatically for you to
authenticate in and in other cases you may need to copy a link to a browser
manually and authenticate there. Please follow the instructions that show up
when running the setup script.

Linking Globus and NeSI accounts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You may be asked for a linked identity with *NeSI Keycloak*.
If you already have a linked identity it should appear in the list like
*<username>@iam.nesi.org.nz* (where *<username>* is your NeSI username).
Otherwise, you can follow the instructions to *Link an identity from NeSI
Keycloak*.

Run the NeSI setup script (once per machine)
--------------------------------------------

Running the NeSI setup script with no arguments (:code:`rjm_config`) will
do the following:

* create a "Globus Guest Collection" in your NeSI nobackup directory, which will
  be used to transfer files to and from NeSI
* set the NeSI Globus Compute Multi-User endpoint id
* write configutation values from the above steps into the RJM config file on
  your local machine
* obtain the required authentication tokens so that you can start using RJM
  (tokens are cached on the local machine so you should not need to
  reauthenticate on the same machine)

When you run the setup script you will need to input values and authenticate
multiple times, which can't be avoided. In some cases
a browser window will open where you will need to do the authentication, in other
cases you will enter details directly at the terminal. You will
need to authenticate with NeSI (first and sector factor) and with Globus, which
can usually be done using your institutional credentials, e.g. *The University of
Auckland*.

**Note:** you only need to run this script once per machine.

Run a simple test
-----------------

Health check script
~~~~~~~~~~~~~~~~~~~

The health check script tests that basic functionality is working by uploading
a file to NeSI and executing a simple command.

.. code-block:: bash

   rjm_health_check

If should exit successfully with no errors if things are working.

Example simulation
~~~~~~~~~~~~~~~~~~

Clone the repository and run the example simulation using the steps below:

.. code-block:: bash

   git clone https://github.com/chrisdjscott/RemoteJobManager.git
   cd RemoteJobManager/examples/nonmem
   rjm_batch_submit -f localdirs.txt -ll info
   rjm_batch_wait -f localdirs.txt -ll info

**Note:** for the above to work you need to either make sure the RJM executables
are in your PATH or provide the full path to them.

If the above worked there should be a file created at *files/output.zip*.

You should also notice the file *files/remote_job.json*. This file records the
progress of the remote job. If you try to run a simulation again in the same
directory, RJM should detect that it already ran before and won't repeat the
same steps again. You can override this behaviour by passing the :code:`--force`
option to :code:`rjm_batch_submit` or delete the *remote_job.json* file.
