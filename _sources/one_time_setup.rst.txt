One-time setup
==============

.. contents::
   :local:
   :backlinks: none

This guide has been tested with NeSI as the remote.

A Globus account is required: https://app.globus.org/


Remote machine
--------------

Globus Guest Collection
~~~~~~~~~~~~~~~~~~~~~~~

On the remote machine (e.g. NeSI), you need to have a Globus Guest Collection.
Globus has the following guide on how to create a Guest Collection:

https://docs.globus.org/how-to/share-files/

You must have write access to the Guest Collection. On NeSI, this means you
should create the shared directory on the *nobackup* file system.

1. Open the `Globus Web App file manager`_
2. Connect to the "NeSI Wellington DTN v5" endpoint (requires NeSI 2 factor
   authentication); create, if required, and navigate to the directory
   you wish to use for remote jobs; and select the *Share* option

   .. image:: _static/images/00_sharedir.png
      :target: _static/images/00_sharedir.png

3. On the share settings page, select *Add a Guest Collection*

   .. image:: _static/images/01_sharescreen.png
      :target: _static/images/01_sharescreen.png

4. On the *Create New Guest Collection* page, make sure the *Path* is correct
   and give the share a name (also make a note of the *Path* as this will be
   required during :doc:`configuration`)

   .. image:: _static/images/02_create_guest_collection.png
      :target: _static/images/02_create_guest_collection.png

5. Make a note of the guest collection endpoint id as this will be required
   during :doc:`configuration`

   .. image:: _static/images/03_endpointid.png
      :target: _static/images/03_endpointid.png

**TODO:** how to share a globus guest collection with others...

.. _Globus Web App file manager: https://app.globus.org/file-manager

funcX endpoint
~~~~~~~~~~~~~~

A funcX endpoint is also required on the remote machine. On NeSI this could
be created as follows:

.. code-block:: bash

    # the following should be run on a mahuika login node:
    #   (if using jupyter terminal, run
    #    "ssh login.mahuika.nesi.org.nz before continuing")

    # load funcx endpoint software into the environment
    ml funcx-endpoint

    # some versions of funcX had a bug where the config directory 
    # must already exist before running configure
    mkdir -p ~/.funcx

    # first time setup for funcx, will ask you to authenticate with
    # Globus and copy a code back to the terminal
    funcx-endpoint configure

    # start the default endpoint
    funcx-endpoint start

    # verify the endpoint is running and obtain the endpoint id
    funcx-endpoint list

Make a note of the endpoint id that shows up in the list command, you will
need it during :doc:`configuration`. The output from list will look something
like follows:

Note: we are effectively just using funcX as a Slurm API (running Slurm commands
on the remote machine), so the *default* endpoint running on a login node is
entirely appropriate and sufficient.

Also note: after running the above commands, it is safe to close the
window, SSH connection, Jupyter session, etc. - funcX daemonises the process
running the endpoint so it is no longer attached to the running session.

.. code-block:: bash

    +---------------+---------+--------------------------------------+
    | Endpoint Name | Status  |             Endpoint ID              |
    +===============+=========+======================================+
    | default       | Running | ffd77d5c-b65f-4479-bbc3-66a2f7346858 |
    +---------------+---------+--------------------------------------+

It may sometimes be necessary to restart the endpoint, for example if the
login node was rebooted or some other issue occurred. The following would
achieve this:

.. code-block:: bash

    # the following should be run on a mahuika login node:
    #   (if using jupyter terminal, run
    #    "ssh login.mahuika.nesi.org.nz before continuing")

    # load funcx endpoint software into the environment
    ml funcx-endpoint

    # restart the default endpoint
    funcx-endpoint restart

    # verify the endpoint is running
    funcx-endpoint list
