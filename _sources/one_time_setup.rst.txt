One-time setup
==============

This guide has been tested on NeSI.

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

1. Open the `Globus Web App`_
2. Connect to the "NeSI Wellington DTN v5" endpoint (requires NeSI 2 factor
   authentication); create, if required, and navigate to the directory
   you wish to use for remote jobs; and select the *Share* option

   .. image:: _static/images/00_sharedir.png
      :target: _static/images/00_sharedir.png

3. On the share settings page, select *Add a Guest Collection*

   .. image:: _static/images/01_sharescreen.png
      :target: _static/images/01_sharescreen.png

4. On the *Create New Guest Collection* page, make sure the path is correct
   and give the share a name (make a note of the path as this will be required
   during :doc:`configuration`)

   .. image:: _static/images/02_create_guest_collection.png
      :target: _static/images/02_create_guest_collection.png

5. Make a note of the guest collection endpoint id as this will be required
   during :doc:`configuration`

   .. image:: _static/images/03_endpointid.png
      :target: _static/images/03_endpointid.png

.. _Globus Web App: https://app.globus.org/

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

    # first time setup for funcx, will ask you to authenticate with
    # Globus and copy a code back to the terminal
    funcx-endpoint configure

    # start the default endpoint
    funcx-endpoint start

    # verify the endpoint is running
    funcx-endpoint list

We are effectively just using funcX as a Slurm API (running Slurm commands
on the remote machine), so the *default* endpoint running on a login node is
appropriate and sufficient.

**TODO**: where to get the endpoint id


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
    funcx_endpoint restart

    # verify the endpoint is running
    funcx-endpoint list
