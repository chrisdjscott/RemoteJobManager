Installation
============

On Windows you could download the executables from the latest release
on the `GitHub repository`_.

Otherwise, install the *rjm* python package with:

.. code-block:: bash

    python -m pip install git+https://github.com/chrisdjscott/RemoteJobManager

To use the Paramiko SSH runner and SFTP transferer instead of the Globus
components, install the optional ``ssh`` extra (which pulls in ``paramiko``):

.. code-block:: bash

    python -m pip install "RemoteJobManager[ssh] @ git+https://github.com/chrisdjscott/RemoteJobManager"

.. _GitHub repository: https://github.com/chrisdjscott/RemoteJobManager/releases
