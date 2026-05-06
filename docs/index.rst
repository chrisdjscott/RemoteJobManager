.. RemoteJobManager documentation master file, created by
   sphinx-quickstart on Fri Feb 11 09:40:57 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to RemoteJobManager's documentation!
============================================

RemoteJobManager (RJM) provides an API to offload work to a remote system. It
has been developed for use on NeSI's Mahuika HPC cluster (Slurm) and supports
two interchangeable backends:

* **Globus stack (default and primary)**: file transfer over HTTPS via a Globus
  guest collection, plus Globus Compute (formerly funcX) to invoke commands and
  Slurm jobs on the remote machine.
* **Paramiko stack (experimental)**: SSH for command execution and SFTP for
  file transfer using a locally generated keypair. This backend exists as an
  alternative for environments where Globus is not available, but it is far
  less well tested than the Globus stack and should be considered experimental.
  Most development effort targets the Globus path.

The runner and transferer abstractions allow other backends to be added.

Some installation and configuration of RJM are required before using the tool.


.. toctree::
   :maxdepth: 1

   getting_started_nesi
   installation
   configuration
   using_rjm
   troubleshooting_on_nesi
   api







Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
