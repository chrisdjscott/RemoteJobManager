.. RemoteJobManager documentation master file, created by
   sphinx-quickstart on Fri Feb 11 09:40:57 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to RemoteJobManager's documentation!
============================================

RemoteJobManager (RJM) provides an API to offload work to a remote system. It uses
Globus to transfer files and funcX to execute commands on the remote machine,
but could be extended to use other similar mechanisms (e.g. Slurm API). It has
been developed for use on NeSI's HPC platform (Slurm cluster with Globus support).

Some "one-time setup" and installation and configuration of RJM are required
before using the tool.


.. toctree::
   :maxdepth: 2

   getting_started_nesi
   getting_started_manual
   using_rjm







Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
