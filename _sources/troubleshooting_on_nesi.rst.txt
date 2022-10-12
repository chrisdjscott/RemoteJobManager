Troubleshooting on NeSI
=======================

The NeSI platform occasionally experiences stability issues related the filesystems, Slurm, networking and Globus.
RJM attempts to handle these issues by retrying commands that have failed but it is not always successful.

If RJM isn't working well first try running the :code:`rjm_health_check` program (:code:`-ll debug` will print additional
output that can be useful for debugging):

.. code-block:: bash

   rjm_health_check -ll debug

If this command fails, a good first step is to try resetting your funcX endpoint on NeSI, which can
sometimes get into a bad state, particularly if there was a network issue on NeSI or one of the login
nodes went down:

.. code-block:: bash

   rjm_nesi_setup --restart -ll debug

After running this command, try the :code:`rjm_health_check` program. If it still doesn't work, there is likely to be a
bigger issue, please contact `NeSI support <https://support.nesi.org.nz/hc/en-gb/requests/new>`_ with the error message
and mention you are using Globus, funcX and the RemoteJobManager tool.

Common errors
-------------

 




