Troubleshooting on NeSI
=======================

The NeSI platform occasionally experiences stability issues related the filesystems, Slurm, networking and Globus.
RJM attempts to handle these issues by retrying commands that have failed but it is not always successful.

If RJM isn't working well first try running the :code:`rjm_health_check` program (:code:`-ll debug` will print additional
output that can be useful for debugging):

.. code-block:: bash

   rjm_health_check -ll debug

There is no longer any need to run :code:`rjm_restart` as we are now using the NeSI managed Globus Compute endpoint.

If you encounter problems, please contact [NeSI Support](mailto:support@nesi.org.nz) and mention that you are using
Globus Transfer and Compute via the RemoteJobManager tool. You could also include the output from :code:`rjm_health_check -ll debug`.
