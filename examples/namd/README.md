# RJM NAMD example

To upload files and submit Slurm jobs:

```
rjm_batch_submit -f dirslist2.txt -ll info
```

Then wait for the Slurm job to complete and download files:

```
rjm_batch_wait -f dirslist2.txt -ll info -z 30
```
