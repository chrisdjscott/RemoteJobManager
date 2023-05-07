#!/bin/bash -e

./cleanup.sh
rjm_batch_submit -f localdirs.txt -ll debug -l submit.log
rjm_batch_wait -f localdirs.txt -ll debug -l wait.log -z 15 -o
