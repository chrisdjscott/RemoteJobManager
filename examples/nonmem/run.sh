#!/bin/bash -e

./cleanup.sh
rjm_batch_submit -f localdirs.txt -ll debug
rjm_batch_wait -f localdirs.txt -ll debug -z 15
