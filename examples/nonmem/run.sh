#!/usr/bin/env bash

set -e

./cleanup.sh
rjm_batch_submit -f localdirs.txt -ll debug
rjm_batch_wait -f localdirs.txt -ll debug
