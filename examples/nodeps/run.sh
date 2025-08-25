#!/usr/bin/env bash

set -e

./cleanup.sh
rjm_batch_submit -f dirlist.txt -ll debug
rjm_batch_wait -f dirlist.txt -ll debug
