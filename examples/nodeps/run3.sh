#!/usr/bin/env bash

set -e

./cleanup.sh
rjm_batch_submit -f dirlist3.txt -ll debug
rjm_batch_wait -f dirlist3.txt -ll debug
