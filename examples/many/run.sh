#!/bin/bash -e

rjm_batch_submit -f dirlist.txt -ll debug
rjm_batch_wait -f dirlist.txt -ll debug -z 10 -o
