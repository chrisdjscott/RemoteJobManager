#!/usr/bin/env bash

set -e

./cleanup.sh
echo ""
echo "================================================================================"
echo "Start of rjm_batch_submit..."
echo ""
rjm_batch_submit -f dirlist1.txt -ll debug -n
echo ""
echo "================================================================================"
echo "Start of rjm_batch_wait..."
echo ""
rjm_batch_wait -f dirlist1.txt -ll debug -n
