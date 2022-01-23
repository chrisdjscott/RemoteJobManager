#!/bin/bash
exefile=$1
shift
./${exefile} $*
status=$?
if [[ $status -eq 1 ]]; then 
  status=0
fi
exit $status
