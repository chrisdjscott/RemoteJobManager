#!/usr/bin/env bash
set -euo pipefail

cat input.txt > output.txt
echo "ran on $(hostname) at $(date -u +%FT%TZ)" >> output.txt

sleep 90
