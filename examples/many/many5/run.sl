#!/bin/bash
#SBATCH --job-name=5.testfxmany
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=128
#SBATCH --time=00:05:00

sleepfor=$(( $RANDOM % 120 + 31 ))
echo "sleeping for $sleepfor" > dummy.txt
sleep $sleepfor
touch dummy2.txt
