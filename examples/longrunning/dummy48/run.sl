#!/bin/bash
#SBATCH --job-name=testfuncx
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=128
#SBATCH --time=48:01:00

touch dummy.txt
sleep 48h
touch dummy2.txt
