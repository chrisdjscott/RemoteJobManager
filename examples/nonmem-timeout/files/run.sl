#!/bin/bash
#SBATCH --job-name rjm.nonmem.test
#SBATCH --output=stdout.txt
#SBATCH --error=stderr.txt
#SBATCH --time=00:01:00
#SBATCH --mem-per-cpu=250
#SBATCH --ntasks=4

module load NONMEM/7.5.0
sleep 90
bash nmgrid.sh
