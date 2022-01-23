#!/bin/bash
#SBATCH --job-name rjm.nonmem.test
#SBATCH --output=stdout.txt
#SBATCH --error=stderr.txt
#SBATCH --time=00:05:00
#SBATCH --mem-per-cpu=250
#SBATCH --ntasks=4

module load NONMEM/7.5.0
bash nmgrid.sh
