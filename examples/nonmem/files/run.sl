#!/bin/bash
#SBATCH --job-name rjm.nonmem.test
#SBATCH --output=stdout.txt
#SBATCH --error=stderr.txt
#SBATCH --time=00:10:00
#SBATCH --mem-per-cpu=500
#SBATCH --ntasks=4

module load NONMEM/7.5.1-iimpi-2022a
bash nmgrid.sh
