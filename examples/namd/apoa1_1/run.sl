#!/bin/bash -e
#SBATCH --job-name=rjmnamd1
#SBATCH --time=00:10:00
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=500M
#SBATCH --hint=nomultithread
#SBATCH --output=slurm.log

# load the NAMD environment module
ml purge
ml NAMD/2.12-gimkl-2017a-mpi

# run the simulation
srun namd2 apoa1.namd
