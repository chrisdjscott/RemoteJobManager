#!/bin/bash -e
#SBATCH --job-name=funcxtest
#SBATCH --time=00:15:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --gres=gpu:1
#SBATCH --mem=4G
#SBATCH --hint=nomultithread
#SBATCH --output=slurm.log

# load the NAMD environment module
ml purge
ml NAMD/2.12-gimkl-2017a-cuda

# run the simulation
srun namd2 +ppn ${SLURM_CPUS_PER_TASK} +idlepoll apoa1.namd
