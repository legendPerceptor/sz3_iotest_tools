#!/bin/bash
#SBATCH --job-name={{cfg.jobname}}
#SBATCH -p bdwall
#SBATCH -A ECP-EZ
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=00:03:00
#SBATCH -o {{cfg.workdir}}/{{cfg.jobname}}.out
#SBATCH -e {{cfg.workdir}}/{{cfg.jobname}}.error

source ~/spack/share/spack/setup-env.sh
spack load libpressio+sz+zfp+python

srun python {{cfg.workdir}}/compression.py -c {{cfg.compress_cfg}} -f {{cfg.filepath}}