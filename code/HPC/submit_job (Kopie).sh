#!/bin/bash
# Example to submit a Job and get a Nodes exclusivly
#
#SBATCH -J feature_files # A single job name for the array
#SBATCH -A node # Node account
#SBATCH -p node # Node partition
#SBATCH --qos normal  # normal priority level
#SBATCH --mail-user=manuela.koepfli@wsl.ch # Your email
#SBATCH --mail-type=SUBMIT,END,FAIL # notify you at given Events
#SBATCH -o SedCas_Ref%j.out # Standard output
#SBATCH -e SedCas_Ref%j.err # Standard error output
#SBATCH --ntasks 24 # number of cores --> should be 1 in array job
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1G #request of RAM
#SBATCH --time=00-01:00 #run for DD-HH:MM
#

#load all necessary modules:
source /home/koepfli/env/ILLenv/bin/activate

#execute your script:
python comp_con_attributes_HPC.py
