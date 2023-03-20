#!/bin/bash
# Example to submit a Job and get a Nodes exclusivly
#
#SBATCH -J 1feature_files # A single job name for the array
#SBATCH --qos normal  # normal priority level
#SBATCH --mail-user=manuela.koepfli@wsl.ch # Your email
#SBATCH --mail-type=SUBMIT,END,FAIL # notify you at given Events
#SBATCH -o feature_files%j.out # Standard output
#SBATCH -e feature_files%j.err # Standard error output
#SBATCH --nodes=24
#SBATCH --tasks-per-node=1
#SBATCH --partition=node
#SBATCH --time=00-01:00 #run for DD-HH:MM
#SBATCH -t 0-48:00 # Running time of xx hours
#

#execute your script:
python comp_con_attributes_HPC1.py
