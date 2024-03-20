#!/bin/bash
arguments=($SSH_ORIGINAL_COMMAND)
# arg0 = directory
# arg1 = sbatch command kwargs

echo Switching to directory: ${arguments[0]}
cd ${arguments[0]}

echo Submitting: ${arguments[1]}
# sbatch --account=def-eros-ab ${arguments[1]}
sbatch --account=rrg-eros-ab ${arguments[1]}
