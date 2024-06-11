#!/bin/bash
arguments=($SSH_ORIGINAL_COMMAND)
# arg0 = directory
# arg1 = branch name to checkout


echo Switching to directory: ${arguments[0]}
cd ${arguments[0]}

rm -r ReductionPipeline
git clone https://github.com/LocalGroup-VLALegacy/ReductionPipeline.git
cd ReductionPipeline
git checkout ${arguments[1]}


