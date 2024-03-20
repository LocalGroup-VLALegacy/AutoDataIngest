#!/bin/bash

# Script to download target/config MSs to a local drive using globus.

source ~/.bashrc

globus_cmd=/home/datamanager/.local/bin/globus
casa_cmd="/mnt/work/ekoch/casa-6.6.0-20-py3.8.el7/bin/casa --nogui --log2term"

casa_script="/home/datamanager/AutoDataIngest/sync_to_local_target_splitter.py"

out_path=/mnt/work/vlaxl/downloads/
mkdir -p $out_path

cd $out_path

source_ep=8dec4129-9ab4-451d-a45f-5b4b8471f7a3
dest_ep=e8fc98cc-9ca8-11eb-92cd-6b08dd67ff48

# Use filter matching to get all tracks for a given target
target=M31

# and config
config=B

# Use below for all configs.
# config='*'

# and speclines vs. continuum
data_type=continuum

# Select field to split.
target_field=M31LARGE_14

target_field2=M31LARGE_47


######################################
######################################
######################################

# Get list of MS files on project space
rm cedar_files.txt
# $globus_cmd ls $source_ep:projects/rrg-eros-ab/ekoch/VLAXL/calibrated/ --filter "~${target}_${config}_*${data_type}*.tar" -r >> cedar_files.txt
$globus_cmd ls $source_ep:projects/rrg-eros-ab/ekoch/VLAXL/calibrated/archive/ --filter "~${target}_${config}_*${data_type}*.tar" -r >> cedar_files.txt

# Check if globus call failed:
if [ -s cedar_files.txt ]; then
    echo "Successful connection to cedar."
else
    echo "Unable to connect to cedar."
    exit 1
fi


FILENAMES=$(cat cedar_files.txt)
for filename in $FILENAMES; do
    echo $filename

    echo "Transferring to local server at $(date)"

    # task_id="$($globus_cmd transfer $source_ep:projects/rrg-eros-ab/ekoch/VLAXL/calibrated/$filename $dest_ep:$out_path/$filename --jmespath 'task_id' --format=UNIX)"
    task_id="$($globus_cmd transfer $source_ep:projects/rrg-eros-ab/ekoch/VLAXL/calibrated/archive/$filename $dest_ep:$out_path/$filename --jmespath 'task_id' --format=UNIX)"

    echo "Waiting on 'globus transfer' task '$task_id'"
    $globus_cmd task wait "$task_id" --polling-interval 120
    if [ $? -eq 0 ]; then
        echo "$task_id completed successfully";
    else
        echo "$task_id failed!";
        # exit 1
    fi

    echo "Finished transferring at $(date)"

    # Untar:
    tar -xf $filename

    # Split out fields
    $casa_cmd -c $casa_script $filename $target_field


    $casa_cmd -c $casa_script $filename $target_field2

    # Cleanup
    rm $filename
    tempstr=${filename%.tar}
    rm -rf ${tempstr#M31_B_}

    echo Completed $filename

done


echo "Finished script at $(date)"
