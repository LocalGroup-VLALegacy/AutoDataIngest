#!/bin/bash


echo "Starting at $(date +%F_%T)"

# This script flattens and tars a file staged from the new data archive.

folder_name=$1

output_dir="/lustre/aoc/projects/20A-346/data_staged_new/"

# Allow looping through the SDMs for multiple staged at once.

cd $folder_name

for sdm_name in *.sb*.eb*;
    do

    echo "Starting on ${sdm_name} at $(date +%F_%T)"

    if [ -f "$sdm_name" ]; then
        continue
    fi

    # Rename nested folders
    mv ${sdm_name} ${sdm_name}_temp

    mv ${sdm_name}_temp/${sdm_name} .

    rmdir ${sdm_name}_temp

    tar -cf ${sdm_name}.tar ${sdm_name}

    mv ${sdm_name}.tar $output_dir

    rm -rf ${sdm_name}

    echo "Finished with ${sdm_name} at $(date +%F_%T)"

    done

echo "Finished all at $(date +%F_%T)"
