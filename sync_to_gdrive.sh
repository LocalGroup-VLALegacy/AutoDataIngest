#!/bin/bash

# Script to run as a cronjob. Checks for new calibrated MS files
# in project space on cedar and uploads to the shared google drive with rclone

# The MS files are NOT kept locally after they have been added to gdrive.
# The MS files are simply too large to keep on any storage attached to the
# QA server

# Setup requires: (1) rclone to the shared drive.
# (2) login to globus with credentials to access Eric's rrg-eros-ab project space on cedar

# cronjob:
# 0 2 * * * bash ~/AutoDataIngest/sync_to_gdrive.sh > /home/datamanager/cron_sync_to_gdrive.log 2>&1

source ~/.bashrc

cd /home/datamanager/space/vlaxl/calibrated/

source_ep=c99fd40c-5545-11e7-beb6-22000b9a448b
dest_ep=e8fc98cc-9ca8-11eb-92cd-6b08dd67ff48

# Get current list of MS files on the drive after deleting the previous list.
rm drive_files.txt
rclone lsf lglbs_gdrive:MeasurementSets >> drive_files.txt

# If the gdrive pull failed, check for an empty txt file
# Otherwise it will try to download EVERYTHING!
if [ -s drive_files.txt ]; then
    echo "Successful connection to gdrive."
    rm gdrive_fail.txt
else
    echo "Unable to connect to gdrive."
    touch gdrive_fail.txt
    exit 1
fi

# Get list of MS files on project space
rm cedar_files.txt
/home/datamanager/.local/bin/globus ls $source_ep:projects/rrg-eros-ab/ekoch/VLAXL/calibrated/ >> cedar_files.txt

# Check if globus call failed:
if [ -s cedar_files.txt ]; then
    echo "Successful connection to cedar."
    rm cedar_fail.txt
else
    echo "Unable to connect to cedar."
    touch cedar_fail.txt
    exit 1
fi

if cmp -s drive_files.txt cedar_files.txt; then
    echo "No new MS files found"
    exit 0
fi

# Find the new files.
rm new_ms_files.txt batch_files.txt
/home/datamanager/miniconda3/envs/py37/bin/python ~/AutoDataIngest/sync_to_gdrive_diffchecker.py

# To avoid keeping too many files on disk, set a max number of files
# for each execution.
MAX_FILES=8

filenum=0

FILENAMES=$(cat new_ms_files.txt)
for filename in $FILENAMES; do
    echo "$filename $filename" >> batch_files.txt

    filenum=$((filenum + 1))

    if [ "$filenum" -eq $MAX_FILES ]; then
        echo "Reach max number of files to transfer: ${MAX_FILES}"
        break
    fi

done

# diff drive_files.txt cedar_files.txt >> new_ms_files.txt

# Transfer new files here:

echo "Found files to upload: ${FILENAMES}"

# Transfer to QA webserver
echo "Transferring to QA server at $(date)"

task_id="$(/home/datamanager/.local/bin/globus transfer $source_ep:projects/rrg-eros-ab/ekoch/VLAXL/calibrated/ $dest_ep:space/vlaxl/calibrated/ --jmespath 'task_id' --format=UNIX --batch < batch_files.txt)"

echo "Waiting on 'globus transfer' task '$task_id'"
/home/datamanager/.local/bin/globus task wait "$task_id" --polling-interval 300
if [ $? -eq 0 ]; then
    echo "$task_id completed successfully";
else
    echo "$task_id failed!";
    exit 1
fi

echo "Finished transferring at $(date)"

# Upload to gdrive then delete from here
for filename in *.tar; do

    # Upload to gdrive
    echo "Uploading ${filename} to gdrive at $(date)"
    flock -n /tmp/google_drv_sync.lock /usr/bin/rclone copy --retries 5 $filename lglbs_gdrive:MeasurementSets/
    echo "Finished uploading ${filename} to gdrive at $(date)"

    echo "Removing ${filename} at $(date)"
    rm $filename

    done

echo "Finished script at $(date)"
