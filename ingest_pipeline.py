
'''
The full staging/transfer/reduction pipeline process.
'''

import sys
from pathlib import Path

# fabric handles ssh to cluster running jobs
import fabric

from email_notifications.receive_gmail_notifications import check_for_archive_notification
from gsheet_tracker.gsheet_functions import (find_new_tracks, update_track_status,
                                             update_cell, return_cell)
from archive_request_LG import archive_copy_SDM
from globus_function.globus_wrapper import (transfer_file, transfer_pipeline,
                                            cleanup_source, globus_wait_for_completion)
from get_track_info import match_ebid_to_source

import job_templates.job_import_and_merge as jobs_import

# Cluster where the data will be transferred to and reduced.
CLUSTERNAME = 'cc-cedar'

# Check for new tracks

new_track_ebids = find_new_tracks()

if len(new_track_ebids) == 0:
    sys.exit(0)

# Stage the archive downloads.

staged_ebids = []

for ebid in new_track_ebids[-2:]:

    print(f"Staging archive request for new EBID {ebid}")

    archive_copy_SDM(ebid)

    # Update the spreadsheet:
    update_track_status(ebid, message="Archive download staged",
                        sheetname='20A - OpLog Summary',
                        status_col=1)

    staged_ebids.append(ebid)

# Check for the archive email to come in. Once it has, start the globus transfer
tracks_processing = {}

for ebid in staged_ebids:

    out = check_for_archive_notification(ebid)

    # Not available yet. Check again later.
    if out is None:
        continue

    # Otherwise we should have the path on AOC and the full MS name
    # from the email.
    path_to_data, track_name = out

    # Update track name in sheet:
    update_cell(ebid, track_name,
                name_col=3,
                sheetname='20A - OpLog Summary')

    # Scrap the VLA archive for target and config w/ astroquery
    # This will query the archive for the list of targets until the output has a matching EBID.
    target, datasize = match_ebid_to_source(ebid,
                                            targets=['M31', 'M33', 'NGC6822', 'IC10', 'IC1613', 'WLM'],
                                            project_code='20A-346',
                                            verbose=False)

    # Add track target to the sheet
    update_cell(ebid, target, name_col=4,
                sheetname='20A - OpLog Summary')

    # And the data size
    update_cell(ebid, datasize.rstrip('GB'), name_col=14,
                sheetname='20A - OpLog Summary')

    # We want to easily track (1) target, (2) config, and (3) track name
    # We'll combine these for our folder names where the data will get placed
    # after transfer from the archive.
    config = return_cell(ebid, column=9)

    track_folder_name = f"{target}_{config}_{track_name}"

    # Do globus transfer:

    transfer_taskid = transfer_file(track_name, track_folder_name,
                                    startnode='nrao-aoc',
                                    endnode='cc-cedar',
                                    wait_for_completion=False)

    # Transfer a copy of the ReductionPipeline:
    # This has moved to pulling the github repo directly on the machine to avoid
    # an additional transfer here
    # If the cluster cannot do this for some reason, uncomment out below

    # transfer_pipeline(track_name)

    tracks_processing[ebid] = [track_name, track_folder_name, transfer_taskid]


    update_track_status(ebid, message=f"Data transferred to {CLUSTERNAME}",
                        sheetname='20A - OpLog Summary',
                        status_col=1)




# Wait for any active transfer to finish.
for ebid in tracks_processing:

    track_name = tracks_processing[ebid][0]
    transfer_taskid = tracks_processing[ebid][2]
    # Hold at this stage
    globus_wait_for_completion(transfer_taskid)

    # Remove the data staged at NRAO to avoid exceeding our storage quota
    cleanup_source(track_name, node='nrao-aoc')


scripts_dir = Path('reduction_job_scripts/')

# Submit pipeline job for data ingest and split
# This needs to be separate because the pipeline jobs
# will be subject to successful completion of this job
# i.e. we need the job number before submitting the others
for ebid in tracks_processing:

    track_name = tracks_processing[ebid][0]
    track_folder_name = tracks_processing[ebid][1]

    track_scripts_dir = scripts_dir / track_folder_name

    if not track_scripts_dir.exists():
        track_scripts_dir.mkdir()

    # Ingest and line/continuum split job
    # TODO: generalize submission to other clusters

    job_filename = f"{track_folder_name}_job_import_and_split.sh"

    print(jobs_import.cedar_submission_script(target_name=track_folder_name.split('_')[0],
                                              config=track_folder_name.split('_')[1],
                                              trackname=track_folder_name.split('_')[2],
                                              slurm_kwargs={},
                                              setup_kwargs={},
          file=open(job_filename, 'a')))

    # Requires keys to be setup
    connect = fabric.Connection('cedar.computecanada.ca')

    # Grab the repo; this is where we can also specify a version number, too
    git_clone_command = 'git clone https://github.com/LocalGroup-VLALegacy/ReductionPipeline.git'
    result = connect.run(f'cd scratch/VLAXL_reduction/{track_folder_name}/ && {git_clone_command}',
                         hide=True)

    if result.failed:
        raise ValueError(f"Failed to clone pipeline! See stderr: {result.stderr}")

    # TODO: Fix to a pipeline version here (i.e. git branch/tag to production pipeline)


    # Move the job script to the cluster:
    


# With the job number for that track, submit the reduction pipeline
# jobs

for ebid in tracks_processing:

    # Continuum pipeline job

    # Line pipeline jobs


# Check on pipeline job status and transfer pipeline products to gdrive:
# Trigger completion based on finding email w/ job status
# TODO: Will need to add in checks for job failures, still

# Create QA suite for review
# Where to do this? Here or another instance?
