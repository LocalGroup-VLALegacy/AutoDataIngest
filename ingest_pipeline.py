
'''
The full staging/transfer/reduction pipeline process.
'''

import sys
from pathlib import Path
from glob import glob

# fabric handles ssh to cluster running jobs
import fabric

# TODO: cleanup imports. Separated now as development is on-going.
from autodataingest.email_notifications.receive_gmail_notifications import (check_for_archive_notification, check_for_job_notification)
from autodataingest.gsheet_tracker.gsheet_functions import (find_new_tracks, update_track_status,
                                             update_cell, return_cell)
from autodataingest.archive_request_LG import archive_copy_SDM
from autodataingest.globus_functions.globus_wrappers import (transfer_file, transfer_pipeline,
                                            cleanup_source, globus_wait_for_completion)
from autodataingest.get_track_info import match_ebid_to_source
from autodataingest.download_vlaant_corrections import download_vla_antcorr

import autodataingest.job_templates.job_import_and_merge as jobs_import
import autodataingest.job_templates.job_continuum_pipeline as jobs_continuum
import autodataingest.job_templates.job_line_pipeline as jobs_line


# Cluster where the data will be transferred to and reduced.
CLUSTERNAME = 'cc-cedar'
CLUSTERACCOUNT = 'rrg-eros-ab'

CLUSTER_SPLIT_JOBTIME = '8:00:00'
CLUSTER_CONTINUUM_JOBTIME = '30:00:00'
CLUSTER_LINE_JOBTIME = '30:00:00'

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

    update_cell(ebid, "TRUE", name_col=18,
                sheetname='20A - OpLog Summary')

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
                                              setup_kwargs={}),
          file=open(track_scripts_dir / job_filename, 'a'))

    # Requires keys to be setup
    connect = fabric.Connection('cedar.computecanada.ca')

    # Grab the repo; this is where we can also specify a version number, too
    git_clone_command = 'git clone https://github.com/LocalGroup-VLALegacy/ReductionPipeline.git'

    result = connect.run(f'cd scratch/VLAXL_reduction/{track_folder_name}/ && rm -r ReductionPipeline && {git_clone_command}',
                         hide=True)

    if result.failed:
        raise ValueError(f"Failed to clone pipeline! See stderr: {result.stderr}")

    # TODO: Fix to a pipeline version here (i.e. git branch/tag to production pipeline)


    # Move the job script to the cluster:
    result = connect.put(track_scripts_dir / job_filename,
                         remote=f'scratch/VLAXL_reduction/{track_folder_name}/')

    # Submit the script:

    chdir_cmd = f"cd scratch/VLAXL_reduction/{track_folder_name}/"
    submit_cmd = f"sbatch --account={CLUSTERACCOUNT} --time={CLUSTER_SPLIT_JOBTIME} {job_filename}"

    result = connect.run(f"{chdir_cmd} && {submit_cmd}", hide=True)

    if result.failed:
        raise ValueError(f"Failed to submit split job! See stderr: {result.stderr}")

    # Record the job ID so we can check for completion.
    importsplit_jobid = result.stdout.replace("\n", '').split(" ")[-1]

    # Push the job ID to the EB ID dictionary to track completion
    tracks_processing[ebid].append(CLUSTERNAME)
    tracks_processing[ebid].append(importsplit_jobid)

    update_cell(ebid, f"{CLUSTERNAME}:{importsplit_jobid}", name_col=20,
                sheetname='20A - OpLog Summary')

    connect.close()

    update_track_status(ebid, message=f"Reduction running on {CLUSTERNAME}",
                        sheetname='20A - OpLog Summary',
                        status_col=1)


# With the job number for that track, submit the reduction pipeline
# jobs

for ebid in tracks_processing:

    # Before running any reduction, update the antenna correction files
    # and copy that folder to each folder where the pipeline is run
    download_vla_antcorr(data_folder="VLA_antcorr_tables")

    # Continuum pipeline job

    track_name = tracks_processing[ebid][0]
    track_folder_name = tracks_processing[ebid][1]
    importsplit_jobid = tracks_processing[ebid][4]

    track_scripts_dir = scripts_dir / track_folder_name

    if not track_scripts_dir.exists():
        track_scripts_dir.mkdir()

    # Ingest and line/continuum split job
    # TODO: generalize submission to other clusters

    job_filename = f"{track_folder_name}_job_continuum.sh"

    print(jobs_continuum.cedar_submission_script(target_name=track_folder_name.split('_')[0],
                                                 config=track_folder_name.split('_')[1],
                                                 trackname=track_folder_name.split('_')[2],
                                                 slurm_kwargs={},
                                                 setup_kwargs={},
                                                 conditional_on_jobnum=importsplit_jobid),
          file=open(track_scripts_dir / job_filename, 'a'))

    connect = fabric.Connection('cedar.computecanada.ca')

    # Move the job script to the cluster:
    result = connect.put(track_scripts_dir / job_filename,
                         remote=f'scratch/VLAXL_reduction/{track_folder_name}/')

    # Move the antenna correction folder over:
    result = connect.run(f"mkdir scratch/VLAXL_reduction/{track_folder_name}/VLA_antcorr_tables")
    for file in glob("VLA_antcorr_tables/*.txt"):
        result = connect.put(file, remote=f"scratch/VLAXL_reduction/{track_folder_name}/VLA_antcorr_tables/")

    # Submit the script:

    chdir_cmd = f"cd scratch/VLAXL_reduction/{track_folder_name}/"
    submit_cmd = f"sbatch --account={CLUSTERACCOUNT} --time={CLUSTER_CONTINUUM_JOBTIME} {job_filename}"

    result = connect.run(f"{chdir_cmd} && {submit_cmd}", hide=True)

    if result.failed:
        raise ValueError(f"Failed to submit continuum pipeline job! See stderr: {result.stderr}")

    # Record the job ID so we can check for completion.
    continuum_jobid = result.stdout.replace("\n", '').split(" ")[-1]

    update_cell(ebid, f"{CLUSTERNAME}:{continuum_jobid}", name_col=22,
                sheetname='20A - OpLog Summary')


    # Line pipeline jobs

    job_filename = f"{track_folder_name}_job_line.sh"

    print(jobs_line.cedar_submission_script_default(target_name=track_folder_name.split('_')[0],
                                                    config=track_folder_name.split('_')[1],
                                                    trackname=track_folder_name.split('_')[2],
                                                    slurm_kwargs={},
                                                    setup_kwargs={},
                                                    conditional_on_jobnum=importsplit_jobid),
          file=open(track_scripts_dir / job_filename, 'a'))

    # Move the job script to the cluster:
    result = connect.put(track_scripts_dir / job_filename,
                         remote=f'scratch/VLAXL_reduction/{track_folder_name}/')

    # Submit the script:

    chdir_cmd = f"cd scratch/VLAXL_reduction/{track_folder_name}/"
    submit_cmd = f"sbatch --account={CLUSTERACCOUNT} --time={CLUSTER_LINE_JOBTIME} {job_filename}"

    result = connect.run(f"{chdir_cmd} && {submit_cmd}", hide=True)

    if result.failed:
        raise ValueError(f"Failed to submit line pipeline job! See stderr: {result.stderr}")

    # Record the job ID so we can check for completion.
    line_jobid = result.stdout.replace("\n", '').split(" ")[-1]

    update_cell(ebid, f"{CLUSTERNAME}:{line_jobid}", name_col=24,
                sheetname='20A - OpLog Summary')

    connect.close()

# Check on pipeline job status and transfer pipeline products to gdrive:
# Trigger completion based on finding email w/ job status
# ALSO: use fabric to get the log files and put them here for inclusion with
# the pipeline outputs.
# TODO: Will need to add in checks for job failures

for ebid in tracks_processing:

    importsplit_jobid = return_cell(ebid, column=20).split(":")[-1]
    continuum_jobid = return_cell(ebid, column=22).split(":")[-1]
    line_jobid = return_cell(ebid, column=24).split(":")[-1]


    # Check for a job completion email and check the final status
    job_check = check_for_job_notification(importsplit_jobid)
    # If None, it isn't done yet!
    if job_check is None:
        continue

    job_status_split, job_runtime =  job_check
    is_done_split = True

    update_cell(ebid, job_status_split, name_col=19,
                sheetname='20A - OpLog Summary')
    update_cell(ebid, job_runtime, name_col=25,
                sheetname='20A - OpLog Summary')

    job_check = check_for_job_notification(continuum_jobid)

    is_done_continuum = False
    if job_check is not None:

        is_done_continuum = True

        job_status_continuum, job_runtime =  job_check

        update_cell(ebid, job_status_continuum, name_col=21,
                    sheetname='20A - OpLog Summary')
        update_cell(ebid, job_runtime, name_col=26,
                    sheetname='20A - OpLog Summary')

    job_check = check_for_job_notification(line_jobid)

    is_done_line = False
    if job_check is not None:

        is_done_line = True

        job_status_line, job_runtime =  job_check

        update_cell(ebid, job_status_line, name_col=23,
                    sheetname='20A - OpLog Summary')
        update_cell(ebid, job_runtime, name_col=27,
                    sheetname='20A - OpLog Summary')

    if all([is_done_split, is_done_continuum, is_done_line]):
        # Remove this EBID! This round of reductions is done!
        tracks_processing.pop(ebid)

        # Check if these were successful runs:
        job_split_complete = job_status_split == "COMPLETED"
        job_continuum_complete = job_status_continuum == "COMPLETED"
        job_line_complete = job_status_line == "COMPLETED"

        completeness_checks = [job_split_complete, job_continuum_complete, job_line_complete]

        if all(completeness_checks):

            update_track_status(ebid, message=f"Ready for QA",
                                sheetname='20A - OpLog Summary',
                                status_col=1)

        else:

            update_track_status(ebid, message=f"ISSUE needs manual check of job status",
                                sheetname='20A - OpLog Summary',
                                status_col=1)



# Create QA suite for review
# Where to do this? Here or another instance?

# Add some check of "final" and therefore finished reduction for each track

# update_track_status(ebid, message=f"Ready for imaging",
#                     sheetname='20A - OpLog Summary',
#                     status_col=1)

# Add some check for what a true fail case looks like.
# COuld be manual because this should (hopefully) not happen much

# update_track_status(ebid, message=f"FAILED",
#                     sheetname='20A - OpLog Summary',
#                     status_col=1)
