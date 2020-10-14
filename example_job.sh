
#!/bin/bash

# This will need to be converted to run within python
# (since we'll interact with the google sheet via
# python).

# Gather track info from google sheet.

# Interactive
globus login

export cedar_nodeid="c99fd40c-5545-11e7-beb6-22000b9a448b"
export nm_nodeid="62708910-8e89-11e8-9641-0a6d4e044368"
export msuhpc_nodeid="a640bafc-6d04-11e5-ba46-22000b92c6ec"

# Check activation on endpoints
# Need to enable a delegate proxy renewal with X.509
# files so this is automated.
#
globus endpoint activate $cedar_nodeid --force
globus endpoint activate $nm_nodeid --force --my-proxy  # put in username + password interactively.
# globus endpoint activate $msuhpc_nodeid --force

export cedar_base_path="scratch/VLAXL_reduction"

# export nm_base_path="/lustre/aoc/observers/nm-7669/data"
export nm_base_path="/lustre/aoc/projects/20A-346/data_staged/"

export msuhpc_base_path="/mnt/research/ChomiukLab/LocalGroupX/M31_20A-346"


# Will be given this:
# export track_name="20A-346.sb38097770.eb38161238.58986.707791782406"
export track_name="20A-346.sb38492195.eb38516348.59050.30097761574"

export target="M31"
# export config="C"
export config="B"

# Mkdir on cedar:
export track_folder_name="${target}_${config}_${track_name}"

globus mkdir $cedar_nodeid:"${cedar_base_path}/${track_folder_name}"

# Transfer the track
task_id="$(globus transfer $nm_nodeid:${nm_base_path}/${track_name}.tar $cedar_nodeid:${cedar_base_path}/${track_folder_name}/${track_name}.tar --jmespath 'task_id' --format=UNIX )"

# Wait 30 sec before moving on to transferring the pipeline and
# submission scripts
globus task wait $task_id --timeout 30

# Here we'll transfer the newest/set version of the reduction pipeline
mkdir -P ${track_name}_reduction_pipeline

cd ${track_name}_reduction_pipeline
# TODO: (1) use main repo, not fork
git clone https://github.com/e-koch/ReductionPipeline.git
# TODO: (2) lock to a specific version

# TODO: Copy local repo to the output
# Need to create a globus endpoint here.

# Lastly, generate the job scripts and transfer
cd ~/AutoDataIngest
# This should ALWAYS work. We don't want to touch
# this repo manually for any reason.
git pull

# Run python script to produce the job scripts for this
# track:
# (1) untar -> SDM -> MS -> line/continuum split
# (2) continuum pipeline run
# (3) line pipeline run

# Transfer

# Clean up and delete pipeline repo from here
cd ../


# The other transfers should be quick from this instance.
# Wait on the big data transfer to finish
globus task wait $task_id

# Trigger removing the data from NRAO storage
globus rm $nm_nodeid:${nm_base_path}/${track_name}.tar

# Now ssh into cedar (using key) and submit the jobs


# Then (1) create drive directory and track status sheet
# (2) Push the jobs numbers to the sheet (updates from email notifications)
# (3) Push the download status to the master track sheet.
