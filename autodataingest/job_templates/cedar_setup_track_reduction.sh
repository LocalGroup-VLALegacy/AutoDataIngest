
# Setup, create, and submit the jobs to run the pipeline.

target='M31'

# config="C"
config="B"

# track_name="20A-346.sb38098105.eb38158028.58985.68987263889"
# track_name="20A-346.sb38097770.eb38161238.58986.707791782406"
# track_name="20A-346.sb38095502.eb38174408.58988.69745849537"
# track_name="20A-346.sb38096442.eb38209668.58991.5012732176"
# track_name="20A-346.sb38096442.eb38216745.58992.52579186343"
# track_name="20A-346.sb38098534.eb38272581.59009.385237395836"
track_name="20A-346.sb38492195.eb38516348.59050.30097761574"

cd ~/scratch/VLAXL_reduction/

# Make the track folder
foldername="${target}_${config}_${track_name}"

# mkdir $foldername

# # Move the tar file there
# mv "${track_name}.tar" $foldername/

cd $foldername

# Grab the latest version of the pipeline.
git clone https://github.com/LocalGroup-VLALegacy/ReductionPipeline.git

# Make the import and split
~/anaconda3/bin/python ~/VLAXL/AutoDataIngest/job_templates/job_import_and_merge.py $track_name --target_name $target --config $config

# Submit job
# But we want to record the job number. That's what --parsable is for
split_jobnum=$(sbatch --parsable --account=rrg-eros-ab --time=8:00:00 --mem=8000M "${foldername}_split.sh")

# Submit the line pipeline (conditional on the split finishing)
~/anaconda3/bin/python ~/VLAXL/AutoDataIngest/job_templates/job_line_pipeline.py $track_name --target_name $target --config $config --conditional_on_jobnum $split_jobnum

sbatch --account=rrg-eros-ab --dependency=afterok:${split_jobnum} "${foldername}_line_pipeline.sh"

# Submit the continuum pipeline (conditional on the split finishing)
# ~/anaconda3/bin/python ~/VLAXL/AutoDataIngest/job_templates/job_continuum_pipeline.py $track_name --target_name $target --config $config --conditional_on_jobnum $split_jobnum

# sbatch --account=rrg-eros-ab "${foldername}_continuum_pipeline.sh"

cd ~/scratch/VLAXL_reduction/
