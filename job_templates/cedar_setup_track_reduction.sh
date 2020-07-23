
# Setup, create, and submit the jobs to run the pipeline.

target='M31'

config="C"

track_name="20A-346.sb38095502.eb38174408.58988.69745849537"

cd ~/scratch/VLAXL_reduction/

# Make the track folder
foldername="${target}_${config}_${track_name}"

mkdir $foldername

# Move the tar file there
mv "${track_name}.tar" $foldername/

cd $foldername

# Grab the latest version of the pipeline.
git clone https://github.com/LocalGroup-VLALegacy/ReductionPipeline.git

# Make the import and split
python ~/VLAXL/AutoDataIngest/job_templates/job_import_and_merge.py $track_name --target_name $target --config $config

# Submit job
# But we want to record the job number. That's what --parsable is for
split_jobum=$(sbatch --parsable --account=rrg-eros-ab --time=24:00:00 --mem=8000M "${foldername}_split.sh")

# Submit the line pipeline (conditional on the split finishing)
python ~/AutoDataIngest/job_templates/job_line_pipeline.py $track_name --target_name $target --config $config --conditional_on_jobnum $split_jobum

sbatch --account=rrg-eros-ab "${foldername}_line_pipeline.sh"

# Submit the continuum pipeline (conditional on the split finishing)
# python ~/AutoDataIngest/job_templates/job_continuum_pipeline.py $track_name --target_name $target --config $config --conditional_on_jobnum $split_jobum

# sbatch --account=rrg-eros-ab "${foldername}_continuum_pipeline.sh"

cd ~/scratch/VLAXL_reduction/
