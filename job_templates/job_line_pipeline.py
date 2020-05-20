
'''
Create a slurm submission script to convert to an MS and split the SPWs.

To be run in python3
'''

from job_tools import cedar_slurm_setup, cedar_job_setup


def cedar_submission_script_default(target_name="M31",
                                    config="C",
                                    trackname="20A-346.sb38098105.eb38158028.58985.68987263889",
                                    slurm_kwargs={},
                                    setup_kwargs={},
                                    conditional_on_jobnum=None):
    '''
    Runs the default VLA pipeline.

    TODO: Make job start conditional on the split job finishing (need to pass that job num)
    '''

    slurm_kwargs['job_name'] = f"{target_name}_{config}_{trackname}"
    slurm_kwargs['job_type'] = "line_pipeline_default"

    if conditional_on_jobnum is not None:
        slurm_kwargs['dependency'] = f"afterok:{conditional_on_jobnum}"

    slurm_str = cedar_slurm_setup(**slurm_kwargs)
    setup_str = cedar_job_setup(**setup_kwargs)

    job_str = \
        f'''{slurm_str}\n{setup_str}

export TRACK_FOLDER="{target_name}_{config.upper()}_{trackname}"

cd /home/ekoch/scratch/VLAXL_reduction/$TRACK_FOLDER

# Copy the rcdir here and append the pipeline path
cp -r ~/.casa .
echo "sys.path.append('/home/ekoch/scratch/VLAXL_reduction/$TRACK_FOLDER/ReductionPipeline/')" >> .casa/init.py

# Move into the continuum pipeline

cd $TRACK_FOLDER"_speclines"

echo 'Start casa default speclines pipeline'

xvfb-run -a ~/casa-pipeline-release-5.6.2-3.el7/bin/casa --rcdir ../.casa --nologger --nogui --log2term --nocrashreport --pipeline -c ../ReductionPipeline/lband_pipeline/line_pipeline.py {trackname}.speclines.ms

echo "casa default speclines pipeline finished."

        '''

    return job_str
