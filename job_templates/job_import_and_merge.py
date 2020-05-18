
'''
Create a slurm submission script to convert to an MS and split the SPWs.

To be run in python3
'''

from job_tools import cedar_slurm_setup, cedar_job_setup


def cedar_submission_script(target_name="M31", config="C",
                            trackname="20A-346.sb38098105.eb38158028.58985.68987263889",
                            slurm_kwargs={},
                            setup_kwargs={}):

    slurm_str = cedar_slurm_setup(**slurm_kwargs)
    setup_str = cedar_job_setup(**setup_kwargs)

    job_str = \
        f'''{slurm_str}\n{setup_str}

cd /home/ekoch/scratch/VLAXL_reduction/{target_name}_{config.upper()}_{trackname}

tar -xf {trackname}.tar

echo 'Start casa'

xvfb-run -a ~/casa-pipeline-release-5.6.2-3.el7/bin/casa --nologger --nogui --log2term --nocrashreport --pipeline -c ReductionPipeline/ms_split.py {trackname} all

echo "casa split finished."

        '''

    return job_str
