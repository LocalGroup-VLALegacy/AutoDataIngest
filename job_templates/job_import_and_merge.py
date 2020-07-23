
'''
Create a slurm submission script to convert to an MS and split the SPWs.

To be run in python3
'''

from job_tools import cedar_slurm_setup, cedar_job_setup


def cedar_submission_script(target_name="M31", config="C",
                            trackname="20A-346.sb38098105.eb38158028.58985.68987263889",
                            slurm_kwargs={},
                            setup_kwargs={}):

    # Add in default info to set the log file, job name, etc
    slurm_kwargs['job_name'] = f"{target_name}_{config}_{trackname}"
    slurm_kwargs['job_type'] = "import_and_split"

    slurm_str = cedar_slurm_setup(**slurm_kwargs)
    setup_str = cedar_job_setup(**setup_kwargs)

    job_str = \
        f'''{slurm_str}\n{setup_str}

cd /home/ekoch/scratch/VLAXL_reduction/{target_name}_{config.upper()}_{trackname}

# If untarred directory does not exist, untar

[[ -d {trackname} ]] || tar -xf {trackname}.tar

# Copy the rcdir here and append the pipeline path
cp -r ~/.casa .
echo "sys.path.append('/home/ekoch/scratch/VLAXL_reduction/$TRACK_FOLDER/ReductionPipeline/')" >> .casa/init.py

echo 'Start casa'

xvfb-run -a ~/casa-pipeline-release-5.6.2-3.el7/bin/casa --rcdir .casa --nologger --nogui --log2term --nocrashreport --pipeline -c ReductionPipeline/lband_pipeline/ms_split.py {trackname} all

echo "casa split finished."

        '''

    return job_str


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description='Options for creating slurm job files.')

    parser.add_argument('trackname', type=str,
                        help='VLA SDM/MS Name')

    parser.add_argument('--target_name', type=str, default='M31',
                        help='Target name (e.g., M31)')

    parser.add_argument('--config', type=str, default='C',
                        help='VLA configuration')

    args = parser.parse_args()

    out_file = f"{args.target_name}_{args.config}_{args.trackname}_split.sh"

    print(cedar_submission_script(target_name=args.target_name,
                                  config=args.config,
                                  trackname=args.trackname,
                                  slurm_kwargs={},  # Keep defaults
                                  setup_kwargs={}),
          file=open(out_file, 'a'))
