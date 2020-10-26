
'''
Create a slurm submission script to convert to an MS and split the SPWs.

To be run in python3
'''

from .job_tools import cedar_slurm_setup, cedar_job_setup


def cedar_submission_script(target_name="M31",
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

~/casa-pipeline-release-5.6.2-3.el7/bin/casa --rcdir ../.casa --nologger --nogui --log2term --nocrashreport --pipeline -c ../ReductionPipeline/lband_pipeline/line_pipeline.py {trackname}.speclines.ms

# Copy the casa log file into the products folder
cp casa*.log products/

# Clean up small unneeded files
rm *.last

# Tar the products folder for export off cedar
tar -cvf $TRACK_FOLDER"_speclines_products.tar" products

# Copy to long term storage
# Account for previous runs and label numerically
outfolder=/home/ekoch/projects/rpp-pbarmby/ekoch/VLAXL_products/
name=$TRACK_FOLDER"_speclines_products"
if [[ -e $outfolder/$name.tar || -L $outfolder/$name.tar ]] ; then
    i=1
    while [[ -e $outfolder/$name-$i.tar || -L $outfolder/$name-$i.tar ]] ; do
        let i++

        # Build in check to avoid endless loop.
        if [ "$i" -gt 100 ]; then
            break
        fi

        echo "$i"

    done
    name=$name-$i
fi

cp $TRACK_FOLDER"_speclines_products.tar" $outfolder/$name.tar

echo "casa default speclines pipeline finished."

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

    parser.add_argument('--conditional_on_jobnum', type=str, default='none',
                        help='Job number this job is conditional on finishing.')

    args = parser.parse_args()

    conditional_on_jobnum = None if args.conditional_on_jobnum == 'none' else args.conditional_on_jobnum

    out_file = f"{args.target_name}_{args.config}_{args.trackname}_line_pipeline.sh"

    print(cedar_submission_script_default(target_name=args.target_name,
                                          config=args.config,
                                          trackname=args.trackname,
                                          slurm_kwargs={},  # Keep defaults
                                          setup_kwargs={},
                                          conditional_on_jobnum=conditional_on_jobnum),
          file=open(out_file, 'a'))
