
'''
Create a slurm submission script to convert to an MS and split the SPWs.

To be run in python3
'''

from .job_tools import (cedar_slurm_setup, cedar_job_setup,
                        cedar_qa_plots, cedar_casa_startupfile,
                        path_to_casa)


def cedar_submission_script(target_name="M31",
                            config="C",
                            trackname="20A-346.sb38098105.eb38158028.58985.68987263889",
                            slurm_kwargs={},
                            setup_kwargs={},
                            conditional_on_jobnum=None,
                            run_casa6=True,
                            run_qaplotter=False,
                            casa_version='6.5'):
    '''
    Runs the default VLA pipeline.

    TODO: Make job start conditional on the split job finishing (need to pass that job num)
    '''

    slurm_kwargs['job_name'] = f"{target_name}_{config}_{trackname}"
    slurm_kwargs['job_type'] = "continuum_pipeline_default"

    if conditional_on_jobnum is not None:
        slurm_kwargs['dependency'] = f"afterok:{conditional_on_jobnum}"

    slurm_str = cedar_slurm_setup(**slurm_kwargs)
    setup_str = cedar_job_setup(**setup_kwargs)

    startup_filename = cedar_casa_startupfile(casa6=run_casa6)

    casa_path = path_to_casa(version=casa_version)

    if run_qaplotter:
        plots_str = cedar_qa_plots()
    else:
        plots_str = ""

    job_str = \
        f'''{slurm_str}\n{setup_str}

export TRACK_FOLDER="{target_name}_{config}_{trackname}"

cd /home/ekoch/scratch/VLAXL_reduction/$TRACK_FOLDER

# Move into the continuum pipeline

cd $TRACK_FOLDER"_continuum"

# Copy the offline ant correction tables to here.
cp -r ../VLA_antcorr_tables .
cp ../manual_flagging_continuum.txt manual_flagging.txt

echo 'Start casa default continuum pipeline'

xvfb-run -a ~/{casa_path}/bin/casa --rcdir ~/.casa --nologger --nogui --log2term --nocrashreport --pipeline -c ~/ReductionPipeline/lband_pipeline/continuum_pipeline.py {trackname}.continuum.ms

# Trigger an immediate re-run attempt: This will skip completed parts and QA txt files.
# It's here because repeated plotms calls seem to stop working after awhile.
xvfb-run -a ~/{casa_path}/bin/casa --rcdir ~/.casa --nologger --nogui --log2term --nocrashreport --pipeline -c ~/ReductionPipeline/lband_pipeline/continuum_pipeline.py {trackname}.continuum.ms

export exitcode=$?
if [ $exitcode -ge 1 ]; then
    tar -cvf $TRACK_FOLDER"_continuum_products_failure.tar" products
    mv $TRACK_FOLDER"_continuum_products_failure.tar" $SCRATCH_FOLDER/$TRACK_FOLDER"_continuum"/
    echo "Non-zero exit code from CASA. Exiting"
    exit 1
fi

# Clean up temp files
rm *.last *.log *.txt
rm -rf pdiff*.tbl iono.*.im flux*gain*.g rq_temp*.tbl

# Make the QA plots
{plots_str}\n

# Tar the products folder for export off cedar
tar -cvf $TRACK_FOLDER"_continuum_products.tar" products

# Copy to long term storage
# Account for previous runs and label numerically
outfolder=/home/ekoch/projects/rrg-eros-ab/ekoch/VLAXL/VLAXL_products/
name=$TRACK_FOLDER"_continuum_products"
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

cp $TRACK_FOLDER"_continuum_products.tar" $outfolder/$name.tar

# Tar the MS file.

# As of 10/25/21 we split the calibrated column into a target and calibrator part.
tar -cf "{target_name}_{config}_{trackname}.continuum.ms.split.tar" "{trackname}.continuum.ms.split"
tar -cf "{target_name}_{config}_{trackname}.continuum.ms.split_calibrators.tar" "{trackname}.continuum.ms.split_calibrators"

# Remove the original tar file to save space
rm -r "{trackname}.continuum.ms"
rm -r "{trackname}.continuum.ms.flagversions"
rm -r calibrators.ms finalcalibrators.ms
rm -r "{trackname}.continuum.ms.split"
rm -r "{trackname}.continuum.ms.split_calibrators"

echo "casa default continuum pipeline finished."

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

    out_file = f"{args.target_name}_{args.config}_{args.trackname}_continuum_pipeline.sh"

    print(cedar_submission_script(target_name=args.target_name,
                                  config=args.config,
                                  trackname=args.trackname,
                                  slurm_kwargs={},  # Keep defaults
                                  setup_kwargs={},
                                  conditional_on_jobnum=conditional_on_jobnum),
          file=open(out_file, 'a'))
