
'''
Create a slurm submission script for the unified VLA calibration pipeline.

The new VLA pipeline (CASA 6.6 / pipeline-2024.1.x) runs a single pass that
handles import, continuum and spectral-line calibration in one job — no
separate import/split step is required.

To be run in python3
'''

from .job_tools import (cedar_slurm_setup, cedar_job_setup,
                        cedar_qa_plots, cedar_casa_startupfile,
                        path_to_casa)


def cedar_submission_script(config="C",
                            trackname="20A-346.sb38098105.eb38158028.58985.68987263889",
                            slurm_kwargs={},
                            setup_kwargs={},
                            run_qaplotter=False,
                            casa_version='6.6'):
    '''
    Runs the unified VLA L-band calibration pipeline (lband_reduction_pipeline.py).

    The track folder is {config}_{trackname} and the pipeline runs directly
    within it — no _continuum or _speclines subdirectories are created.
    '''

    track_folder = f"{config}_{trackname}"

    slurm_kwargs['job_name'] = track_folder
    slurm_kwargs['job_type'] = "unified_pipeline"

    slurm_str = cedar_slurm_setup(**slurm_kwargs)
    setup_str = cedar_job_setup(**setup_kwargs)

    startup_filename = cedar_casa_startupfile(casa6=True)
    casa_path = path_to_casa(version=casa_version)

    if run_qaplotter:
        plots_str = cedar_qa_plots()
    else:
        plots_str = ""

    data_path = "scratch/VLAXL_reduction/"

    job_str = \
        f'''{slurm_str}\n{setup_str}

export TRACK_FOLDER="{track_folder}"

cd /home/ekoch/{data_path}/$TRACK_FOLDER

# If untarred SDM directory does not exist, untar
[[ -d {trackname} ]] || tar -xf {trackname}.tar

# To save storage space, remove the tar file after extraction
if [[ -f {trackname}.tar ]]; then
    rm {trackname}.tar
fi

# Copy the offline antenna correction tables to here.
cp -r ../VLA_antcorr_tables .

# Copy manual flagging file if present
if [ -f manual_flagging.txt ]; then
    cp manual_flagging.txt manual_flagging.txt
fi

# Copy refant ignore file if present
if [ -f refant_ignore.txt ]; then
    cp refant_ignore.txt refant_ignore.txt
fi

echo 'Start casa unified VLA pipeline'

xvfb-run -a ~/{casa_path}/bin/casa --rcdir ~/.casa --nologger --nogui --log2term --nocrashreport --pipeline -c ~/ReductionPipeline/lband_pipeline/lband_reduction_pipeline.py {trackname}

# Trigger an immediate re-run attempt: this will skip completed stages and
# QA txt files. Included because repeated plotms calls can stop working.
xvfb-run -a ~/{casa_path}/bin/casa --rcdir ~/.casa --nologger --nogui --log2term --nocrashreport --pipeline -c ~/ReductionPipeline/lband_pipeline/lband_reduction_pipeline.py {trackname}

export exitcode=$?
if [ $exitcode -ge 1 ]; then
    tar -cvf $TRACK_FOLDER"_pipeline_products_failure.tar" products
    echo "Non-zero exit code from CASA. Exiting"
    exit 1
fi

# Clean up temp files
rm -f *.last *.log *.txt
rm -rf pdiff*.tbl iono.*.im flux*gain*.g rq_temp*.tbl

# Make the QA plots
{plots_str}

# Tar the products folder for export off cluster
tar -cvf $TRACK_FOLDER"_pipeline_products.tar" products

# Copy to long-term storage (with numeric suffix if name already exists)
outfolder=/home/ekoch/projects/rrg-eros-ab/ekoch/VLAXL/VLAXL_products/
name=$TRACK_FOLDER"_pipeline_products"
if [[ -e $outfolder/$name.tar || -L $outfolder/$name.tar ]] ; then
    i=1
    while [[ -e $outfolder/$name-$i.tar || -L $outfolder/$name-$i.tar ]] ; do
        let i++
        if [ "$i" -gt 100 ]; then
            break
        fi
        echo "$i"
    done
    name=$name-$i
fi
cp $TRACK_FOLDER"_pipeline_products.tar" $outfolder/$name.tar

echo "Unified VLA pipeline finished."

        '''

    return job_str


if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description='Options for creating slurm job files.')

    parser.add_argument('trackname', type=str,
                        help='VLA SDM/MS Name')

    parser.add_argument('--config', type=str, default='C',
                        help='VLA configuration')

    args = parser.parse_args()

    out_file = f"{args.config}_{args.trackname}_unified_pipeline.sh"

    print(cedar_submission_script(config=args.config,
                                  trackname=args.trackname,
                                  slurm_kwargs={},
                                  setup_kwargs={}),
          file=open(out_file, 'a'))
