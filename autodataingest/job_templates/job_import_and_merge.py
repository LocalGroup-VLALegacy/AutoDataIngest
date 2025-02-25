
'''
Create a slurm submission script to convert to an MS and split the SPWs.

To be run in python3
'''

from .job_tools import (cedar_slurm_setup, cedar_job_setup,
                        cedar_casa_startupfile,
                        path_to_casa)

# from ..cluster_configs import ENDPOINT_INFO


def cedar_submission_script(target_name="M31", config="C",
                            trackname="20A-346.sb38098105.eb38158028.58985.68987263889",
                            split_type='all',
                            reindex=False,
                            slurm_kwargs={},
                            setup_kwargs={},
                            run_casa6=True,
                            casa_version='6.5'):

    # Add in default info to set the log file, job name, etc
    slurm_kwargs['job_name'] = f"{target_name}_{config}_{trackname}"
    slurm_kwargs['job_type'] = "import_and_split"

    slurm_str = cedar_slurm_setup(**slurm_kwargs)
    setup_str = cedar_job_setup(**setup_kwargs)

    startup_filename = cedar_casa_startupfile(casa6=run_casa6)

    casa_path = path_to_casa(version=casa_version)

    # Append removing any existing split directory for individual parts.
    if split_type != "all":
        remove_old_string = f'[[ -d $TRACK_FOLDER"_{split_type}" ]] || rm -rf $TRACK_FOLDER"_{split_type}"'
    else:
        remove_old_string = ""

    data_path = ENDPOINT_INFO['cc-cedar']['data_path']
    # data_path = "projects/rrg-eros-ab/ekoch/VLAXL/VLAXL_reduction/"

    job_str = \
        f'''{slurm_str}\n{setup_str}

export TRACK_FOLDER="{target_name}_{config}_{trackname}"

cd /home/ekoch/{data_path}/$TRACK_FOLDER

# If untarred directory does not exist, untar

[[ -d {trackname} ]] || tar -xf {trackname}.tar

# To save storage space, remove the tar file after it's been extracted
if [[ -f {trackname}.tar ]]; then
    rm {trackname}.tar
fi

# Remove an existing split directory
{remove_old_string}

echo 'Start casa'

~/{casa_path}/bin/casa --rcdir ~/.casa --nologger --nogui --log2term --nocrashreport --pipeline -c ~/ReductionPipeline/lband_pipeline/ms_split.py {trackname} {split_type} {reindex}

export exitcode=$?
if [ $exitcode -ge 1 ]; then
    echo "Non-zero exit code from CASA. Exiting"
    exit 1
fi

# Copy manual flag files if they're present:
if [ -f manual_flagging_speclines.txt ]; then
    cp manual_flagging_speclines.txt $TRACK_FOLDER"_speclines"/manual_flagging.txt
fi

if [ -f manual_flagging_continuum.txt ]; then
    cp manual_flagging_continuum.txt $TRACK_FOLDER"_continuum"/manual_flagging.txt
fi

# Copy refant ignore files if they are present:
if [ -f refantignore_speclines.txt ]; then
    cp refantignore_speclines.txt $TRACK_FOLDER"_speclines"/refant_ignore.txt
fi

if [ -f refantignore_continuum.txt ]; then
    cp refantignore_continuum.txt $TRACK_FOLDER"_continuum"/refant_ignore.txt
fi

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
