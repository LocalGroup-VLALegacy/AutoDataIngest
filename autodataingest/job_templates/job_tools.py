
'''
Must be run in python3
'''


def cedar_slurm_setup(job_time="72:00:00", mem="20000M",
                      ncpus=4,
                      job_name="M31_C_20A-346.sb38098105.eb38158028.58985.68987263889",
                      job_type="import_and_split",
                      sendto="ekoch@ualberta.ca",
                      dependency=None,
                      mail_complete=True,
                      mail_fail=True):

    '''
    Dependency example: --dependency=afterok:11254323
    This requires the job to wait until the job number successfully finished.
    (e.g., the split job should start before the pipeline runs)
    See https://hpc.nih.gov/docs/job_dependencies.html.
    '''

    if dependency is not None:
        dependency_str = f"#SBATCH --dependency={dependency}"
    else:
        dependency_str = ""

    mail_on_complete = "#SBATCH --mail-type=END" if mail_complete else ""
    mail_on_fail = "#SBATCH --mail-type=FAIL" if mail_fail else ""

    slurm_setup = \
        f'''#!/bin/bash
#SBATCH --time={job_time}
#SBATCH --mem={mem}
#SBATCH --cpus-per-task={ncpus}
#SBATCH --job-name={job_name}.vla_pipeline.{job_type}-%J
#SBATCH --output={job_name}_{job_type}-%J.out
#SBATCH --mail-user={sendto}
{mail_on_complete}
{mail_on_fail}
{dependency_str}

export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
        '''

    return slurm_setup


def cedar_job_setup():
    setup_script = \
        '''

module load nixpkgs/16.09 # NOTE: outdated but still needed for imagemagick
module load imagemagick/7.0.8-53
module load StdEnv

module load qt/4.8.7

source /home/ekoch/.bashrc

# This is what I had in preload.bash
export NIXDIR=/cvmfs/soft.computecanada.ca/nix/var/nix/profiles/16.09/lib/

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/usr/

export CASALD_LIBRARY_PATH=$LD_LIBRARY_PATH

# These don't get linked properly in casaplotms.
# Force to be in known position.
#ln -s $NIXDIR/libXi.so.6 $HOME/usr/
#ln -s $NIXDIR/libXrandr.so.2 $HOME/usr/
#ln -s $NIXDIR/libXcursor.so.1 $HOME/usr/
#ln -s $NIXDIR/libXinerama.so.1 $HOME/usr/
# As of 06/2024 with cedar's upgrade to RHEL8:
#ln -s $NIXDIR/libnsl.so.1 $HOME/usr/

#Xvfb :1 &
#export DISPLAY=:1

        '''

    return setup_script


def cedar_qa_plots(pythonpath='/home/ekoch/miniconda3/bin/python'):
    """
    Append code to produce the QA plots.
    """

    plot_script = \
    f'''

# Change to product directory.
cd products

{pythonpath} -c "import qaplotter; qaplotter.make_all_plots()"

cd ../

    '''

    return plot_script


def cedar_casa_startupfile(casa6=True):
    '''
    CASA 5 uses init.py on startup. CASA 6 uses config.py on startup.
    Make sure the job knows the correct startup file.
    '''

    if casa6:
        filename = 'config.py'
    else:
        filename = 'init.py'

    return filename


def path_to_casa(version='6.4'):

    if version == '6.4':
        return "casa-6.4.1-12-pipeline-2022.2.0.64"
    elif version == '6.2':
        return "casa-6.2.1-7-pipeline-2021.2.0.128"
    elif version == '6.1':
        return "casa-6.1.2-7-pipeline-2020.1.0.36"
    elif version == "5.6":
        return "casa-pipeline-release-5.6.2-3.el7"
    else:
        raise ValueError(f"No pipeline version corresponding to {version}")
