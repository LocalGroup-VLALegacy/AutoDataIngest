
'''
Must be run in python3
'''


def cedar_slurm_setup(job_time="72:00:00", mem="16000M",
                      job_name="M31_C_20A-346.sb38098105.eb38158028.58985.68987263889",
                      job_type="import_and_split",
                      sendto="ekoch@ualberta.ca",
                      dependency=None):

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

    slurm_setup = \
        f'''#!/bin/bash
#SBATCH --time={job_time}
#SBATCH --mem={mem}
#SBATCH --job-name={job_name}.vla_pipeline.{job_type}-%J
#SBATCH --output={job_name}_{job_type}-%J.out
#SBATCH --mail-user={sendto}
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL
{dependency_str}
        '''

    return slurm_setup


def cedar_job_setup():
    setup_script = \
        '''

module load nixpkgs/16.09
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

Xvfb :1 &
export DISPLAY=:1

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
