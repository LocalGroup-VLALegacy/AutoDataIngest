
'''
Must be run in python3
'''


def cedar_slurm_setup(job_time="72:00:00", mem="16000M",
                      job_name="M31_C_20A-346.sb38098105.eb38158028.58985.68987263889",
                      job_type="import_and_split",
                      sendto="ekoch@ualberta.ca"):

    slurm_setup = \
        f'''#!/bin/bash
#SBATCH --time={job_time}
#SBATCH --mem={mem}
#SBATCH --job-name={job_name}_{job_type}-%J
#SBATCH --output={job_name}_{job_type}-%J.out
#SBATCH --mail-user={sendto}
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL

        '''

    return slurm_setup


def cedar_job_setup():
    setup_script = \
        '''

module load nixpkgs/16.09
module load StdEnv

module load qt/4.8.7

source /home/ekoch/.bashrc

# These don't get linked properly in casaplotms.
# Force to be in known position.
ln -s $NIXDIR/libXi.so.6 $HOME/usr/
ln -s $NIXDIR/libXrandr.so.2 $HOME/usr/
ln -s $NIXDIR/libXcursor.so.1 $HOME/usr/
ln -s $NIXDIR/libXinerama.so.1 $HOME/usr/

        '''

    return setup_script
