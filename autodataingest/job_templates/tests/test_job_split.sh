#!/bin/bash
#SBATCH --time=72:00:00
#SBATCH --mem=16000M
#SBATCH --job-name=M31_C_20A-346.sb38098105.eb38158028.58985.68987263889_import_and_split-%J
#SBATCH --output=M31_C_20A-346.sb38098105.eb38158028.58985.68987263889_import_and_split-%J.out
#SBATCH --mail-user=ekoch@ualberta.ca
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL

        


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

        

cd /home/ekoch/scratch/VLAXL_reduction/M31_C_20A-346.sb38098105.eb38158028.58985.68987263889

# If untarred directory does not exist, untar

[[ -d 20A-346.sb38098105.eb38158028.58985.68987263889 ]] || tar -xf 20A-346.sb38098105.eb38158028.58985.68987263889.tar

# Copy the rcdir here and append the pipeline path
cp -r ~/.casa .
echo "sys.path.append(os.path.abspath('ReductionPipeline/lband_pipeline/'))" >> .casa/init.py

echo 'Start casa'

xvfb-run -a ~/casa-pipeline-release-5.6.2-3.el7/bin/casa --rcdir .casa --nologger --nogui --log2term --nocrashreport --pipeline -c ReductionPipeline/lband_pipeline/ms_split.py 20A-346.sb38098105.eb38158028.58985.68987263889 all

echo "casa split finished."

        
