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

# These don't get linked properly in casaplotms.
# Force to be in known position.
ln -s $NIXDIR/libXi.so.6 $HOME/usr/
ln -s $NIXDIR/libXrandr.so.2 $HOME/usr/
ln -s $NIXDIR/libXcursor.so.1 $HOME/usr/
ln -s $NIXDIR/libXinerama.so.1 $HOME/usr/

        

cd /home/ekoch/scratch/VLAXL_reduction/M31_C_20A-346.sb38098105.eb38158028.58985.68987263889

tar -xf 20A-346.sb38098105.eb38158028.58985.68987263889.tar

echo 'Start casa'

xvfb-run -a ~/casa-pipeline-release-5.6.2-3.el7/bin/casa --nologger --nogui --log2term --nocrashreport --pipeline -c ReductionPipeline/ms_split.py 20A-346.sb38098105.eb38158028.58985.68987263889 all

echo "casa split finished."

        
