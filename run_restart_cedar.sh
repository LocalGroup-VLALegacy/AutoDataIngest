
# Helper script for quickly restarting jobs that have timed out.

# Usage: ./run_restart_cedar.sh trackname tracktype
# trackname should be the name of the track, NOT including the source and config
# e.g.20A-346.sb38098105.eb38158028.58985.68987263889
# tracktype should be either 'continuum' or 'speclines'

trackname=$1
tracktype=$2

# Change to the directory of the track

cd scratch/VLAXL_reduction/

fulltrackname=$(ls . | grep *${trackname})

cd $fulltrackname

# Remove any intermediate imaging products

rm -rf ${fulltrackname}_${tracktype}/quicklook_imaging/*.psf
rm -rf ${fulltrackname}_${tracktype}/quicklook_imaging/*.residual
rm -rf ${fulltrackname}_${tracktype}/quicklook_imaging/*.image
rm -rf ${fulltrackname}_${tracktype}/quicklook_imaging/*.pb
rm -rf ${fulltrackname}_${tracktype}/quicklook_imaging/*.sumwt
rm -rf ${fulltrackname}_${tracktype}/quicklook_imaging/*.weight
rm -rf ${fulltrackname}_${tracktype}/quicklook_imaging/*.mask

# and resubmit the pipeline script

if [ ${tracktype} == 'continuum' ]; then
    # ls "${fulltrackname}_job_${tracktype}.sh" | xargs sbatch --dependency=
    sbatch --dependency= ${fulltrackname}_job_${tracktype}.sh
elif [ ${tracktype} == 'speclines' ]; then
    sbatch --dependency= ${fulltrackname}_job_line.sh
else
    echo "tracktype must be either 'continuum' or 'speclines'"
    echo $tracktype
fi
