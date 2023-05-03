
'''
Currently we keep all pipeline product versions including initial
and intermediate versions. This script will remove all but the final
version.
'''

import os
from glob import glob

trackprod_folder = "/home/ekoch/projects/rrg-eros-ab/ekoch/VLAXL/VLAXL_products/"

# Define all unique track name versions.

all_trackproducts = glob(f"{trackprod_folder}/*.tar")

all_tracknames = [os.path.basename(track).split("_products")[0] for track in all_trackproducts]

unique_tracknames = list(set(all_tracknames))

def sorter_key(x):

    num = os.path.basename(x).split("products")[1].strip(".tar")
    if len(num) == 0:
        return 0

    return float(num)


# Loop through the tracknames to find the last version
# That we'll keep as the final version
for this_trackname in unique_tracknames:

    all_matched_tracknames = [track for track in all_trackproducts if this_trackname in track]

    # If there's only 1, keep it:
    if len(all_matched_tracknames) == 1:
        continue


    all_matched_tracknames = sorted(all_matched_tracknames,
                                    key=sorter_key)

    # Otherwise, pop the non-numbered version to the delete
    # list, then sort the remainder.
    final_version = all_matched_tracknames[0]

    print(final_version)

    # print([os.path.basename(track) for track in all_matched_tracknames])

    all_matched_tracknames.remove(final_version)

    # print([os.path.basename(track) for track in all_matched_tracknames])

    for this_remove_track in all_matched_tracknames:
        os.remove(this_remove_track)
