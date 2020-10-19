
'''
The full staging/transfer/reduction pipeline process.
'''

import sys

from email_notifications.receive_gmail_notifications import check_for_archive_notification
from gsheet_tracker.gsheet_functions import (find_new_tracks, update_track_status,
                                             update_cell, return_cell)
from archive_request_LG import archive_copy_SDM
from globus_function.globus_wrapper import (transfer_file, transfer_pipeline,
                                            cleanup_source, globus_wait_for_completion)
from get_track_info import match_ebid_to_source

# Cluster where the data will be transferred to and reduced.
CLUSTERNAME = 'cc-cedar'

# Check for new tracks

new_track_ebids = find_new_tracks()

if len(new_track_ebids) == 0:
    sys.exit(0)

# Stage the archive downloads.

staged_ebids = []

for ebid in new_track_ebids[-2:]:

    print(f"Staging archive request for new EBID {ebid}")

    archive_copy_SDM(ebid)

    # Update the spreadsheet:
    update_track_status(ebid, message="Archive download staged",
                        sheetname='20A - OpLog Summary',
                        status_col=1)

    staged_ebids.append(ebid)

# Check for the archive email to come in. Once it has, start the globus transfer
transfer_taskids = []

for ebid in staged_ebids:

    out = check_for_archive_notification(ebid)

    # Not available yet. Check again later.
    if out is None:
        continue

    # Otherwise we should have the path on AOC and the full MS name
    # from the email.
    path_to_data, track_name = out

    # Update track name in sheet:
    update_cell(ebid, track_name,
                name_col=3,
                sheetname='20A - OpLog Summary')

    # Scrap the VLA archive for target and config w/ astroquery
    # This will query the archive for the list of targets until the output has a matching EBID.
    target, datasize = match_ebid_to_source(ebid,
                                            targets=['M31', 'M33', 'NGC6822', 'IC10', 'IC1613', 'WLM'],
                                            project_code='20A-346',
                                            verbose=False)

    # Add track target to the sheet
    update_cell(ebid, target, name_col=4,
                sheetname='20A - OpLog Summary')

    # And the data size
    update_cell(ebid, datasize.rstrip('GB'), name_col=14,
                sheetname='20A - OpLog Summary')

    # We want to easily track (1) target, (2) config, and (3) track name
    # We'll combine these for our folder names where the data will get placed
    # after transfer from the archive.
    config = return_cell(ebid, column=9)

    track_folder_name = f"{target}_{config}_{track_name}"

    # Do globus transfer:

    transfer_taskid = transfer_file(track_name, track_folder_name,
                                    startnode='nrao-aoc',
                                    endnode='cc-cedar',
                                    wait_for_completion=False)

    # Transfer a copy of the ReductionPipeline:
    # This has moved to pulling the github repo directly on the machine to avoid
    # an additional transfer here
    # If the cluster cannot do this for some reason, uncomment out below

    # transfer_pipeline(track_name)

    transfer_taskids.append(transfer_taskid)


    update_track_status(ebid, message=f"Data transferred to {CLUSTERNAME}",
                        sheetname='20A - OpLog Summary',
                        status_col=1)




# Wait for any active transfer to finish.
for transfer_taskid in transfer_taskids:

    # Hold at this stage
    globus_wait_for_completion(transfer_taskid)


# Submit pipeline jobs:
