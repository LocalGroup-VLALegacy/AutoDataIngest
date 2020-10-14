
'''
The full staging/transfer/reduction pipeline process.
'''

import sys

from email_notifications.receive_gmail_notifications import check_for_archive_notification
from gsheet_tracker.gsheet_functions import find_new_tracks
from archive_request_LG import archive_copy_SDM


# Check for new tracks

new_track_ebids = find_new_tracks()

if len(new_track_ebids) == 0:
    sys.exit(0)

# Stage the archive downloads.

for ebid in new_track_ebids[-2:]:

    print(f"Staging archive request for new EBID {ebid}")

    archive_copy_SDM(ebid)

    # Update the spreadsheet:
    # how to change row color??
    update_track_status(ebid, message="Archive download staged")
