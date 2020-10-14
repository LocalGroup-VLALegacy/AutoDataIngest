
'''
Functions for reading/writing to the master google sheet tracking the reduction status of VLA tracks.

Note that we have a service account attached to the gdrive ("datamanager@vlaxlgdrive.iam.gserviceaccount.com")
that the authentication is handled through

'''

import gspread


def do_authentication():
    """
    This looks for the json credentials file in `~/.config/gspread/service_account.json` and will fail
    if it doesn't find it.

    """

    gc = gspread.service_account()
    return gc


def read_tracksheet():
    """
    Read in the tracksheet.

    """

    gc = do_authentication()

    tracksheet = gc.open("20A-346 Tracks")

    return tracksheet


def find_new_tracks(sheetname='20A - OpLog Summary', status_check=''):
    """
    Find new tracks where the sheet has not recorded the data being staged from the archive on AOC.
    """

    full_sheet = read_tracksheet()

    # Find the right sheet according to sheetname

    worksheet = full_sheet.worksheet(sheetname)

    # Grab the track info.
    tracks_info = worksheet.get_all_records()

    new_tracks = []

    for track in tracks_info:
        # Check if the status is equal to `status_check`
        if track['Status'] != status_check:
            continue

        new_tracks.append(track['Exec. Block ID\n(EBID)'])

    return new_tracks
