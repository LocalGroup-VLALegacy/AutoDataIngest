
'''
Functions for reading/writing to the master google sheet tracking the reduction status of VLA tracks.

Note that we have a service account attached to the gdrive ("datamanager@vlaxlgdrive.iam.gserviceaccount.com")
that the authentication is handled through

'''

import gspread
from gspread_formatting import cellFormat, color, textFormat, format_cell_range


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


stage_colors = {'Archive download staged': {'row_color': [1., 1., 0.3],
                                            'text_color': [0., 0., 0.],
                                            'bold_text': False}}


def update_track_status(ebid, message="Archive download staged",
                        sheetname='20A - OpLog Summary',
                        status_col=1,
                        bool_status_colname="Staged data \nfrom archive",
                        row_color=[1., 1., 1.],
                        text_color=[0., 0., 0.],
                        bold_text=False):
    """
    docstring
    """

    full_sheet = read_tracksheet()
    worksheet = full_sheet.worksheet(sheetname)

    cell = worksheet.find(str(ebid))

    worksheet.update_cell(cell.row, status_col, message)

    # Update the boolean flags for the different stages.
    bool_cell_col = worksheet.find(bool_status_colname).col
    worksheet.update_cell(cell.row, bool_cell_col, "TRUE")

    # Check if we have a color to update for the row at this stage:
    if message in stage_colors:
        row_color = stage_colors[message]['row_color']
        text_color = stage_colors[message]['text_color']
        bold_text = stage_colors[message]['bold_text']

    fmt = cellFormat(backgroundColor=color(*row_color),
                        textFormat=textFormat(bold=bold_text,
                                              foregroundColor=color(*text_color)))

    format_cell_range(worksheet, f'{cell.row}', fmt)
