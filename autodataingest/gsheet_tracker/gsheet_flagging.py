
'''
Functions for handling our google sheet with track flagging.
'''

from .gsheet_functions import do_authentication_gspread

import numpy as np
from pathlib import Path
import os

import gspread
from gspread_formatting import cellFormat, color, textFormat, format_cell_range


def read_flagsheet():
    """
    Read in the tracksheet.

    """

    gc = do_authentication_gspread()

    gsheet = gc.open("SB_Issue_Tracking")

    return gsheet


def read_track_flagsheet(trackname):
    '''
    Return the sheet for the given track name.
    '''

    gsheet = read_flagsheet()

    worksheet = gsheet.worksheet(trackname)

    return worksheet


def download_flagsheet_to_flagtxt(trackname, output_folder):
    """
    Create a txt file of the flagging commands generated in the spreadsheet.
    We will also link to the MS name to include in the file header.

    Parameters
    ----------
    trackname : str
        Name of the track name. Must be contained in the `SB_Issue_Tracking` sheet.
    """

    # TODO: add handling for the continuum and speclines parts

    worksheet = read_track_flagsheet(trackname)

    vers = 1
    max_vers = 10
    while True:

        outfilename = Path(output_folder) / f"{trackname}_manualflagging_{vers}.txt"

        if not os.path.exists(outfilename):
            break

        # Else make a new version txt file
        vers += 1

        if vers >= max_vers:
            raise ValueError(f"Reached maximum versions of {max_vers}. This seems like a bug, as you probably haven't re-done the reduction for {trackname} >{max_vers} times.")

    # Define the # rows in the header
    head_nrow = 6

    # Find the column with the flagging string in it
    # Column starts at 1, so -1 here for the slice
    flgstr_col = worksheet.find("Flag string").col - 1

    # Write flagging lines to txt file.
    with open(outfilename, "w") as outfile:

        outfile.write(f"# Manual flagging for track {trackname}\n")

        # Loop over columns with TRUE enabled for applying the flags
        applyflag_column = worksheet.col_values(4)[head_nrow:]
        rownumbers_with_flags = np.where(np.array(applyflag_column) == "TRUE")

        for row in rownumbers_with_flags:

            # Note that the counting starts at 1. So we want: row + head_nrow + 1
            row_values = worksheet.row_values(row + head_nrow + 1)

            if len(row_values[flgstr_col]) == 0:
                raise ValueError(f"Empty flag string in {trackname}. Check for mistakes in the google sheet!")

            outfile.write(f"{row_values[flgstr_col]}\n")