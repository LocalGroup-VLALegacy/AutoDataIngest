
'''
Functions for handling our google sheet with track flagging.
'''

from .gsheet_functions import do_authentication_gspread

import time
import numpy as np
from pathlib import Path
import os
import filecmp

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


def download_flagsheet_to_flagtxt(trackname, output_folder,
                                  raise_noflag_error=True,
                                  debug=False,
                                  test_against_previous=True):
    """
    Create a txt file of the flagging commands generated in the spreadsheet.
    We will also link to the MS name to include in the file header.

    Parameters
    ----------
    trackname : str
        Name of the track name. Must be contained in the `SB_Issue_Tracking` sheet.
    output_folder : str
        Existing output folder to save to.
    raise_noflag_error : bool, optional
        Raises an error if no valid flags are found. When False, we allow empty
        flag files to be written.
    debug : bool, optional
        Print to terminal the rows that are being used for valid flags.
    """

    # TODO: add handling for the continuum and speclines parts

    worksheet = read_track_flagsheet(trackname)

    vers = 1
    max_vers = 10
    while True:

        outfilename = Path(output_folder) / f"{trackname}_manualflagging_v{vers}.txt"

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

        # Loop over columns with TRUE enabled for applying the flags
        applyflag_column = worksheet.col_values(4)[head_nrow:]
        rownumbers_with_flags = np.where(np.array(applyflag_column) == "TRUE")[0]

        if len(rownumbers_with_flags) == 0 and raise_noflag_error:
            raise ValueError(f"No flags found for {trackname}")

        outfile.write(f"# Manual flagging for track {trackname} version {vers}\n")

        for row in rownumbers_with_flags:

            if debug:
                print(f"On {row}")

            # Note that the counting starts at 1. So we want: row + head_nrow + 1
            row_values = worksheet.row_values(row + head_nrow + 1)

            if len(row_values[flgstr_col]) == 0:
                raise ValueError(f"Empty flag string in {trackname}. Check for mistakes in the google sheet!")

            outfile.write(f"{row_values[flgstr_col]}\n")

    # Add check to see if the new version matches the previous.
    # If there's no change, remove the new version.
    if test_against_previous and vers > 1:

        newfilename = outfilename
        oldfilename = Path(output_folder) / f"{trackname}_manualflagging_v{vers-1}.txt"

        if filecmp.cmp(oldfilename, newfilename, shallow=False):
            os.remove(newfilename)


def download_all_flags(output_folder="manual_flags",
                       waittime=30):
    """
    Download all manual flags from the flag sheet.
    """

    if not os.path.exists(output_folder):
        os.mkdir(output_folder)

    gsheet = read_flagsheet()

    skip_list = ['FRONT', 'TEMPLATE', "Testing"]

    worksheet_names = [worksheet.title for worksheet in gsheet.worksheets()]

    for sheetname in worksheet_names:

        if any([skip in sheetname for skip in skip_list]):
            continue

        download_flagsheet_to_flagtxt(sheetname, output_folder,
                                      raise_noflag_error=False,
                                      debug=False,
                                      test_against_previous=True)

        # You hit the read quota limit without some pausing
        time.sleep(waittime)
