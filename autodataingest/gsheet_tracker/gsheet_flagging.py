
'''
Functions for handling our google sheet with track flagging.
'''

from .gsheet_functions import do_authentication_gspread

from qa_plotter import datetime_from_msname

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


def download_flagsheet_to_flagtxt(trackname, target, config,
                                  output_folder,
                                  data_type='continuum',
                                  raise_nosheet_exists=False,
                                  raise_noflag_error=True,
                                  debug=False,
                                  test_against_previous=True):
    """
    Create a txt file of the flagging commands generated in the spreadsheet.
    We will also link to the MS name to include in the file header.

    Parameters
    ----------
    TODO: add description for all inputs
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

    if not data_type in ['continuum', 'speclines']:
        raise ValueError(f"data_type must be 'continuum' or 'speclines'. Given {data_type}")

    gsheet = read_flagsheet()

    orig_worksheet = gsheet.worksheet(template_name)

    # Abbrev. name b/c it hits the charac. limit
    # projcode_mjd_ebid
    abbrev_tname = "_".join([trackname.split('.')[0],
                             trackname.split('.')[3],
                             trackname.split('.')[2][2:]])
    sheet_name = f"{target}_{config}_{abbrev}_{data_type}"

    # Check if it exists:
    if new_sheet_name not in [sheet.title for sheet in gsheet.worksheets()]:
        if raise_nosheet_exists:
            raise ValueError(f"The worksheet {new_sheet_name} does not exist.")
        else:
            print((f"The worksheet {new_sheet_name} does not exist. Skipping")
            return

    worksheet = read_track_flagsheet(sheet_name)

    vers = 1
    max_vers = 100
    while True:

        outfilename = Path(output_folder) / f"{trackname}_{data_type}_manualflagging_v{vers}.txt"

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

        outfile.write(f"# Manual flagging for track {trackname} {data_type}\n")
        outfile.write(f"# Target: {target} Config: {config}\n")

        # Do single read in of the whole sheet.
        all_values = worksheet.get_all_values()

        for row in rownumbers_with_flags:

            if debug:
                print(f"On {row}")

            # Note that the counting starts at 1. So we want: row + head_nrow + 1
            # row_values = worksheet.row_values(row + head_nrow + 1)

            # But from worksheet.get_all_values() we get a list and so don't need the +1
            row_values = all_values[row + head_nrow]

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
                       waittime=10):
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


def make_new_flagsheet(trackname, target, config,
                       data_type='continuum',
                       template_name='TEMPLATE'):
    '''
    Copy the template flagging sheet to a new sheet for the track.
    Return the new sheet
    '''

    if not data_type in ['continuum', 'speclines']:
        raise ValueError(f"data_type must be 'continuum' or 'speclines'. Given {data_type}")

    gsheet = read_flagsheet()

    orig_worksheet = gsheet.worksheet(template_name)

    # Abbrev. name b/c it hits the charac. limit
    # projcode_mjd_ebid
    abbrev_tname = "_".join([trackname.split('.')[0],
                             trackname.split('.')[3],
                             trackname.split('.')[2][2:]])
    new_sheet_name = f"{target}_{config}_{abbrev}_{data_type}"

    # Check if it exists:
    if new_sheet_name in [sheet.title for sheet in gsheet.worksheets()]:
        print(f"A worksheet with the name {new_sheet_name} already exists.")

    worksheet = orig_worksheet.duplicate(new_sheet_name=new_sheet_name,
                                         insert_sheet_index=2)

    # Insert new metadata
    worksheet.update_cell(1, 5, trackname)
    worksheet.update_cell(2, 5, datetime_from_msname(trackname))
    worksheet.update_cell(3, 5, target)
    worksheet.update_cell(4, 5, config)

    worksheet.update_cell(1, 10, data_type.upper())

    return worksheet
