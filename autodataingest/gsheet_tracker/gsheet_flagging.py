
'''
Functions for handling our google sheet with track flagging.
'''

from .gsheet_functions import do_authentication_gspread, read_tracksheet

from qaplotter.utils import datetime_from_msname

import time
import numpy as np
from pathlib import Path
import os
import filecmp

import gspread
from gspread_formatting import cellFormat, color, textFormat, format_cell_range

from ..logging import setup_logging
log = setup_logging()


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
                                  test_against_previous=True,
                                  warn=True):
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

    # Abbrev. name b/c it hits the charac. limit
    # projcode_mjd_ebid
    abbrev_tname = "_".join([trackname.split('.')[0],
                             trackname.split('.')[3],
                             trackname.split('.')[2][2:]])
    sheet_name = f"{target}_{config}_{abbrev_tname}_{data_type}"

    # Check if it exists:
    if sheet_name not in [sheet.title for sheet in gsheet.worksheets()]:
        if raise_nosheet_exists:
            raise ValueError(f"The worksheet {sheet_name} does not exist.")
        else:
            log.info(f"The worksheet {sheet_name} does not exist. Skipping")
            return None

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

            log.debug(f"On {row}")

            # Note that the counting starts at 1. So we want: row + head_nrow + 1
            # row_values = worksheet.row_values(row + head_nrow + 1)

            # But from worksheet.get_all_values() we get a list and so don't need the +1
            row_values = all_values[row + head_nrow]

            if len(row_values[flgstr_col]) == 0:
                error_str = f"Empty flag string in {trackname}. Check for mistakes in the google sheet!"
                log.info(error_str)
                if warn:
                    log.info("Skipping flag error. Ignoring this line.")
                    continue
                else:
                    raise ValueError(error_str)

            outfile.write(f"{row_values[flgstr_col]}\n")

    # Add check to see if the new version matches the previous.
    # If there's no change, remove the new version.
    if test_against_previous and vers > 1:

        newfilename = outfilename
        oldfilename = Path(output_folder) / f"{trackname}_{data_type}_manualflagging_v{vers-1}.txt"

        if filecmp.cmp(oldfilename, newfilename, shallow=False):
            os.remove(newfilename)
            outfilename = oldfilename

    return outfilename


def download_refant(trackname, target, config,
                    output_folder,
                    data_type='continuum',
                    raise_nosheet_exists=False):

    if not data_type in ['continuum', 'speclines']:
        raise ValueError(f"data_type must be 'continuum' or 'speclines'. Given {data_type}")

    gsheet = read_flagsheet()

    # Abbrev. name b/c it hits the charac. limit
    # projcode_mjd_ebid
    abbrev_tname = "_".join([trackname.split('.')[0],
                             trackname.split('.')[3],
                             trackname.split('.')[2][2:]])
    sheet_name = f"{target}_{config}_{abbrev_tname}_{data_type}"

    # Check if it exists:
    if sheet_name not in [sheet.title for sheet in gsheet.worksheets()]:
        if raise_nosheet_exists:
            raise ValueError(f"The worksheet {sheet_name} does not exist.")
        else:
            log.info(f"The worksheet {sheet_name} does not exist. Skipping")
            return None

    worksheet = read_track_flagsheet(sheet_name)

    # Get the cell value:
    refant_ignore_cell = worksheet.acell('O1').value

    if refant_ignore_cell is None:
        log.info(f"No refant ignore found in the flagging sheet for {sheet_name}")
        return None

    outfilename = Path(output_folder) / f"{trackname}_{data_type}_refantignore.txt"

    if os.path.exists(outfilename):
        os.remove(outfilename)

    with open(outfilename, "w") as outfile:

        outfile.write(refant_ignore_cell)

    return outfilename

# def download_all_flags(output_folder="manual_flags",
#                        waittime=10):
#     """
#     Download all manual flags from the flag sheet.
#     """

#     if not os.path.exists(output_folder):
#         os.mkdir(output_folder)

#     gsheet = read_flagsheet()

#     skip_list = ['FRONT', 'TEMPLATE', "Testing"]

#     worksheet_names = [worksheet.title for worksheet in gsheet.worksheets()]

#     for sheetname in worksheet_names:

#         if any([skip in sheetname for skip in skip_list]):
#             continue

#         filename = download_flagsheet_to_flagtxt(sheetname, output_folder,
#                                                  raise_noflag_error=False,
#                                                  debug=False,
#                                                  test_against_previous=True)

#         # You hit the read quota limit without some pausing
#         time.sleep(waittime)

all_target_names = ['NGC6822', 'WLM', 'IC10', 'IC1613',
                    'NGC4254', 'NGC628', 'NGC1087', 'NGC3627',
                    'M33', 'M31']


def copy_to_sheets_by_target(output_folder_id="1vXje7cR4BdMo2tWms_Y29VpwtA0XhUkD",
                             waittime=60, waittime_per_wsheet=3,
                             target_names=['NGC6822', 'WLM', 'IC10', 'IC1613',
                                           'NGC4254', 'NGC628', 'NGC1087', 'NGC3627',
                                           'M33', 'M31'],
                             make_other_sheets=False):
    """
    Copy sheets from the master sheet to new sheets per
    target. This is intended to limit the number of active tabs
    we have on the main flagging sheet.
    """

    import time

    gc = do_authentication_gspread()

    gsheet = gc.open("SB_Issue_Tracking")

    skip_list = ['FRONT',
                 'TEMPLATE',
                 'TEMPLATE-SPECLINES',
                 'TEMPLATE-CONTINUUM',
                 "Testing"]

    # Grab all sheet names
    worksheet_names = [worksheet.title for worksheet in gsheet.worksheets()
                       if worksheet.title not in skip_list]

    target_sheets = {}

    for this_target in all_target_names:
        this_target_sheets = list(filter(lambda x: this_target in x, worksheet_names))
        print(f"Found {len(this_target_sheets)} for target {this_target}")

        target_sheets[this_target] = this_target_sheets

    # Filter out any sheet not classified into the "other" category
    all_target_sheets = sum([sheets for target, sheets in target_sheets.items()],
                             [])

    other_sheets = list(set(worksheet_names) - set(all_target_sheets))
    if len(other_sheets) > 0 and make_other_sheets:
        print(f"Found {len(other_sheets)} for the 'other' category")
        target_sheets['other'] = other_sheets

    for this_target in target_names:

        target_sheet_name = f"{this_target}_SB_Issue_Tracking"

        try:
            this_sheet = gc.open(target_sheet_name, folder_id=output_folder_id)
        except:
            # Make a new sheet
            this_sheet = gc.create(target_sheet_name, folder_id=output_folder_id)

        # Check whether that sheet already exists. If so, skip it.
        existing_worksheet_names = [worksheet.title for worksheet in this_sheet.worksheets()
                                    if worksheet.title not in skip_list]
        # Copy all the sheets to the new one.
        for sheetname in target_sheets[this_target]:
            time.sleep(waittime_per_wsheet)

            if sheetname in existing_worksheet_names:
                # print(f"Skipping {sheetname} because it already exists.")
                continue

            this_worksheet = gsheet.worksheet(sheetname)
            this_worksheet.copy_to(this_sheet.id)
            this_sheet.worksheet(f"Copy of {sheetname}").update_title(sheetname)

        print(f"Finished copying sheets for target {this_target}")
        # You hit the read quota limit without some pausing
        time.sleep(waittime)


def clear_completed_flags(target_names=None,
                          sheetnames=['20A - OpLog Summary',
                                      'Archival Track Summary'],
                          test_run=True):
    '''
    Clear flagging sheets that have been completed.
    '''

    if target_names is None:
        target_names = all_target_names

    gsheet = read_flagsheet()

    full_sheet = read_tracksheet()

    for sheetname in sheetnames:

        worksheet = full_sheet.worksheet(sheetname)

        # Grab the track info.
        tracks_info = worksheet.get_all_records()

        for track in tracks_info:

            if track['Target'] not in target_names:
                continue

            trackname = track['Trackname']
            target = track['Target']
            config = track['Configuration']

            # Abbrev. name b/c it hits the charac. limit
            # projcode_mjd_ebid
            abbrev_tname = "_".join([trackname.split('.')[0],
                                     trackname.split('.')[3],
                                     trackname.split('.')[2][2:]])

            if 'imaging' in track['Status: continuum']:
                wsheet_name = f"{target}_{config}_{abbrev_tname}_continuum"
                if not test_run:
                    # Delete it!
                    try:
                        this_wsheet = gsheet.worksheet(wsheet_name)
                        gsheet.del_worksheet(this_wsheet)
                    except:
                        print(f"Cannot find {wsheet_name} to delete")

            if 'imaging' in track['Status: speclines']:
                wsheet_name = f"{target}_{config}_{abbrev_tname}_speclines"
                if not test_run:
                    # Delete it!
                    try:
                        this_wsheet = gsheet.worksheet(wsheet_name)
                        gsheet.del_worksheet(this_wsheet)
                    except:
                        print(f"Cannot find {wsheet_name} to delete")


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
    new_sheet_name = f"{target}_{config}_{abbrev_tname}_{data_type}"

    # Check if it exists:
    if new_sheet_name in [sheet.title for sheet in gsheet.worksheets()]:
        log.info(f"A worksheet with the name {new_sheet_name} already exists.")
        return gsheet.worksheet(new_sheet_name)

    worksheet = orig_worksheet.duplicate(new_sheet_name=new_sheet_name,
                                         insert_sheet_index=4)

    # Insert new metadata
    worksheet.update_cell(1, 5, trackname)
    worksheet.update_cell(2, 5, datetime_from_msname(trackname).strftime('%Y-%B-%d'))
    worksheet.update_cell(3, 5, target)
    worksheet.update_cell(4, 5, config)

    worksheet.update_cell(1, 10, data_type.upper())

    return worksheet


def translate_with_no_spw_reindexing(flag_sheet, start_idx=0):
    '''
    Eventually re-write the flagging spreadsheets to use the non reindexed SPW
    numbering.
    '''

    raise NotImplementedError("This error is here to stop this being re-run. Remove"
                              " only if you've backed everything up beforehand.")

    # Mapping dictionaries from reindex=True to reindex=False

    speclines_spw_mapping = dict.fromkeys(range(7 + 1))
    speclines_spw_mapping[0] = 0
    speclines_spw_mapping[1] = 4
    speclines_spw_mapping[2] = 5
    speclines_spw_mapping[3] = 7
    speclines_spw_mapping[4] = 8
    speclines_spw_mapping[5] = 10
    speclines_spw_mapping[6] = 11
    speclines_spw_mapping[7] = 13

    # 20 continuum SPWs in total, including the backups with the lines.
    continuum_spw_mapping = dict.fromkeys(range(19 + 1))
    continuum_spw_mapping[0] = 0
    continuum_spw_mapping[1] = 4
    continuum_spw_mapping[2] = 8
    continuum_spw_mapping[3] = 11

    # Continuum baseband starts at SPW 16.
    for ii, key in enumerate(range(4, 19 + 1)):
        continuum_spw_mapping[key] = ii + 16

    # Now loop through all of the sheets.
    # NOTE: ONLY WORKING FOR 20A-346 tracks right now!
    # there are far fewer processed archival tracks as of 10/26/2021
    # these can be done by hand

    # flag_sheet = read_flagsheet()

    # sheet_name = f"{target}_{config}_{abbrev_tname}_{data_type}"

    all_worksheets = flag_sheet.worksheets()

    total_sheets = len(all_worksheets)

    # Clip out some sheets to make this go faster for already finished sheets.
    all_worksheets = all_worksheets[start_idx:]

    time.sleep(30)

    for num, wsheet in enumerate(all_worksheets):

        time.sleep(10)

        wsheet_name = wsheet.title

        print(f"On {wsheet_name}. {num + 1 + start_idx} of {total_sheets}")

        # This only works if we know if it's continuum or speclines
        # some early flagging from summer 2020 will be skipped b/c of this
        if "continuum" not in wsheet_name and "speclines" not in wsheet_name:
            print(f"Skipping sheet {wsheet_name}")
            continue

        # Add a marker onto the sheet for when the reindexing has already been done
        # if found, don't do it again!
        reindex_cell = wsheet.cell(1, 17).value
        if reindex_cell == "REINDEXED":
            print(f"Already reindexed {wsheet_name}. Continuing")
            continue


        # Split out the project code and the data type
        proj_code = wsheet_name.split("_")[2]
        data_type = wsheet_name.split("_")[-1]

        if proj_code != "20A-346":
            print(f"Reindexing only working for 20A-346. Skipping {wsheet_name}.")
            continue

        if data_type == "continuum":
            spw_mapping = continuum_spw_mapping
        elif data_type == "speclines":
            spw_mapping = speclines_spw_mapping
        else:
            raise ValueError(f"Unable to identify spw_mapping for: {data_type}")

        # We want the spw column. Nothing else should change.
        column = 8
        start_row = 7

        all_spw_values = wsheet.get("H:H")[start_row - 1:]

        if len(all_spw_values) == 0:
            print("No SPW information. Continuing")
            time.sleep(1)
            wsheet.update_cell(1, 17, "REINDEXED")
            continue

        for idx, this_cell_value in enumerate(all_spw_values):

            time.sleep(0.5)

            row = start_row + idx

            # if row >= wsheet.row_count:
            #     break

            # Get cell value
            # this_cell = wsheet.cell(row, column)

            # this_cell_value = this_cell.value

            if len(this_cell_value)  == 0:
                continue

            this_cell_value = this_cell_value[0]

            # Split SPW and channel is ":" is in the value
            spws_chans = this_cell_value.split(":")

            if len(spws_chans) == 1:
                spws = spws_chans[0]
                chans = None
            elif len(spws_chans) == 2:
                spws, chans = this_cell_value.split(":")
            else:
                raise ValueError(f"Unable to process SPW cell: {this_cell_value} in {wsheet_name}")

            # spw_list = [int(val) for val in spws.split(",")]
            new_spw_list = []
            for these_spws in spws.split(","):
                if "~" in these_spws:
                    spw_init, spw_end = these_spws.split("~")
                    spw_init = int(spw_init)
                    spw_end = int(spw_end)
                    new_spw_list.append(f"{spw_mapping[spw_init]}~{spw_mapping[spw_end]}")
                else:
                    new_spw_list.append(spw_mapping[int(these_spws)])

            new_spws = ",".join([str(this_spw) for this_spw in new_spw_list])

            if chans is None:
                new_spws_chans = new_spws
            else:
                new_spws_chans = ":".join([new_spws, chans])

            time.sleep(0.5)

            wsheet.update_cell(row, column, new_spws_chans)

        time.sleep(0.5)

        # Once finished, indicate the sheet was reindexed.
        wsheet.update_cell(1, 17, "REINDEXED")
