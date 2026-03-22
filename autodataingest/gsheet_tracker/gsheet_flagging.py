'''
Functions for handling the Google Sheet with track flagging (SB_Issue_Tracking).

All public functions are async and must be called with ``await`` from within
a running asyncio event loop.
'''

import asyncio
import filecmp
import os
import sys
import traceback
from pathlib import Path

import gspread                    # kept for exception types
import numpy as np
from gspread_formatting import cellFormat, color, textFormat, format_cell_range

from qaplotter.utils import datetime_from_msname

from .gsheet_functions import get_agc, read_tracksheet
from ..logging import setup_logging
log = setup_logging()


# ---------------------------------------------------------------------------
# Flagging spreadsheet access
# ---------------------------------------------------------------------------

async def read_flagsheet():
    """Open and return the 'SB_Issue_Tracking' spreadsheet."""
    agc = await get_agc()
    return await agc.open("SB_Issue_Tracking")


async def read_track_flagsheet(sheet_name: str):
    """Return the worksheet for the given tab name in SB_Issue_Tracking."""
    gsheet = await read_flagsheet()
    return await gsheet.worksheet(sheet_name)


def _abbrev_tname(trackname: str) -> str:
    """Return the abbreviated track name used as the sheet tab suffix."""
    return "_".join([
        trackname.split('.')[0],
        trackname.split('.')[3],
        trackname.split('.')[2][2:],
    ])


# ---------------------------------------------------------------------------
# Download flagging data
# ---------------------------------------------------------------------------

async def download_flagsheet_to_flagtxt(trackname: str,
                                        config: str,
                                        output_folder,
                                        raise_nosheet_exists: bool = False,
                                        raise_noflag_error: bool = True,
                                        debug: bool = False,
                                        test_against_previous: bool = True,
                                        warn: bool = True) -> Path | None:
    """
    Download manual flagging commands from the spreadsheet and write them to a
    versioned text file.

    Parameters
    ----------
    trackname:
        VLA SDM name.
    config:
        VLA array configuration string (e.g. ``'C'``).
    output_folder:
        Directory to write the flag file into.
    raise_nosheet_exists:
        If True, raise if the worksheet tab does not exist.
    raise_noflag_error:
        If True, raise if no flags are found (column 4 has no TRUE entries).
    test_against_previous:
        If True, remove the new version if it is identical to the previous one.
    """
    gsheet = await read_flagsheet()
    sheet_name = f"{config}_{_abbrev_tname(trackname)}"

    worksheets = await gsheet.worksheets()
    existing_names = [ws.title for ws in worksheets]

    if sheet_name not in existing_names:
        if raise_nosheet_exists:
            raise ValueError(f"Worksheet '{sheet_name}' does not exist.")
        log.info(f"Worksheet '{sheet_name}' does not exist. Skipping.")
        return None

    worksheet = await read_track_flagsheet(sheet_name)

    # Determine next version number
    vers, max_vers = 1, 100
    while True:
        outfilename = Path(output_folder) / f"{trackname}_manualflagging_v{vers}.txt"
        if not outfilename.exists():
            break
        vers += 1
        if vers >= max_vers:
            raise ValueError(
                f"Reached maximum flag-file versions ({max_vers}) for {trackname}. "
                "Check for a bug — you probably haven't re-reduced this track >100 times."
            )

    head_nrow = 6

    flag_col_cell = await worksheet.find("Flag string")
    flgstr_col = flag_col_cell.col - 1   # 0-based for list indexing

    applyflag_col_values = await worksheet.col_values(4)
    applyflag_column = applyflag_col_values[head_nrow:]
    rownumbers_with_flags = np.where(np.array(applyflag_column) == "TRUE")[0]

    if len(rownumbers_with_flags) == 0 and raise_noflag_error:
        raise ValueError(f"No flags found for {trackname}.")

    all_values = await worksheet.get_all_values()

    with open(outfilename, "w") as outfile:
        outfile.write(f"# Manual flagging for track {trackname}\n")
        outfile.write(f"# Config: {config}\n")

        for row in rownumbers_with_flags:
            if debug:
                log.debug(f"Flag row index: {row}")
            row_values = all_values[row + head_nrow]

            if len(row_values[flgstr_col]) == 0:
                msg = (f"Empty flag string in {trackname}. "
                       "Check for mistakes in the Google Sheet!")
                log.info(msg)
                if warn:
                    log.info("Skipping this line.")
                    continue
                else:
                    raise ValueError(msg)

            outfile.write(f"{row_values[flgstr_col]}\n")

    # Remove new version if identical to previous
    if test_against_previous and vers > 1:
        oldfilename = Path(output_folder) / f"{trackname}_manualflagging_v{vers - 1}.txt"
        if filecmp.cmp(oldfilename, outfilename, shallow=False):
            os.remove(outfilename)
            outfilename = oldfilename

    return outfilename


async def download_refant(trackname: str,
                          config: str,
                          output_folder,
                          raise_nosheet_exists: bool = False) -> Path | None:
    """
    Download the refant-ignore string from the flagging sheet and write it to a
    text file.  Returns the output path, or None if no entry exists.
    """
    gsheet = await read_flagsheet()
    sheet_name = f"{config}_{_abbrev_tname(trackname)}"

    worksheets = await gsheet.worksheets()
    if sheet_name not in [ws.title for ws in worksheets]:
        if raise_nosheet_exists:
            raise ValueError(f"Worksheet '{sheet_name}' does not exist.")
        log.info(f"Worksheet '{sheet_name}' does not exist. Skipping.")
        return None

    worksheet = await read_track_flagsheet(sheet_name)
    refant_cell = await worksheet.acell('O1')
    refant_ignore = refant_cell.value

    if not refant_ignore:
        log.info(f"No refant ignore found in flagging sheet '{sheet_name}'.")
        return None

    outfilename = Path(output_folder) / f"{trackname}_refantignore.txt"
    if outfilename.exists():
        outfilename.unlink()
    outfilename.write_text(refant_ignore)

    return outfilename


# ---------------------------------------------------------------------------
# Create / manage flagging sheets
# ---------------------------------------------------------------------------

async def make_new_flagsheet(trackname: str,
                             config: str,
                             template_name: str = 'TEMPLATE'):
    """
    Duplicate the template flagging tab and populate metadata cells.
    Returns the new (or existing) worksheet.

    Sheet tab name: ``{config}_{projcode}_{mjd}_{ebid_suffix}``
    """
    gsheet = await read_flagsheet()
    new_sheet_name = f"{config}_{_abbrev_tname(trackname)}"

    worksheets = await gsheet.worksheets()
    existing_names = [ws.title for ws in worksheets]

    if new_sheet_name in existing_names:
        log.info(f"Worksheet '{new_sheet_name}' already exists.")
        return await gsheet.worksheet(new_sheet_name)

    template_ws = await gsheet.worksheet(template_name)
    worksheet = await template_ws.duplicate(
        new_sheet_name=new_sheet_name,
        insert_sheet_index=4,
    )

    # Populate metadata cells
    obs_date = datetime_from_msname(trackname).strftime('%Y-%B-%d')
    await worksheet.update_cell(1, 5, trackname)
    await worksheet.update_cell(2, 5, obs_date)
    await worksheet.update_cell(3, 5, config)

    return worksheet


async def clear_completed_flags(target_names: list | None = None,
                                sheetnames: list | None = None,
                                test_run: bool = True) -> None:
    """
    Delete flagging sheet tabs for tracks that have reached 'imaging' status.
    Only operates when ``test_run=False``.
    """
    all_target_names = [
        'NGC6822', 'WLM', 'IC10', 'IC1613',
        'NGC4254', 'NGC628', 'NGC1087', 'NGC3627',
        'M33', 'M31',
    ]
    if target_names is None:
        target_names = all_target_names
    if sheetnames is None:
        sheetnames = ['20A - OpLog Summary', 'Archival Track Summary']

    gsheet = await read_flagsheet()
    wsheet_list = await gsheet.worksheets()
    wsheet_names = [ws.title for ws in wsheet_list]

    full_sheet = await read_tracksheet()

    for sheetname in sheetnames:
        worksheet = await full_sheet.worksheet(sheetname)
        tracks_info = await worksheet.get_all_records()

        for track in tracks_info:
            targets = [t.strip() for t in track['Targets'].split(',')]
            if not any(t in target_names for t in targets):
                continue

            trackname = track['Trackname']
            config    = track['Configuration']
            wsheet_name = f"{config}_{_abbrev_tname(trackname)}"

            if 'imaging' in track['Status: pipeline']:
                if not test_run:
                    if wsheet_name in wsheet_names:
                        try:
                            target_ws = await gsheet.worksheet(wsheet_name)
                            await gsheet.del_worksheet(target_ws)
                        except Exception:
                            traceback.print_exc()
                            log.warning(f"Unable to delete sheet: {wsheet_name}")
                    else:
                        log.warning(f"Cannot find sheet: {wsheet_name}")
                    await asyncio.sleep(5)

    await asyncio.sleep(30)


# ---------------------------------------------------------------------------
# Imaging trigger
# ---------------------------------------------------------------------------

async def check_imaging_trigger(trackname: str, config: str) -> dict:
    """
    Read the imaging submission trigger cells from the flagging sheet.

    Returns ``{'continuum': bool, 'speclines': bool}``.
    Cell addresses P1 and Q1 are placeholders pending the flagging sheet
    template update.
    """
    gsheet = await read_flagsheet()
    sheet_name = f"{config}_{_abbrev_tname(trackname)}"

    worksheets = await gsheet.worksheets()
    if sheet_name not in [ws.title for ws in worksheets]:
        log.info(f"Flagging sheet '{sheet_name}' not found; imaging trigger defaults to False.")
        return {'continuum': False, 'speclines': False}

    worksheet = await read_track_flagsheet(sheet_name)

    # TODO: confirm exact cell addresses once the flagging sheet template is updated
    submit_continuum_cell = await worksheet.acell('P1')
    submit_speclines_cell  = await worksheet.acell('Q1')

    return {
        'continuum': str(submit_continuum_cell.value).strip().upper() == 'TRUE',
        'speclines':  str(submit_speclines_cell.value).strip().upper() == 'TRUE',
    }


# ---------------------------------------------------------------------------
# Admin utilities (not used in the main pipeline loop)
# ---------------------------------------------------------------------------

async def copy_to_sheets_by_target(
        output_folder_id: str = "1vXje7cR4BdMo2tWms_Y29VpwtA0XhUkD",
        waittime: float = 60,
        waittime_per_wsheet: float = 3,
        target_names: list | None = None,
        make_other_sheets: bool = False,
        test_run: bool = True) -> None:
    """
    Copy flagging tabs to per-target spreadsheets to limit tabs on the master sheet.
    Only copies when ``test_run=False``.
    """
    all_target_names = [
        'NGC6822', 'WLM', 'IC10', 'IC1613',
        'NGC4254', 'NGC628', 'NGC1087', 'NGC3627',
        'M33', 'M31',
    ]
    if target_names is None:
        target_names = all_target_names

    skip_list = ['FRONT', 'TEMPLATE', 'TEMPLATE-SPECLINES', 'TEMPLATE-CONTINUUM', 'Testing']

    agc = await get_agc()
    gsheet = await agc.open("SB_Issue_Tracking")

    all_worksheets = await gsheet.worksheets()
    worksheet_names = [ws.title for ws in all_worksheets if ws.title not in skip_list]

    target_sheets: dict = {}
    for this_target in all_target_names:
        target_sheets[this_target] = [n for n in worksheet_names if this_target in n]
        log.info(f"Found {len(target_sheets[this_target])} sheets for {this_target}.")

    all_target_sheet_names = [n for sheets in target_sheets.values() for n in sheets]
    other_sheets = list(set(worksheet_names) - set(all_target_sheet_names))
    if other_sheets and make_other_sheets:
        log.info(f"Found {len(other_sheets)} 'other' sheets.")
        target_sheets['other'] = other_sheets

    if test_run:
        log.info("Test run — no changes made.")
        return

    for this_target in target_names:
        target_sheet_name = f"{this_target}_SB_Issue_Tracking"
        try:
            this_sheet = await agc.open(target_sheet_name)
        except Exception:
            this_sheet = await agc.create(target_sheet_name)

        existing_ws = await this_sheet.worksheets()
        existing_names = [ws.title for ws in existing_ws if ws.title not in skip_list]

        for sheetname in target_sheets.get(this_target, []):
            await asyncio.sleep(waittime_per_wsheet)
            if sheetname in existing_names:
                continue
            src_ws = await gsheet.worksheet(sheetname)
            await src_ws.copy_to(this_sheet.id)
            copy_ws = await this_sheet.worksheet(f"Copy of {sheetname}")
            await copy_ws.update_title(sheetname)

        log.info(f"Finished copying sheets for {this_target}.")
        await asyncio.sleep(waittime)


def translate_with_no_spw_reindexing(flag_sheet, start_idx: int = 0) -> None:
    """One-time migration utility — intentionally disabled."""
    raise NotImplementedError(
        "This function is intentionally disabled. Remove this guard only after "
        "backing up all flagging sheets."
    )
