'''
Functions for reading/writing to the master Google Sheet tracking the reduction
status of VLA tracks.

Authentication uses a service account JSON credential file at:
    ~/.config/gspread/service_account.json
(same location as the old gspread.service_account() default).

All public functions are async and must be called with ``await`` from within
a running asyncio event loop.
'''

import asyncio
import string
from datetime import datetime
from pathlib import Path

import gspread                    # kept for exception types only
import gspread_asyncio
import requests                   # for ReadTimeout in update_track_status
from google.oauth2.service_account import Credentials
from gspread_formatting import cellFormat, color, textFormat, format_cell_range

from ..logging import setup_logging
log = setup_logging()


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def _make_credentials() -> Credentials:
    """Load service account credentials from the default gspread file location."""
    creds_path = Path.home() / '.config' / 'gspread' / 'service_account.json'
    creds = Credentials.from_service_account_file(str(creds_path))
    return creds.with_scopes([
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets',
    ])


# Shared client manager — handles token refresh automatically; safe to use
# concurrently from multiple coroutines.
_agcm = gspread_asyncio.AsyncioGspreadClientManager(_make_credentials)


async def get_agc() -> gspread_asyncio.AsyncioGspreadClient:
    """Return an authorised async gspread client."""
    return await _agcm.authorize()


# ---------------------------------------------------------------------------
# Spreadsheet access
# ---------------------------------------------------------------------------

async def read_tracksheet() -> gspread_asyncio.AsyncioGspreadSpreadsheet:
    """Open and return the '20A-346 Tracks' spreadsheet."""
    agc = await get_agc()
    return await agc.open("20A-346 Tracks")


# ---------------------------------------------------------------------------
# Track discovery
# ---------------------------------------------------------------------------

async def find_new_tracks(sheetname: str = '20A - OpLog Summary',
                          status_check: str = '') -> list:
    """
    Return EBIDs where ``Status: pipeline`` equals ``status_check`` (default: empty).
    """
    full_sheet = await read_tracksheet()
    worksheet = await full_sheet.worksheet(sheetname)
    tracks_info = await worksheet.get_all_records()

    return [
        track['EBID']
        for track in tracks_info
        if track['Status: pipeline'] == status_check
    ]


async def find_rerun_status_tracks(sheetname: str = '20A - OpLog Summary',
                                   job_type: str | None = None,
                                   max_per_exec: int = 10) -> list:
    """
    Find tracks with a re-run trigger set and timestamp the ``Prev pipeline status``
    column.  Returns ``[[ebid, [['pipeline', type]]], ...]``.
    """
    full_sheet = await read_tracksheet()
    worksheet = await full_sheet.worksheet(sheetname)
    tracks_info = await worksheet.get_all_records()

    new_tracks = []
    if job_type is None:
        job_type = 'ALL'

    for track in tracks_info:
        run_types = []

        await asyncio.sleep(5)   # rate-limit Sheets API writes below

        if len(track['Re-run pipeline']) > 0:
            time_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            this_type = track['Re-run pipeline']

            if job_type != 'ALL' and job_type not in this_type:
                log.info(f"Skipping {this_type} — not a {job_type} job.")
            else:
                pipeline_trigger = f"{this_type} at {time_stamp}"
                await update_cell(track['EBID'],
                                  pipeline_trigger,
                                  name_col='Prev pipeline status',
                                  sheetname=sheetname)
                run_types.append(['pipeline', this_type])
                new_tracks.append([track['EBID'], run_types])

        if len(new_tracks) >= max_per_exec:
            log.info(f"Hit maximum jobs per execution: {max_per_exec}")
            break

    return new_tracks


async def find_running_tracks(sheetname: str = '20A - OpLog Summary',
                              status_check: str = 'Reduction running') -> list:
    """
    Return ``[ebid, 'pipeline', job_id]`` for every track whose
    ``Status: pipeline`` contains ``status_check``.
    """
    full_sheet = await read_tracksheet()
    worksheet = await full_sheet.worksheet(sheetname)
    tracks_info = await worksheet.get_all_records()

    return [
        [track['EBID'], "pipeline", track['Pipeline job ID']]
        for track in tracks_info
        if status_check in track['Status: pipeline']
    ]


async def return_all_ebids(sheetname: str = '20A - OpLog Summary') -> list:
    full_sheet = await read_tracksheet()
    worksheet = await full_sheet.worksheet(sheetname)
    ebids = await worksheet.col_values(7)
    # Drop header and empty entries
    return [ebid for ebid in ebids if ebid][1:]


# ---------------------------------------------------------------------------
# Row / cell updates
# ---------------------------------------------------------------------------

# Row highlight colours keyed on status message prefix
stage_colors = {
    'Archive download staged': {'row_color': [0.9254901960784314, 0.8823529411764706, 0.2],
                                'text_color': [0., 0., 0.], 'bold_text': False},
    'Queued':                  {'row_color': [0.9254901960784314, 0.8823529411764706, 0.2],
                                'text_color': [0., 0., 0.], 'bold_text': False},
    'Data transferred to':     {'row_color': [0.33725490196078434, 0.7058823529411765, 0.9137254901960784],
                                'text_color': [0., 0., 0.], 'bold_text': False},
    'Reduction running on':    {'row_color': [0.00392156862745098, 0.45098039215686275, 0.6980392156862745],
                                'text_color': [1., 1., 1.], 'bold_text': False},
    'Ready for QA':            {'row_color': [0.8352941176470589, 0.3686274509803922, 0.0],
                                'text_color': [1., 1., 1.], 'bold_text': False},
    'Ready for imaging':       {'row_color': [0.00784313725490196, 0.6196078431372549, 0.45098039215686275],
                                'text_color': [1., 1., 1.], 'bold_text': True},
    'Restarting pipeline':     {'row_color': [0.00392156862745098, 0.45098039215686275, 0.6980392156862745],
                                'text_color': [1., 1., 1.], 'bold_text': False},
    'ISSUE':                   {'row_color': [0., 0., 0.], 'text_color': [1., 1., 1.], 'bold_text': False},
    'FAILED':                  {'row_color': [0., 0., 0.], 'text_color': [1., 1., 1.], 'bold_text': False},
    'HELP':                    {'row_color': [0.8, 0.47058823529411764, 0.7372549019607844],
                                'text_color': [1., 1., 1.], 'bold_text': False},
}


async def update_track_status(ebid,
                              message: str = "Archive download staged",
                              sheetname: str = '20A - OpLog Summary',
                              status_col: int = 1,
                              bool_status_colname: str = "Staged data \nfrom archive",
                              row_color: list = None,
                              text_color: list = None,
                              bold_text: bool = False,
                              max_retry: int = 5) -> None:
    """Update the processing status of a track and colour the row."""
    if row_color is None:
        row_color = [1., 1., 1.]
    if text_color is None:
        text_color = [0., 0., 0.]

    worksheet = None
    for attempt in range(max_retry):
        try:
            full_sheet = await read_tracksheet()
            worksheet = await full_sheet.worksheet(sheetname)
            break
        except requests.ReadTimeout:
            await asyncio.sleep(10)
    if worksheet is None:
        raise ValueError("Timed out multiple times reading Google Sheet.")

    cell = await worksheet.find(str(ebid))
    await worksheet.update_cell(cell.row, status_col, message)

    bool_col_cell = await worksheet.find(bool_status_colname)
    await worksheet.update_cell(cell.row, bool_col_cell.col, "TRUE")

    # Determine row colour from status message
    key_match = [key for key in stage_colors if key in message]
    if len(key_match) > 1:
        log.info(f"Multiple matching statuses found: {key_match}. Using the first.")
    if key_match:
        key = key_match[0]
        row_color  = stage_colors[key]['row_color']
        text_color = stage_colors[key]['text_color']
        bold_text  = stage_colors[key]['bold_text']

    fmt = cellFormat(
        backgroundColor=color(*row_color),
        textFormat=textFormat(bold=bold_text, foregroundColor=color(*text_color)),
    )
    cell_range = f'{string.ascii_uppercase[status_col - 1]}{cell.row}'

    # format_cell_range is synchronous — run in thread pool so we don't block the loop
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, format_cell_range, worksheet.ws, cell_range, fmt)


async def update_cell(ebid,
                      value,
                      name_col: str | None = None,
                      num_col: int = 3,
                      sheetname: str = '20A - OpLog Summary') -> None:
    """
    Update a single cell for the row identified by ``ebid``.

    Parameters
    ----------
    name_col:
        Column header string.  When given, overrides ``num_col``.
    num_col:
        1-based column index (used when ``name_col`` is None or not found).
    """
    if name_col is None and num_col is None:
        raise ValueError("Either name_col or num_col must be provided.")

    full_sheet = await read_tracksheet()
    worksheet = await full_sheet.worksheet(sheetname)

    if name_col is not None:
        try:
            col_cell = await worksheet.find(name_col)
            num_col = col_cell.col
        except gspread.exceptions.CellNotFound:
            log.exception(f"Column '{name_col}' not found; falling back to num_col={num_col}.")

    cell = await worksheet.find(str(ebid))
    await worksheet.update_cell(cell.row, num_col, value)


async def return_cell(ebid,
                      name_col: str | None = None,
                      column: int = 9,
                      sheetname: str = '20A - OpLog Summary'):
    """
    Return the value of a cell for the row identified by ``ebid``.

    Parameters
    ----------
    name_col:
        Column header string.  When given, overrides ``column``.
    column:
        1-based column index.
    """
    if name_col is None and column is None:
        raise ValueError("Either name_col or column must be provided.")

    full_sheet = await read_tracksheet()
    worksheet = await full_sheet.worksheet(sheetname)

    if name_col is not None:
        try:
            col_cell = await worksheet.find(name_col)
            column = col_cell.col
        except gspread.exceptions.CellNotFound:
            log.exception(f"Column '{name_col}' not found; falling back to column={column}.")

    ebid_cell = await worksheet.find(str(ebid))
    cell = await worksheet.cell(ebid_cell.row, column)
    return cell.value


async def download_refant_summsheet(ebid,
                                    output_folder,
                                    sheetname: str = '20A - OpLog Summary') -> Path | None:
    """
    Download the refant-ignore string from the master status sheet and write it
    to a file.  Returns the output path, or None if no entry exists.
    """
    refant_ignore = await return_cell(ebid, name_col="Avoid as refant", sheetname=sheetname)

    if not refant_ignore:
        log.info(f"No refant ignore found in the summary sheet for EBID {ebid}.")
        return None

    trackname = await return_cell(ebid, name_col="Trackname", sheetname=sheetname)
    outfilename = Path(output_folder) / f"{trackname}_refantignore.txt"

    if outfilename.exists():
        outfilename.unlink()
    outfilename.write_text(refant_ignore)

    return outfilename


# ---------------------------------------------------------------------------
# Utility queries (not called from the main pipeline loop)
# ---------------------------------------------------------------------------

async def get_tracknames(source_name: str,
                         sheetnames: list = None,
                         config: str = 'all',
                         completed_status: bool = True) -> list:
    """Return all tracknames that include ``source_name`` as one of their targets."""
    if sheetnames is None:
        sheetnames = ['20A - OpLog Summary', 'Archival Track Summary']

    full_sheet = await read_tracksheet()
    track_names = []

    for sheetname in sheetnames:
        worksheet = await full_sheet.worksheet(sheetname)
        tracks_info = await worksheet.get_all_records()

        for track in tracks_info:
            targets = [t.strip() for t in track['Targets'].split(',')]
            if source_name not in targets:
                continue
            if config != 'all' and track['Configuration'] != config:
                continue
            if completed_status and 'imaging' not in track['Status: pipeline']:
                continue
            track_names.append(track['Trackname'])

    return track_names


async def check_tracks_on_disk(source_name: str,
                               local_path: Path,
                               sheetnames: list = None,
                               config: str = 'all',
                               completed_status: bool = True):
    """
    Cross-reference sheet tracknames with MS files on disk.
    Returns ``(table, unmatched_files)``.
    """
    if sheetnames is None:
        sheetnames = ['20A - OpLog Summary', 'Archival Track Summary']

    track_names = await get_tracknames(source_name, sheetnames=sheetnames,
                                       config=config, completed_status=completed_status)

    pipeline_ondisk = []
    local_ms_files = list(local_path.glob("*ms.split.tar"))

    for this_track in track_names:
        matches = [i for i, f in enumerate(local_ms_files) if this_track in f.name]
        if len(matches) == 1:
            local_ms_files.pop(matches[0])
            pipeline_ondisk.append(True)
        elif len(matches) > 1:
            raise ValueError(f"Multiple pipeline MS matches for {this_track} — check files.")
        else:
            pipeline_ondisk.append(False)

    from astropy.table import Table
    tab = Table()
    tab['tracknames'] = track_names
    tab['pipeline_ondisk'] = pipeline_ondisk

    return tab, local_ms_files
