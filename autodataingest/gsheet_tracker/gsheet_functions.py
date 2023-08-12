
'''
Functions for reading/writing to the master google sheet tracking the reduction status of VLA tracks.

Note that we have a service account attached to the gdrive ("datamanager@vlaxlgdrive.iam.gserviceaccount.com")
that the authentication is handled through

'''

import time
import requests
import gspread
from gspread_formatting import cellFormat, color, textFormat, format_cell_range
import string
from datetime import datetime
from pathlib import Path

from ..logging import setup_logging
log = setup_logging()


def do_authentication_gspread():
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

    gc = do_authentication_gspread()

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
        if track['Status: continuum'] != status_check:
            continue

        if track['Status: speclines'] != status_check:
            continue

        new_tracks.append(track['EBID'])

    return new_tracks


def find_rerun_status_tracks(sheetname='20A - OpLog Summary',
                             job_type=None,
                             max_per_exec=10):
    """
    Find new tracks where an updated run status has been indicated. Fill in last given
    status with a timestamp in the sheet.
    """

    full_sheet = read_tracksheet()

    # Find the right sheet according to sheetname

    worksheet = full_sheet.worksheet(sheetname)

    # Grab the track info.
    tracks_info = worksheet.get_all_records()

    new_tracks = []

    if job_type is None:
        job_type = 'ALL'

    for track in tracks_info:
        # Check if the status is equal to `status_check`
        run_types = []

        time.sleep(5)

        if len(track['Re-run\ncontinuum']) > 0 or len(track['Re-run\nspeclines']) > 0:

            time_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            append_this_track = False

            if len(track['Re-run\ncontinuum']) > 0:
                this_type_cont = track['Re-run\ncontinuum']
                if job_type is not 'ALL' and job_type not in this_type_cont:
                    log.info(f"Skipping continuum job since it is a {this_type_cont} not a  {job_type} job.")
                else:

                    append_this_track = True

                    continuum_trigger = f"{this_type_cont} at {time_stamp}"

                    update_cell(track['EBID'],
                                continuum_trigger,
                                name_col='Prev continuum status',
                                sheetname=sheetname)

                    run_types.append(['continuum', this_type_cont])

            if len(track['Re-run\nspeclines']) > 0:
                this_type_line = track['Re-run\nspeclines']

                if job_type is not 'ALL' and job_type not in this_type_line:
                    log.info(f"Skipping line job since it isn't a {this_type_line} not a {job_type} job.")
                else:

                    append_this_track = True

                    speclines_trigger = f"{this_type_line} at {time_stamp}"

                    update_cell(track['EBID'],
                                speclines_trigger,
                                name_col='Prev speclines status',
                                sheetname=sheetname)

                    run_types.append(['speclines', this_type_line])

            if append_this_track:
                new_tracks.append([track['EBID'], run_types])

        if len(new_tracks) >= max_per_exec:
            log.info(f"Hit maximum jobs per execution to submit: {max_per_exec}")
            break

    return new_tracks


def find_running_tracks(sheetname='20A - OpLog Summary',
                        status_check='Reduction running'):
    """
    Return the EBID and job type of tracks that were last actively running.
    """

    full_sheet = read_tracksheet()

    # Find the right sheet according to sheetname

    worksheet = full_sheet.worksheet(sheetname)

    # Grab the track info.
    tracks_info = worksheet.get_all_records()

    running_tracks = []

    for track in tracks_info:
        # Check if the status is equal to `status_check`
        if status_check in track['Status: continuum']:
            running_tracks.append([track['EBID'], "continuum", track['Continuum job ID']])

        if status_check in track['Status: speclines']:
            running_tracks.append([track['EBID'], "speclines", track['Line job ID']])

    return running_tracks


def return_all_ebids(sheetname='20A - OpLog Summary'):

    # Find the right sheet according to sheetname
    full_sheet = read_tracksheet()

    worksheet = full_sheet.worksheet(sheetname)

    ebids = worksheet.col_values(7)

    # Drop empties and the first (the column name)
    ebids = [ebid for ebid in ebids if len(ebid) > 0][1:]

    return ebids


# Existing colors:
# Light yellow: staged data for transfer
# Light blue: data transferred finished; ready for reduction pipeline run

stage_colors = {'Archive download staged': {'row_color': [0.9254901960784314, 0.8823529411764706, 0.2],
                                            'text_color': [0., 0., 0.],
                                            'bold_text': False},
                'Queued': {'row_color': [0.9254901960784314, 0.8823529411764706, 0.2],
                                            'text_color': [0., 0., 0.],
                                            'bold_text': False},
                'Data transferred to': {'row_color': [0.33725490196078434, 0.7058823529411765, 0.9137254901960784],
                                        'text_color': [0., 0., 0.],
                                        'bold_text': False},
                'Reduction running on': {'row_color': [0.00392156862745098, 0.45098039215686275, 0.6980392156862745],
                                        'text_color': [1., 1., 1.],
                                        'bold_text': False},
                'Ready for QA': {'row_color': [0.8352941176470589, 0.3686274509803922, 0.0],
                                        'text_color': [1., 1., 1.],
                                        'bold_text': False},
                'Ready for imaging': {'row_color': [0.00784313725490196, 0.6196078431372549, 0.45098039215686275],
                                        'text_color': [1., 1., 1.],
                                        'bold_text': True},
                'Restarting pipeline': {'row_color': [0.00392156862745098, 0.45098039215686275, 0.6980392156862745],
                                        'text_color': [1., 1., 1.],
                                        'bold_text': False},
                'ISSUE': {'row_color': [0., 0., 0.],
                          'text_color': [1., 1., 1.],
                          'bold_text': False},
                'FAILED': {'row_color': [0., 0., 0.],
                           'text_color': [1., 1., 1.],
                           'bold_text': False},
                'HELP': {'row_color': [0.8, 0.47058823529411764, 0.7372549019607844],
                         'text_color': [1., 1., 1.],
                         'bold_text': False}}

# Future stage colors:
# Deep blue: initial/revised reduction running
# [0.00392156862745098, 0.45098039215686275, 0.6980392156862745], [1., 1., 1.]
# Deep orange: initidual reduction done; awaiting
# [0.8352941176470589, 0.3686274509803922, 0.0], [1., 1., 1.]
# Green: READY for imaging
# [0.00784313725490196, 0.6196078431372549, 0.45098039215686275], [1., 1., 1.]
# Black: Failed
# [0., 0., 0.], [1., 1., 1.]



def update_track_status(ebid, message="Archive download staged",
                        sheetname='20A - OpLog Summary',
                        status_col=1,
                        bool_status_colname="Staged data \nfrom archive",
                        row_color=[1., 1., 1.],
                        text_color=[0., 0., 0.],
                        bold_text=False,
                        max_retry=5):
    """
    Update the processing status of a track running through the pipeline.
    """

    i = 0
    while i == 0:
        try:
            full_sheet = read_tracksheet()
            worksheet = full_sheet.worksheet(sheetname)
            break
        except requests.ReadTimeout:
            time.sleep(10)
            pass

        i += 1

        if i >= max_retry:
            raise ValueError("Error: timed out multiple time reading google sheet.")

    cell = worksheet.find(str(ebid))

    worksheet.update_cell(cell.row, status_col, message)

    # Update the boolean flags for the different stages.
    bool_cell_col = worksheet.find(bool_status_colname).col
    worksheet.update_cell(cell.row, bool_cell_col, "TRUE")

    # Check if we have a color to update for the row at this stage:
    key_match_status = [key for key in stage_colors if key in message]
    if len(key_match_status) > 1:
        log.info("Found multiple matching statuses: {key_match_status}. Going with the first one")

    if len(key_match_status) > 0:
        key = key_match_status[0]
        row_color = stage_colors[key]['row_color']
        text_color = stage_colors[key]['text_color']
        bold_text = stage_colors[key]['bold_text']

    fmt = cellFormat(backgroundColor=color(*row_color),
                     textFormat=textFormat(bold=bold_text,
                                           foregroundColor=color(*text_color)))

    format_cell_range(worksheet, f'{string.ascii_uppercase[status_col-1]}{cell.row}', fmt)


def update_cell(ebid, value,
                name_col=None,
                num_col=3,
                sheetname='20A - OpLog Summary'):
    '''
    Update cell given an execution block ID and column for the output.

    Parameters
    ----------
    ebid : str
        EB ID number of the track.
    name_col : str, optional
        Name of column in the google sheet. When given, overrides `num_col`.
    num_col : int, optional
        Integer number of the column starting at 1(!).
    sheetname : str, optional
        Name of tab sheet name.

    '''
    if name_col is None and num_col is None:
        raise ValueError("Either name_col or num_col must be provided.")

    full_sheet = read_tracksheet()
    worksheet = full_sheet.worksheet(sheetname)

    if name_col is not None:
        try:
            thiscolcell = worksheet.find(name_col)
            num_col = thiscolcell.col
        except gspread.CellNotFound:
            log.exception(f"Unable to find column name {name_col}. Defaulting to `num_col`")

    cell = worksheet.find(str(ebid))

    worksheet.update_cell(cell.row, num_col, value)


def return_cell(ebid,
                name_col=None,
                column=9,
                sheetname='20A - OpLog Summary'):
    '''
    Return cell given an execution block ID and column for the output.

    Parameters
    ----------
    ebid : str
        EB ID number of the track.
    name_col : str, optional
        Name of column in the google sheet. When given, overrides `column`.
    column : int, optional
        Integer number of the column starting at 1(!).
    sheetname : str, optional
        Name of tab sheet name.
    '''

    if name_col is None and column is None:
        raise ValueError("Either name_col or column must be provided.")

    full_sheet = read_tracksheet()
    worksheet = full_sheet.worksheet(sheetname)

    if name_col is not None:
        try:
            thiscolcell = worksheet.find(name_col)
            column = thiscolcell.col
        except gspread.CellNotFound:
            log.exception(f"Unable to find column name {name_col}. Defaulting to `column`")

    ebid_cell = worksheet.find(str(ebid))

    cell = worksheet.cell(ebid_cell.row, column)

    return cell.value


def download_refant_summsheet(ebid,
                              output_folder,
                              data_type='continuum',
                              sheetname='20A - OpLog Summary'):
    '''
    Does the same thing as gsheet_flagging.download_refant but pulls the refant ignore
    from the master status sheet, not the flagging sheets.


    '''

    refant_ignore_cell = return_cell(ebid,
                                     name_col="Avoid as refant",
                                     sheetname=sheetname)

    if refant_ignore_cell is None:
        log.info(f"No refant ignore found in the flagging sheet for {ebid}")
        return None

    trackname = return_cell(ebid, name_col="Trackname", sheetname=sheetname)

    outfilename = Path(output_folder) / f"{trackname}_{data_type}_refantignore.txt"

    if outfilename.exists():
        outfilename.unlink()

    with open(outfilename, "w") as outfile:

        outfile.write(refant_ignore_cell)

    return outfilename


def get_tracknames(source_name,
                   sheetnames=['20A - OpLog Summary',
                               'Archival Track Summary'],
                   config='all',
                   completed_status=True):
    """
    Check which tracks are contained or not in a local directory.
    """

    full_sheet = read_tracksheet()

    # Find the right sheet according to sheetname

    track_names = []

    for sheetname in sheetnames:

        worksheet = full_sheet.worksheet(sheetname)

        # Grab the track info.
        tracks_info = worksheet.get_all_records()

        for track in tracks_info:

            if source_name not in track['Target']:
                continue

            if config != "all":
                if track['Configuration'] != config:
                    continue

            if completed_status:
                if 'imaging' not in track['Status: continuum']:
                    continue

                if 'imaging' not in track['Status: speclines']:
                    continue

            track_names.append(track['Trackname'])

    return track_names


def check_tracks_on_disk(source_name, local_path,
                         sheetnames=['20A - OpLog Summary',
                                     'Archival Track Summary'],
                         config='all',
                         completed_status=True):

    track_names = get_tracknames(source_name, sheetnames=sheetnames,
                                 config=config,
                                 completed_status=completed_status)

    continuum_ondisk = []
    speclines_ondisk = []

    local_ms_files = local_path.glob(f"{source_name}*ms.split.tar")

    for this_track in track_names:

        has_cont = [ii for ii, this_file in enumerate(local_ms_files) if
                    (this_track in this_file) and ("continuum" in this_file)]
        has_lines = [ii for ii, this_file in enumerate(local_ms_files) if
                    (this_track in this_file) and ("speclines" in this_file)]

        if len(has_cont) == 1:
            local_ms_files.pop(has_cont[0])
            continuum_ondisk.append(True)
        elif len(has_cont) > 1:
            raise ValueError(f"Shouldn't have multiple continuum matches. Check {this_track}")
        else:
            continuum_ondisk.append(False)

        if len(has_lines) == 1:
            local_ms_files.pop(has_lines[0])
            speclines_ondisk.append(True)
        elif len(has_lines) > 1:
            raise ValueError(f"Shouldn't have multiple line matches. Check {this_track}")
        else:
            speclines_ondisk.append(False)

    from astropy.table import Table

    tab = Table()
    tab['tracknames'] = track_names
    tab['continuum_ondisk'] = continuum_ondisk
    tab['speclines_ondisk'] = speclines_ondisk

    return tab, local_ms_files
