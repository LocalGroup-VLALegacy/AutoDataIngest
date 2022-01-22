
from datetime import datetime, timedelta
import pandas as pd
from astropy import units as u

from .ssh_utils import run_command

from .logging import setup_logging
log = setup_logging()


def get_slurm_job_monitor(connect, time_range_days=7, timeout=600,
                          raise_jobname_error=False):
    '''
    Return job statuses on clusters running slurm.
    '''

    time_now = datetime.now()
    time_week = timedelta(days=time_range_days)

    start_time = time_now - time_week
    start_time_str = start_time.strftime("%Y-%m-%d")

    slurm_cmd = f'sacct --format="JobID,JobName%110,State%20" --starttime={start_time_str} | grep -v "^[0-9]*\."'

    result = run_command(connect, slurm_cmd, test_connection=False,
                         timeout=timeout)

    # Parse the output into a table.
    lines = result.stdout.split('\n')

    colnames = list(filter(None, lines[0].split(" ")))

    stripped_lines = []

    # Skip line[1]. It's the delimiter in the slurm output
    for ii, line in enumerate(lines[2:]):
        this_line = list(filter(None, line.split(" ")))

        if len(this_line) == 0:
            continue

        # Some cancelled states will list: CANCELLED by NUM
        # when it was cancelled due to a dependent job.
        # Correct for those cases here.
        if len(this_line) > 3:
            if "CANCELLED" in this_line[2]:
                this_line = this_line[:3]
            else:
                raise ValueError(f"Unsure of input line format: {this_line}")

        # Job num is int
        this_line[0] = int(this_line[0])

        # Strip out track info from the job name
        this_name = this_line[1]
        name_info = this_name.split('-%J')[0].split(".vla_pipeline.")
        if len(name_info) != 2:
            if raise_jobname_error:
                raise ValueError(f"Check job name: {name_info}")
            else:
                log.warn(f"Unable to parse job with name: {name_info}")
                continue

        track_name, job_type = name_info

        ebid = int(track_name.split(".")[2].split('eb')[1])

        this_line.extend([track_name, ebid, job_type])

        stripped_lines.append(this_line)

    colnames.extend(["TrackName", 'EBID', 'JobType'])

    df = pd.DataFrame(stripped_lines, columns=colnames)

    return df



def identify_completions(df, running_tracks):
    '''
    Search for completed/failed jobs that are listed as currently running.
    '''

    fail_states = ["FAILED", "OUT_OF_MEMORY", "CANCELLED", "NODE_FAIL"]

    comps = []
    fails = []

    for this_track in running_tracks:
        this_ebid, data_type, job_summ = this_track

        # CLUSTERNAME:JOBNUM
        job_id = int(job_summ.split(":")[1])

        this_row = df[df['JobID'] == job_id]

        if len(this_row) == 0:
            log.error(f"Unable to find job ID {job_id} for EBID {this_ebid} {data_type}")
            continue

        index = this_row.index[0]

        if this_row['State'].to_string(index=False) == "COMPLETED":
            comps.append(index)
        elif this_row['State'].to_string(index=False) in fail_states:
            fails.append(index)
        else:
            # Pending or running.
            pass

    df_comp = df.iloc[comps]
    df_fails = df.iloc[fails]

    return df_comp, df_fails


def number_of_active_jobs(df):

    return (df['State'] == "RUNNING").sum() + (df['State'] == "PENDING").sum()


# def identify_completions(df_old, df_new):

#     diff = df_old.merge(df_new,
#                         indicator=True,
#                         how='right').loc[lambda x : x['_merge'] != 'both']

#     diff_comp = diff[diff['State'] == "COMPLETED"]
#     diff_fails = diff[(diff['State'] != "COMPLETED") & (diff['State'] != "RUNNING")]

#     # We also don't need to keep import/split completions, so filter those
#     # ones out:
#     diff_comp = diff_comp[diff_comp['JobType'] != 'import_and_split']

#     return diff_comp, diff_fails


def get_lustre_storage_avail(connect, diskname='/scratch', timeout=600, username='ekoch'):
    '''
    Runs lfs quota to find current usage.
    '''

    cmd = f'lfs quota -u {username} {diskname}'

    result = run_command(connect, cmd, test_connection=False,
                         timeout=timeout)

    # Parse the output into a table.
    lines = result.stdout.split('\n')

    # Expect format of:
    # 'Disk quotas for usr...'
    # Column headers
    # Values

    colnames = list(filter(None, lines[1].split(" ")))

    usage_vals = list(filter(None, lines[2].split(" ")))

    storage_used =  int(usage_vals[1]) * u.kB
    storage_total = int(usage_vals[2]) * u.kB

    storage_avail = (storage_total - storage_used).to(u.TB)

    files_used =  int(usage_vals[5])
    files_total = int(usage_vals[6])

    files_avail = files_total - files_used

    return storage_avail, files_avail
