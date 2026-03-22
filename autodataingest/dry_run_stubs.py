'''
Dry-run stubs for testing the AutoDataIngest pipeline without real infrastructure.

Every stub logs what the real call would do and sleeps briefly (DRY_RUN_SLEEP
seconds) so the asyncio event loop is exercised exactly as in production.
Return-value shapes match the real implementations so downstream code that
inspects results does not break.

Usage in main scripts
---------------------
Set ``DRY_RUN = True`` in the ``__main__`` block, then rebind the names used
by produce() / consume() before calling asyncio.run():

    if DRY_RUN:
        from autodataingest.dry_run_stubs import (
            DryRunPipeline as AutoPipeline,
            fake_find_new_tracks as find_new_tracks,
            fake_globus_ebid_check_exists as globus_ebid_check_exists,
        )

Because the __main__ block executes at module scope, produce() and consume()
will pick up the rebound names automatically.

Controlling the simulated QA status
------------------------------------
Set ``pipe._dry_run_qa_status`` on the DryRunPipeline instance to test
different branches in the consumer:

    pipe._dry_run_qa_status = "COMPLETE"   # → export_track_for_imaging
    pipe._dry_run_qa_status = "RESTART"    # → rerun_job_submission  (default)
    pipe._dry_run_qa_status = "MANUAL REVIEW"
'''

import asyncio
import pandas as pd

from .ingest_pipeline_functions import AutoPipeline
from .logging import setup_logging

log = setup_logging()

# ---------------------------------------------------------------------------
# Fake data constants — realistic shapes so downstream checks pass
# ---------------------------------------------------------------------------

DRY_RUN_SLEEP: float = 1.0   # seconds; lower for fast tests, higher to stress scheduling

FAKE_EBID        = 99999999
FAKE_TRACK_NAME  = "20A-346.sb12345678.eb99999999.60123.456789012"
FAKE_CONFIG      = "B"
FAKE_TARGET      = "NGC6822"
FAKE_JOB_ID      = 88888888
FAKE_TASK_ID     = "dry-run-globus-task-0000"
FAKE_CLUSTERNAME = "dra-fir"
FAKE_FLAGSHEET_URL = "https://docs.google.com/spreadsheets/d/dry-run-fake/edit"

# Default status returned by _qa_review_input(); override per-instance via
# pipe._dry_run_qa_status = "COMPLETE" etc.
DEFAULT_QA_STATUS = "RESTART"

# ---------------------------------------------------------------------------
# Gsheet stubs — drop-in replacements for functions called in produce()
# ---------------------------------------------------------------------------

async def fake_find_new_tracks(sheetname='20A - OpLog Summary', **kwargs):
    log.info(f"[DRY RUN] find_new_tracks(sheetname={sheetname!r}) → [{FAKE_EBID}]")
    await asyncio.sleep(DRY_RUN_SLEEP)
    return [FAKE_EBID]


async def fake_find_rerun_status_tracks(sheetname='20A - OpLog Summary',
                                        job_type=None, **kwargs):
    status = job_type if job_type and job_type != 'ALL' else DEFAULT_QA_STATUS
    log.info(f"[DRY RUN] find_rerun_status_tracks(job_type={job_type!r}) "
             f"→ [{FAKE_EBID}, pipeline/{status}]")
    await asyncio.sleep(DRY_RUN_SLEEP)
    return [[FAKE_EBID, [['pipeline', status]]]]


async def fake_find_running_tracks(sheetname='20A - OpLog Summary', **kwargs):
    log.info(f"[DRY RUN] find_running_tracks(sheetname={sheetname!r}) → [{FAKE_EBID}]")
    await asyncio.sleep(DRY_RUN_SLEEP)
    return [[FAKE_EBID, 'pipeline', f"{FAKE_CLUSTERNAME}:{FAKE_JOB_ID}"]]


# ---------------------------------------------------------------------------
# Globus stubs
# ---------------------------------------------------------------------------

def fake_globus_ebid_check_exists(ebid, **kwargs):
    log.info(f"[DRY RUN] globus_ebid_check_exists(ebid={ebid}) → {FAKE_TRACK_NAME}")
    return FAKE_TRACK_NAME


# ---------------------------------------------------------------------------
# Cluster / SSH stubs — used in produce() of main_restarts / main_job_completion
# ---------------------------------------------------------------------------

def fake_setup_ssh_connection(cluster_key, **kwargs):
    log.info(f"[DRY RUN] setup_ssh_connection({cluster_key!r}) → None")
    return None


def fake_get_slurm_job_monitor(connect, **kwargs):
    '''
    Return a minimal DataFrame with one COMPLETED job so that
    identify_completions() finds a match for FAKE_EBID / FAKE_JOB_ID.
    Column set matches what the real get_slurm_job_monitor() produces.
    '''
    log.info("[DRY RUN] get_slurm_job_monitor()")
    row = {
        'JobID':     FAKE_JOB_ID,
        'JobName':   f"{FAKE_TRACK_NAME}.vla_pipeline.pipeline_default-%J",
        'State':     'COMPLETED',
        'TrackName': FAKE_TRACK_NAME,
        'EBID':      FAKE_EBID,
        'JobType':   'pipeline_default',
    }
    return pd.DataFrame([row])


def fake_number_of_active_jobs(df):
    log.info("[DRY RUN] number_of_active_jobs() → 0")
    return 0


def fake_get_lustre_storage_avail(connect, diskname='/scratch'):
    from astropy import units as u
    log.info("[DRY RUN] get_lustre_storage_avail() → 10 TB, 1e6 files")
    return 10 * u.TB, 1e6


# ---------------------------------------------------------------------------
# DryRunPipeline — walking skeleton of AutoPipeline
# ---------------------------------------------------------------------------

class DryRunPipeline(AutoPipeline):
    '''
    Walking-skeleton version of AutoPipeline.

    Every method that touches external infrastructure (Google Sheets, Globus,
    SSH, SLURM) is overridden to log a description of what it would do and
    sleep for DRY_RUN_SLEEP seconds.  The asyncio producer/consumer structure,
    queue ordering, and error-handling paths are all exercised identically to
    production.
    '''

    async def _grab_sheetdata(self):
        log.info(f"[DRY RUN] {self.ebid}: _grab_sheetdata()")
        await asyncio.sleep(DRY_RUN_SLEEP)
        self.targets    = [FAKE_TARGET]
        self.target     = FAKE_TARGET
        self.config     = FAKE_CONFIG
        self.track_name = FAKE_TRACK_NAME
        self.incl_rrl   = False

    async def _qa_review_input(self):
        status = getattr(self, '_dry_run_qa_status', DEFAULT_QA_STATUS)
        log.info(f"[DRY RUN] {self.ebid}: _qa_review_input() → {status!r}")
        await asyncio.sleep(DRY_RUN_SLEEP)
        return status

    async def initial_status(self):
        await self._grab_sheetdata()
        log.info(f"[DRY RUN] {self.ebid}: initial_status() — would write 'Queued' to sheet")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def set_qa_queued_status(self, data_type='continuum'):
        log.info(f"[DRY RUN] {self.ebid}: set_qa_queued_status({data_type!r}) "
                 "— would write 'Queued for QA/product transfer' to sheet")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def archive_request_and_transfer(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: archive_request_and_transfer() "
                 "— skipping archive request and Globus transfer")
        await asyncio.sleep(DRY_RUN_SLEEP)
        self.track_name      = FAKE_TRACK_NAME
        self.config          = FAKE_CONFIG
        self.target          = FAKE_TARGET
        self.transfer_taskid = FAKE_TASK_ID

    async def setup_for_reduction_pipeline(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: setup_for_reduction_pipeline() "
                 "— skipping SSH / antenna-corrections upload")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def initial_job_submission(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: initial_job_submission() "
                 "— skipping SLURM job submission")
        await asyncio.sleep(DRY_RUN_SLEEP)
        self.pipeline_jobid = str(FAKE_JOB_ID)

    async def set_job_status(self, job_status):
        log.info(f"[DRY RUN] {self.ebid}: set_job_status({job_status!r}) "
                 "— would update sheet status")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def set_job_stats(self, job_id, job_type):
        log.info(f"[DRY RUN] {self.ebid}: set_job_stats(job_id={job_id}, "
                 f"job_type={job_type!r}) — would update sheet stats")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def transfer_pipeline_products(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: transfer_pipeline_products() "
                 "— skipping Globus transfer of pipeline products")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def make_flagging_sheet(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: make_flagging_sheet() "
                 "— skipping flagging sheet creation")
        await asyncio.sleep(DRY_RUN_SLEEP)
        self._flagsheet_url = FAKE_FLAGSHEET_URL

    async def make_qa_products(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: make_qa_products() "
                 "— skipping qaplotter / file operations")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def get_flagging_files(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: get_flagging_files() "
                 "— skipping flagging sheet download and SSH copy")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def get_refantignore_files(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: get_refantignore_files() "
                 "— skipping refant-ignore download and SSH copy")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def rerun_job_submission(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: rerun_job_submission() "
                 "— skipping cleanup, flagging update, and SLURM re-submit")
        await asyncio.sleep(DRY_RUN_SLEEP)
        self.pipeline_jobid = str(FAKE_JOB_ID)

    async def cleanup_on_cluster(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: cleanup_on_cluster() "
                 "— skipping SSH cleanup on cluster")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def transfer_calibrated_data(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: transfer_calibrated_data() "
                 "— skipping Globus transfer of calibrated MS")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def export_track_for_imaging(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: export_track_for_imaging() "
                 "— skipping MS export and cluster cleanup")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def label_qa_failures(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: label_qa_failures() "
                 "— would write failure label to sheet")
        await asyncio.sleep(DRY_RUN_SLEEP)

    async def transfer_qa_failures(self, **kwargs):
        log.info(f"[DRY RUN] {self.ebid}: transfer_qa_failures() "
                 "— skipping Globus transfer of failure products")
        await asyncio.sleep(DRY_RUN_SLEEP)
