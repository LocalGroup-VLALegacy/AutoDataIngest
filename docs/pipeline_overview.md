# AutoDataIngest Pipeline Overview

## What this system does

AutoDataIngest automates the end-to-end data management workflow for VLA
observations: from detecting a new execution block (EB) in the NRAO archive
through pipeline calibration on the Alliance Canada "fir" cluster, QA product
generation, and preparing calibrated data for imaging.

A Google Sheet ("20A-346 Tracks") is the single source of truth for status.
All three main scripts read from it and write back to it using the async
`gspread-asyncio` library. No state is held in memory between script
invocations — everything is recoverable from the sheet.

---

## System components

```
Local machine (this repo)
  ├─ main.py                 # Watches for new tracks, stages data, submits jobs
  ├─ main_job_completion.py  # Detects pipeline completion, transfers products
  └─ main_restarts.py        # Handles QA-driven restarts, completions, failures

Google Sheets ("20A-346 Tracks")
  └─ One row per execution block (EBID)
     Columns include: EBID, Trackname, Targets, Configuration,
                      Status: pipeline, Pipeline job ID, Re-run pipeline, ...

Flagging Sheet ("SB_Issue_Tracking")
  └─ One tab per track: {config}_{abbrev_trackname}
     Contains manual flagging commands and imaging trigger cells

fir cluster (Alliance Canada)
  ├─ ~/scratch/VLAXL_reduction/{config}_{trackname}/   # Working directory
  ├─ ~/projects/.../VLAXL_products/                    # Long-term product storage
  └─ SLURM: one unified pipeline job per track

ingester server
  └─ /mnt/space/vlaxl/track_products/   # QA products served by the webserver
```

### SSH robot accounts

The scripts never run SLURM commands directly. Instead they SSH into several
restricted robot accounts on the cluster, each locked to a single shell
command:

| SSH alias | Script on cluster | Purpose |
|---|---|---|
| `cedar-robot-generic` | (unrestricted) | File copies, directory creation |
| `cedar-submitter` | `submit_robot.sh` | `sbatch` job submission |
| `cedar-robot-jobstatus` | `status_robot.sh` | `sacct` job status queries |
| `cedar-robot-lfsquota` | `lfs_robot.sh` | Lustre storage quota check |
| `cedar-robot-jobsetup` | (pipeline clone) | Clone/update ReductionPipeline repo |

SSH configuration for these aliases lives in `~/.ssh/config` on the local
machine. Keys are set up so that no password is required.

---

## Full data flow (one track)

```
[New EBID appears in Google Sheet with blank Status: pipeline]
        │
        ▼ main.py
1.  Globus transfer: SDM from NRAO/fir staging → fir scratch
2.  SSH: copy VLA antenna correction tables to track folder
3.  Download manual flagging file from flagging sheet (if exists)
4.  Download refant ignore file from summary sheet (if exists)
5.  SSH: transfer flag files to cluster
6.  Create flagging sheet tab in SB_Issue_Tracking (if not exists)
7.  sbatch: submit unified pipeline job
    → Sheet status set to "Reduction running on dra-fir"
        │
        ▼  [SLURM runs lband_reduction_pipeline.py on fir]
        │   Produces: products/ folder + {track_folder}_pipeline_products.tar
        │
        ▼ main_job_completion.py
8.  sacct: detect job COMPLETED or FAILED
    On COMPLETED:
9.  Globus: transfer calibrated MS to project space (optional)
10. Globus: transfer _pipeline_products.tar to ingester
11. Make QA plots (qaplotter) from products tar
12. Move QA folder to webserver path
    → Sheet status set to "Ready for QA"
        │
        ▼  [Reviewer inspects QA webserver, fills in Re-run pipeline column]
        │
        ▼ main_restarts.py
    If "RESTART":
13. SSH: clean pipeline outputs from cluster
14. Download updated flagging files from sheet
15. sbatch: resubmit pipeline job
    → Sheet status set to "Restarting pipeline for re-run"

    If "COMPLETE":
16. Export calibrated data to long-term project storage for imaging

    If "MANUAL REVIEW" / "HELP REQUESTED":
17. Label track in sheet as requiring manual attention
```

---

## The three main scripts

All three scripts share the same architecture: a single `asyncio.run()` call
drives one or more **producer** coroutines and one or more **consumer**
coroutines over a shared `asyncio.Queue`. The producer loops indefinitely
(sleeping between cycles with `await asyncio.sleep()`), so the process runs
continuously until interrupted — no external shell loop or periodic
re-invocation is needed.

### `main.py` — Initial ingestion and job submission

**Responsibility**: Detect new tracks in the Google Sheet and drive them
through steps 1–7 above.

- **Producer** (`produce()`): Polls the sheet on a long interval (default
  6 hr). Calls `find_new_tracks()` which returns EBIDs where
  `Status: pipeline` is empty. For each new EBID, checks that the SDM has
  already been staged at the NRAO end via Globus before queuing it. A short
  sleep (default 10 min) is inserted between queuing items to stagger starts.
  Loops via `await asyncio.sleep(long_sleep)` at the end of each cycle.

- **Consumer** (`consume()`): Processes one track end-to-end:
  1. `archive_request_and_transfer()` — Globus transfer to fir
  2. `setup_for_reduction_pipeline()` — SSH copy of antenna correction tables
  3. `make_flagging_sheet()` — creates flagging tab in Google Sheet
  4. `get_flagging_files()` — downloads and uploads manual flags
  5. `get_refantignore_files()` — downloads and uploads refant ignore
  6. `initial_job_submission()` — generates and submits SLURM script

**Configuration** (edit in the `if __name__ == "__main__"` block):

| Variable | Default | Description |
|---|---|---|
| `CLUSTERNAME` | `'dra-fir'` | Target cluster |
| `CASA_VERSION` | `'6.6'` | CASA version passed to the job script |
| `CLUSTER_PIPELINE_JOBTIME` | `'48:00:00'` | SLURM wall time for pipeline job |
| `CLUSTER_PIPELINE_MEM` | `'32G'` | SLURM memory request |
| `SHEETNAMES` | `['20A - OpLog Summary', 'Archival Track Summary']` | Sheet tabs to monitor |
| `NUM_CONSUMERS` | `1` | Concurrent tracks being processed |
| `DRY_RUN` | `False` | Set `True` to test without real infrastructure |

**How to run**:
```bash
mkdir -p logs
python main.py
```

Runs continuously until interrupted. Writes to `logs/main.log`. In
production, run inside a `screen` or `tmux` session.

---

### `main_job_completion.py` — Job completion and product transfer

**Responsibility**: Poll SLURM for pipeline jobs that have finished and drive
the completion sequence (steps 8–12).

- **Producer** (`produce()`): Every `longsleeptime` (default 1 hr):
  1. Calls `find_running_tracks()` to get all EBIDs whose sheet status
     contains `"Reduction running"`, along with their SLURM job IDs.
  2. SSHs into `cedar-robot-jobstatus` and runs `sacct` via
     `get_slurm_job_monitor()` to fetch all jobs from the last
     `TIME_RANGE_DAYS` (default 14 days).
  3. Calls `identify_completions()` to cross-reference running tracks with
     SLURM output.
  4. Queues completed tracks for the consumer; immediately updates the sheet
     status for failed/timed-out jobs.
  Loops via `await asyncio.sleep(longsleeptime)` at the end of each cycle.

- **Consumer** (`consume()`): For each completed track:
  1. `set_qa_queued_status()` — marks sheet as queued for QA transfer
  2. `set_job_stats()` — records wall time and job outcome in sheet
  3. `transfer_calibrated_data()` — Globus to project space (if `DO_DATA_TRANSFER`)
  4. `transfer_pipeline_products()` — Globus products tar to ingester
  5. `make_flagging_sheet()` — creates/confirms flagging tab exists
  6. `make_qa_products()` — extracts weblog, runs qaplotter, moves to webserver
  7. `set_job_status("COMPLETED")` — sets sheet to "Ready for QA"

**Configuration** (edit in `if __name__ == "__main__"`):

| Variable | Default | Description |
|---|---|---|
| `CLUSTERNAME` | `'dra-fir'` | Cluster to query |
| `TIME_RANGE_DAYS` | `14` | How far back `sacct` looks |
| `DO_DATA_TRANSFER` | `True` | Whether to transfer calibrated MS to project space |
| `SHEETNAMES` | both sheets | Sheet tabs to monitor |
| `DRY_RUN` | `False` | Set `True` to test without real infrastructure |

**How to run**:
```bash
python main_job_completion.py
```

Runs continuously until interrupted. Writes to `logs/main_job_completion.log`.

---

### `main_restarts.py` — QA-driven restarts and completions

**Responsibility**: React to reviewer decisions entered in the Google Sheet
after QA — restarts, completions, manual reviews, and help requests (steps
13–17).

- **Producer** (`produce()`): On each cycle:
  1. First pass — COMPLETE / MANUAL REVIEW / HELP REQUESTED: calls
     `find_rerun_status_tracks(job_type="COMPLETE")` and the failure states.
     These are queued immediately (no storage check needed since they don't
     start new jobs).
  2. Second pass — RESTART: calls `find_rerun_status_tracks(job_type="RESTART")`.
     Before queuing, SSHs to `cedar-robot-jobstatus` and
     `cedar-robot-lfsquota` to check:
     - Number of active SLURM jobs < `MAX_NUMJOBS` (default 35)
     - Free scratch space > `MIN_STORAGE` (default 3 TB)
     - Free file count > `MIN_NUMFILES` (default 100,000)
     New RESTART jobs are gated on these limits.
  Loops via `await asyncio.sleep(long_sleep)` at the end of each cycle.

- **Consumer** (`consume()`): Reads `Re-run pipeline` cell to determine
  the action:

  | Cell value | Action |
  |---|---|
  | `"COMPLETE"` | `export_track_for_imaging()` — move calibrated MS to imaging project space |
  | `"RESTART"` | `rerun_job_submission()` — clean cluster, refresh flags, resubmit |
  | `"MANUAL REVIEW"` | `label_qa_failures()` — mark track for human review |
  | `"HELP REQUESTED"` | `label_qa_failures()` — mark track for human review |

**Configuration** (edit in `if __name__ == "__main__"`):

| Variable | Default | Description |
|---|---|---|
| `CLUSTERNAME` | `'dra-fir'` | Cluster |
| `CASA_VERSION` | `'6.5'` | CASA version for resubmitted jobs |
| `PIPELINE_BRANCHNAME` | `'main'` | ReductionPipeline git branch |
| `MAX_NUMJOBS` | `35` | Maximum concurrent SLURM jobs before blocking new restarts |
| `MIN_STORAGE` | `3 TB` | Minimum free scratch space |
| `MIN_NUMFILES` | `100,000` | Minimum free Lustre file count |
| `NUM_CONSUMERS` | `3` | Concurrent tracks being restarted |
| `SHEETNAMES` | both sheets | Sheet tabs to monitor |
| `COMPLETEDDATAPATH` | `/project/.../calibrated/` | Export path for completed tracks |
| `DRY_RUN` | `False` | Set `True` to test without real infrastructure |

**How to run**:
```bash
python main_restarts.py
```

Runs continuously until interrupted. Writes to `logs/main_restarts.log`.

---

## Running all three scripts together

Each script handles a distinct phase. In normal operation all three run
simultaneously as separate long-running processes:

```bash
mkdir -p logs

# Terminal 1 (or screen/tmux pane 1)
python main.py

# Terminal 2 (or screen/tmux pane 2)
python main_job_completion.py

# Terminal 3 (or screen/tmux pane 3)
python main_restarts.py
```

Each script loops indefinitely inside a single `asyncio.run()` call — the
producer coroutine sleeps between cycles with `await asyncio.sleep()`. No
external shell loop or periodic re-invocation is needed.

There is no dependency between the processes; each polls the Google Sheet
independently. The sheet acts as the coordination mechanism: `main.py` sets
`"Reduction running"` when it submits a job, `main_job_completion.py` watches
for that status to detect when to poll SLURM, and `main_restarts.py` watches
for reviewer-entered values (`RESTART`, `COMPLETE`, etc.) to trigger the
post-QA actions.

---

## Dry-run mode

All three scripts support a `DRY_RUN` flag (set in the `__main__` block) that
replaces every call to real infrastructure with a logged stub and a short
sleep. This exercises the full asyncio producer/consumer flow — queue
creation, task scheduling, sleep timing, error-handling paths — without
touching Google Sheets, Globus, or SSH.

To enable:

```python
# In main.py / main_job_completion.py / main_restarts.py, near the top of
# the if __name__ == "__main__" block:
DRY_RUN = True
```

When `DRY_RUN = True` the script logs `[DRY RUN]` before every simulated
operation and uses the fake data constants defined in
`autodataingest/dry_run_stubs.py`:

| Constant | Value |
|---|---|
| `FAKE_EBID` | `99999999` |
| `FAKE_TRACK_NAME` | `"20A-346.sb12345678.eb99999999.60123.456789012"` |
| `FAKE_CONFIG` | `"B"` |
| `FAKE_TARGET` | `"NGC6822"` |
| `FAKE_JOB_ID` | `88888888` |
| `DRY_RUN_SLEEP` | `1.0` s (controls simulated I/O latency) |

To test a specific QA branch through `main_restarts.py`, set
`_dry_run_qa_status` on the pipeline object created in `produce()`:

```python
this_pipe._dry_run_qa_status = "COMPLETE"       # exercises export_track_for_imaging
this_pipe._dry_run_qa_status = "RESTART"        # exercises rerun_job_submission (default)
this_pipe._dry_run_qa_status = "MANUAL REVIEW"  # exercises label_qa_failures
```

---

## Dashboard

An optional TUI dashboard runs on the ingester server and lets any authorised
user view the live status of all three scripts, tail their log output, and
send start/stop/restart commands — without SSH access to the machine running
them.

```bash
# Start the daemon (runs the scripts as subprocesses, serves a Unix socket)
python run_dashboard_server.py

# Connect from any terminal on the same machine
python -m dashboard.client
```

Key bindings in the TUI: `1`/`2`/`3` select a script panel; `F1` start,
`F2` stop, `F3` restart; `Q` quit.

The server is designed to run under systemd; see `deploy/vlaxl-dashboard.service`.

---

## Google Sheet column reference

The key columns on each summary tab (e.g., "20A - OpLog Summary"):

| Column | Written by | Read by | Purpose |
|---|---|---|---|
| `EBID` | human | all | Execution block ID — primary key |
| `Trackname` | `main.py` | all | VLA SDM name |
| `Targets` | human | `main.py`, `main_restarts.py` | Comma-separated science target(s) |
| `Configuration` | human | all | VLA array config (A, B, C, D, or combined) |
| `Status: pipeline` | all scripts | all | Current processing status (drives queue logic) |
| `Pipeline job ID` | `main.py` | `main_job_completion.py` | `{cluster}:{SLURM_jobid}` |
| `Re-run pipeline` | human (reviewer) | `main_restarts.py` | Trigger value: `RESTART`, `COMPLETE`, `MANUAL REVIEW`, `HELP REQUESTED` |
| `Prev pipeline status` | `main_restarts.py` | — | Timestamped record of last re-run trigger |
| `Pipeline reduction` | `main_job_completion.py` | — | SLURM job final state |
| `Pipeline job wall time` | `main_job_completion.py` | — | Actual wall time used |
| `Staged data from archive` | `main.py` | — | Boolean flag |
| `Transferred data` | `main.py` | — | Boolean flag |
| `Avoid as refant` | human | `main.py` | Antenna name(s) to avoid as reference antenna |
| `INCL_RRLS` | human | `main.py` | Whether to include radio recombination lines |

---

## Log files

All three scripts write timestamped logs to the `logs/` subdirectory (created
by the operator before first run):

| Script | Log file |
|---|---|
| `main.py` | `logs/main.log` |
| `main_job_completion.py` | `logs/main_job_completion.log` |
| `main_restarts.py` | `logs/main_restarts.log` |
| Dashboard server | `logs/dashboard_server.log` |
