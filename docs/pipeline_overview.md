# AutoDataIngest Pipeline Overview

## What this system does

AutoDataIngest automates the end-to-end data management workflow for VLA
observations: from detecting a new execution block (EB) in the NRAO archive
through pipeline calibration on the Alliance Canada "fir" cluster, QA product
generation, and preparing calibrated data for imaging.

A Google Sheet ("20A-346 Tracks") is the single source of truth for status.
All three main scripts read from it and write back to it. No state is held in
memory between script invocations — everything is recoverable from the sheet.

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

### `main.py` — Initial ingestion and job submission

**Responsibility**: Detect new tracks in the Google Sheet and drive them
through steps 1–7 above.

**Architecture**: `asyncio` producer/consumer.

- **Producer** (`produce()`): Polls the sheet on a long interval (default
  6 hr). Calls `find_new_tracks()` which returns EBIDs where
  `Status: pipeline` is empty. For each new EBID, checks that the SDM has
  already been staged at the NRAO end via Globus before queuing it. A short
  sleep (default 10 min) is inserted between queuing items to stagger starts.

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
| `CASA_VERSION` | `'6.6'` | CASA version (maps to binary path via `path_to_casa()`) |
| `CLUSTER_PIPELINE_JOBTIME` | `'48:00:00'` | SLURM wall time for pipeline job |
| `CLUSTER_PIPELINE_MEM` | `'32G'` | SLURM memory request |
| `SHEETNAMES` | `['20A - OpLog Summary', 'Archival Track Summary']` | Sheet tabs to monitor |
| `NUM_CONSUMERS` | `1` | Concurrent tracks being processed |

**How to run**:
```bash
# From the AutoDataIngest project root:
mkdir -p logs
python main.py
```

Runs continuously until interrupted. Writes to `logs/main.log`. In
production, run inside a `screen` or `tmux` session.

---

### `main_job_completion.py` — Job completion and product transfer

**Responsibility**: Poll SLURM for pipeline jobs that have finished and drive
the completion sequence (steps 8–12).

**Architecture**: `asyncio` producer/consumer with an outer `while True` loop
that recreates the event loop each iteration.

- **Producer** (`produce()`): Every `longsleeptime` (default 1 hr):
  1. Calls `find_running_tracks()` to get all EBIDs whose sheet status
     contains `"Reduction running"`, along with their SLURM job IDs.
  2. SSHs into `cedar-robot-jobstatus` and runs `sacct` via
     `get_slurm_job_monitor()` to fetch all jobs from the last
     `TIME_RANGE_DAYS` (default 14 days).
  3. Calls `identify_completions()` to cross-reference running tracks with
     SLURM output.
  4. Queues completed tracks for the consumer; immediately updates the sheet
     status to ISSUE for failed/timed-out jobs.

- **Consumer** (`consume()`): For each completed track:
  1. `set_qa_queued_status()` — marks sheet as queued for QA transfer
  2. `set_job_stats()` — records wall time and job outcome in sheet
  3. `transfer_calibrated_data()` — Globus to project space (if enabled)
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

**How to run**:
```bash
python main_job_completion.py
```

Runs one full cycle (produce + consume until queue drained) then exits. The
outer `while True` loop with `time.sleep(3600)` at the bottom is commented
out by default — uncomment `# time.sleep(3600)` and comment out `break` to
run continuously. Writes to `logs/main_job_completion.log`.

> **Note (`revamp` branch)**: `main_job_completion.py` still references the
> old per-type job names (`continuum_pipeline_default`, `line_pipeline_default`)
> in `return_job_type()` and calls `set_job_status(data_type, ...)` with the
> old two-argument signature. These calls need to be updated to the new
> `unified_pipeline` job type and single-argument `set_job_status(job_status)`.

---

### `main_restarts.py` — QA-driven restarts and completions

**Responsibility**: React to reviewer decisions entered in the Google Sheet
after QA — restarts, completions, manual reviews, and help requests (steps
13–17).

**Architecture**: `asyncio` producer/consumer. Runs one event loop then exits
(no persistent outer loop by default).

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
| `CLUSTER_PIPELINE_JOBTIME` | — | Wall time for restarted pipeline job |
| `CLUSTER_PIPELINE_MEM` | — | Memory for restarted pipeline job |
| `MAX_NUMJOBS` | `35` | Maximum concurrent SLURM jobs before blocking new restarts |
| `MIN_STORAGE` | `3 TB` | Minimum free scratch space |
| `MIN_NUMFILES` | `100,000` | Minimum free Lustre file count |
| `NUM_CONSUMERS` | `3` | Concurrent tracks being restarted |
| `SHEETNAMES` | both sheets | Sheet tabs to monitor |
| `COMPLETEDDATAPATH` | `/project/.../calibrated/` | Export path for completed tracks |

**How to run**:
```bash
python main_restarts.py
```

Runs one cycle and exits. To run continuously, wrap in a shell loop or a
`screen`/`tmux` session and re-invoke periodically:
```bash
while true; do python main_restarts.py; sleep 3600; done
```

Writes to `logs/main_restarts.log`.

> **Note (`revamp` branch)**: `main_restarts.py` still contains references to
> `_allow_continuum_run` / `_allow_speclines_run` and the old two-type
> `run_types` list structure. These will be simplified once `main_restarts.py`
> is updated: the `run_types` list will always be a single `['pipeline', type]`
> entry, so the "disable the other half" logic is no longer needed.

---

## Running all three scripts together

Each script handles a distinct phase. In normal operation all three run
simultaneously as separate long-running processes:

```bash
mkdir -p logs

# Terminal 1 (or screen/tmux pane 1)
python main.py

# Terminal 2 (or screen/tmux pane 2)
# Run periodically — re-invoke every hour
while true; do python main_job_completion.py; sleep 3600; done

# Terminal 3 (or screen/tmux pane 3)
# Run periodically — re-invoke after QA review cycles
while true; do python main_restarts.py; sleep 3600; done
```

There is no dependency between the processes; each polls the Google Sheet
independently. The sheet acts as the coordination mechanism: `main.py` sets
`"Reduction running"` when it submits a job, `main_job_completion.py` watches
for that status to detect when to poll SLURM, and `main_restarts.py` watches
for reviewer-entered values (`RESTART`, `COMPLETE`, etc.) to trigger the
post-QA actions.

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
