
'''
These are top-level functions for the major steps in the ingestion pipeline.

They are meant to be called in "main.py"
'''

import sys
import os
from pathlib import Path
from glob import glob
import asyncio
import socket
import subprocess
import shutil

# fabric handles ssh to cluster running jobs
import fabric
import paramiko

from autodataingest.logging import setup_logging
log = setup_logging()


from autodataingest.email_notifications.receive_gmail_notifications import (check_for_archive_notification, check_for_job_notification, add_jobtimes)

from autodataingest.gsheet_tracker.gsheet_functions import (find_new_tracks, update_track_status,
                                             update_cell, return_cell)

from autodataingest.gsheet_tracker.gsheet_flagging import (download_flagsheet_to_flagtxt)

from autodataingest.globus_functions import (transfer_file, transfer_pipeline,
                               cleanup_source, globus_wait_for_completion,
                               transfer_general)

from autodataingest.get_track_info import match_ebid_to_source

from autodataingest.download_vlaant_corrections import download_vla_antcorr

from autodataingest.ssh_utils import try_run_command, run_command

from autodataingest.archive_request import archive_copy_SDM

# Import dictionary defining the job creation script functions for each
# cluster.
from autodataingest.cluster_configs import (JOB_CREATION_FUNCTIONS, CLUSTERADDRS,
                                            ENDPOINT_INFO)

from autodataingest.utils import uniquify, uniquify_folder

class AutoPipeline(object):
    """
    Handler for the processing pipeline stages. Each instance is defined by the
    exection block (EB) ID for each track.

    Each stage is its own function and is meant to run asynchronously.
    """

    def __init__(self, ebid, sheetname='20A - OpLog Summary'):
        self.ebid = ebid

        self.sheetname = sheetname

        self._grab_sheetdata()

        # TODO: add flags that can provide the stage we need to run from.
        # This enables easy restarting of tracks partially processed.


    def _grab_sheetdata(self):
        '''
        Get info from the google sheet. This is needed to allow for restarting at
        different stages.
        '''

        target = return_cell(self.ebid,
                            #  column=4,
                             name_col="Target",
                             sheetname=self.sheetname)
        config = return_cell(self.ebid,
                            #  column=9,
                             name_col="Configuration",
                             sheetname=self.sheetname)
        track_name = return_cell(self.ebid,
                                #  column=3,
                                 name_col='Trackname',
                                 sheetname=self.sheetname)

        if target is not "None":
            self.target = target
        else:
            self.target = None

        if config is not "None":
            self.config = config
        else:
            self.config = None

        if track_name is not "None":
            self.track_name = track_name
        else:
            self.track_name = None

        self._restart_split_count = 0
        self._restart_lines_count = 0
        self._restart_continuum_count = 0

    def _qa_review_input(self, data_type='continuum'):
        '''
        Request a restart on the jobs.
        '''

        if data_type == 'continuum' or data_type == 'speclines':
            pass
        else:
            raise ValueError(f"data_type must be 'continuum' or 'speclines'. Given {data_type}.")

        name_col=f"Re-run\n{data_type}"

        return return_cell(self.ebid, name_col=name_col, sheetname=self.sheetname)

    @property
    def track_folder_name(self):
        return f"{self.target}_{self.config}_{self.track_name}"

    @property
    def project_code(self):
        if self.track_name is None:
            raise ValueError("The track name could not be found. Cannot find the project code.")

        return self.track_name.split(".")[0]

    async def setup_ssh_connection(self, clustername, user='ekoch',
                                   max_retry_connection=10,
                                   reconnect_waittime=900):
        '''
        Setup and test the ssh connection to the cluster.
        '''

        retry_times = 0
        while True:
            try:
                connect = fabric.Connection(CLUSTERADDRS[clustername],
                                            user=user,
                                            connect_kwargs={'passphrase': globals()['password'] if 'password' in globals() else ""})
                # I'm getting intermittent DNS issues on the CC cloud.
                # This is to handle waiting until the DNS problem goes away
                connect.open()

                break

            except socket.gaierror as e:
                log.info("Encountering DNS issue with exception {e}")
                log.info("Waiting to retry connection")
                await asyncio.sleep(reconnect_waittime)

                retry_times += 1

                if retry_times >= max_retry_connection:
                    raise Exception(f"Reached maximum retries to connect to {clustername}")
        # Test the connection:
        if not try_run_command(connect):
            raise ValueError(f"Cannot login to {CLUSTERADDRS[clustername]}. Requires password.")

        self._connect = connect

    @property
    def connect(self):
        if not hasattr(self, '_connect'):
            raise ValueError('Run `setup_ssh_connection` first to create the ssh connection.')

        return self._connect

    async def archive_request_and_transfer(self, archive_kwargs={},
                                     timewindow=48 * 3600.,
                                     sleeptime=600,
                                     clustername='cc-cedar',
                                     do_cleanup=True,
                                     default_project_code='20A-346',
                                     targets_to_check=['M31', 'M33', 'NGC6822', 'IC10', 'IC1613', 'WLM',
                                                       'NGC604', 'M33_Sarm', 'NGC300']):
        """
        Step 1.

        Request the data be staged from the VLA archive and transfer to destination via globus.
        """

        ebid = self.ebid

        if self.track_name is not None:
            project_code = self.project_code
        else:
            # Otherwise default to the XL code
            project_code = default_project_code

        # First check for an archive notification within the last
        # 48 hr. If one is found, don't re-request the track.
        out = check_for_archive_notification(ebid, timewindow=timewindow,
                                             project_id=project_code)

        if out is None:

            log.info(f'Sending archive request for {ebid}')

            archive_copy_SDM(ebid, **archive_kwargs)

        else:
            log.info(f"Found recent archive request for {ebid}.")

        # Continuum
        update_track_status(ebid, message="Archive download staged",
                            sheetname=self.sheetname,
                            status_col=1)
        # Lines
        update_track_status(ebid, message="Archive download staged",
                            sheetname=self.sheetname,
                            status_col=2)

        # Wait for the notification email that the data is ready for transfer
        while out is None:
            out = check_for_archive_notification(ebid, timewindow=timewindow,
                                                 project_id=project_code)

            await asyncio.sleep(sleeptime)

        # We should have the path on AOC and the full MS name
        # from the email.
        path_to_data, track_name = out

        self.track_name = track_name

        # Update track name in sheet:
        update_cell(ebid, track_name,
                    # num_col=3,
                    name_col="Trackname",
                    sheetname=self.sheetname)

        # Scrap the VLA archive for target and config w/ astroquery
        # This will query the archive for the list of targets until the output has a matching EBID.
        target, datasize = match_ebid_to_source(ebid,
                                                targets=targets_to_check,
                                                project_code=self.project_code,
                                                verbose=False)

        self.target = target

        log.info(f"Found target {target} with size {datasize} for {ebid}")

        # Add track target to the sheet
        update_cell(ebid, target,
                    # num_col=4,
                    name_col="Target",
                    sheetname=self.sheetname)

        # And the data size
        update_cell(ebid, datasize.rstrip('GB'),
                    # num_col=14,
                    name_col="Data Size",
                    sheetname=self.sheetname)

        # We want to easily track (1) target, (2) config, and (3) track name
        # We'll combine these for our folder names where the data will get placed
        # after transfer from the archive.
        config = return_cell(self.ebid,
                            #  column=9,
                             name_col="Configuration",
                             sheetname=self.sheetname)
        self.config = config

        log.info(f"This track was taken in {config} configuration.")
        log.info(f"This track can be found in the folder with name {self.track_folder_name}")

        # Do globus transfer:

        log.info(f"Transferring {self.track_folder_name} to {clustername}.")
        transfer_taskid = transfer_file(track_name, self.track_folder_name,
                                        startnode='nrao-aoc',
                                        endnode=clustername,
                                        wait_for_completion=False)

        self.transfer_taskid = transfer_taskid

        log.info(f"The globus transfer ID is: {transfer_taskid}")

        # Continuum
        update_track_status(ebid,
                            message=f"Data transferred to {clustername}",
                            sheetname=self.sheetname,
                            status_col=1)
        # Lines
        update_track_status(ebid,
                            message=f"Data transferred to {clustername}",
                            sheetname=self.sheetname,
                            status_col=2)

        log.info(f"Waiting for globus transfer to {clustername} to complete.")
        await globus_wait_for_completion(transfer_taskid)
        log.info(f"Globus transfer {transfer_taskid} completed!")

        update_cell(ebid, "TRUE",
                    # num_col=18,
                    name_col='Transferred data',
                    sheetname=self.sheetname)

        # Remove the data staged at NRAO to avoid exceeding our storage quota
        if do_cleanup:
            log.info(f"Cleaning up {ebid} on nrao-aoc")
            cleanup_source(track_name, node='nrao-aoc')


    async def setup_for_reduction_pipeline(self, clustername='cc-cedar',
                                           **ssh_kwargs):

        """
        Step 2.

        Create products and setup on the cluster running the reduction.
        This should be the destination requested in `archive_request_and_transfer`.

        1. Tests connection to cluster.
        2. Clones the ReductionPipeline repo
        TODO: Allow setting a version for the pipeline repo.
        3. Updates + transfers offline copies of the antenna positions corrections.
        """


        log.info(f"Starting connection to {clustername}")

        await self.setup_ssh_connection(clustername, **ssh_kwargs)

        # Grab the repo; this is where we can also specify a version number, too
        cd_command = f'cd scratch/VLAXL_reduction/{self.track_folder_name}/'

        log.info(f"Cloning ReductionPipeline to {clustername} at {cd_command}")

        git_clone_command = 'git clone https://github.com/LocalGroup-VLALegacy/ReductionPipeline.git'
        full_command = f'{cd_command} ; rm -r ReductionPipeline ; {git_clone_command}'
        result = run_command(self.connect, full_command)

        # Before running any reduction, update the antenna correction files
        # and copy that folder to each folder where the pipeline is run
        log.info("Downloading updates of antenna corrections to 'VLA_antcorr_tables'")
        download_vla_antcorr(data_folder="VLA_antcorr_tables")

        # Move the antenna correction folder over:
        log.info(f"Copying antenna corrections to {clustername}")
        result = self.connect.run(f"{cd_command}/VLA_antcorr_tables || mkdir scratch/VLAXL_reduction/{self.track_folder_name}/VLA_antcorr_tables")
        for file in glob("VLA_antcorr_tables/*.txt"):
            result = self.connect.put(file, remote=f"scratch/VLAXL_reduction/{self.track_folder_name}/VLA_antcorr_tables/")

        if self.connect.is_connected:
            self.connect.close()


    async def initial_job_submission(self,
                                    clustername='cc-cedar',
                                    scripts_dir=Path('reduction_job_scripts/'),
                                    split_type='all',
                                    submit_continuum_pipeline=True,
                                    submit_line_pipeline=True,
                                    clusteracct=None,
                                    split_time=None,
                                    continuum_time=None,
                                    line_time=None,
                                    scheduler_cmd="",
                                    **ssh_kwargs):
        """
        Step 3.

        Submit jobs or start the reduction pipeline.

        This has three steps:
        1. Import to MS and continuum/line split
        2. Continuum reduction pipeline
        3. Line reduction pipeline.

        2 and 3 can run concurrently but require 1 to finish first.

        Parameters
        -----------

        """

        log.info(f"Starting job submission of {self.ebid} on {clustername}.")

        # Create local folder where our job submission scripts will be saved to prior to
        # transfer
        track_scripts_dir = scripts_dir / self.track_folder_name

        if not track_scripts_dir.exists():
            track_scripts_dir.mkdir()

        # Setup connection:
        log.info(f"Starting connection to {clustername}")
        await self.setup_ssh_connection(clustername, **ssh_kwargs)

        # Create 1. job to import and split.
        log.info(f"Making import/split job file for {self.ebid} or {self.track_folder_name}")

        job_split_filename = f"{self.track_folder_name}_{split_type}_job_import_and_split.sh"

        if (track_scripts_dir / job_split_filename).exists():
            (track_scripts_dir / job_split_filename).unlink()

        # Create the job script.
        print(JOB_CREATION_FUNCTIONS[clustername]['IMPORT_SPLIT'](
                target_name=self.track_folder_name.split('_')[0],
                config=self.track_folder_name.split('_')[1],
                trackname=self.track_folder_name.split('_')[2],
                split_type=split_type,
                slurm_kwargs={},
                setup_kwargs={}),
            file=open(track_scripts_dir / job_split_filename, 'a'))

        # Move the job script to the cluster:
        log.info(f"Moving import/split job file for {self.ebid} to {clustername}")
        result = self.connect.put(track_scripts_dir / job_split_filename,
                                  remote=f'scratch/VLAXL_reduction/{self.track_folder_name}/')

        chdir_cmd = f"cd scratch/VLAXL_reduction/{self.track_folder_name}/"

        if clusteracct is not None:
            acct_str = f"--account={clusteracct}"
        else:
            acct_str = ""

        if split_time is not None:
            time_str = f"--time={split_time}"
        else:
            time_str = ""

        submit_cmd = f"{scheduler_cmd} {acct_str} {time_str} {job_split_filename}"

        log.info(f"Submitting command: {submit_cmd}")

        try:
            result = run_command(self.connect, f"{chdir_cmd} && {submit_cmd}")
        except ValueError as exc:
            raise ValueError(f"Failed to submit split job! See stderr: {exc}")

        # Record the job ID so we can check for completion.
        self.importsplit_jobid = result.stdout.replace("\n", '').split(" ")[-1]

        log.info(f"Submitted import/split job file for {self.ebid} on {clustername} as job {self.importsplit_jobid}")

        update_cell(self.ebid, f"{clustername}:{self.importsplit_jobid}",
                    # num_col=20,
                    name_col="Split Job ID",
                    sheetname=self.sheetname)


        # Move on to 2. and 3.
        # NEED to make these jobs conditional on 1. finishing.

        if submit_continuum_pipeline:

            log.info(f"Making continuum pipeline job file for {self.ebid} or {self.track_folder_name}")

            job_continuum_filename = f"{self.track_folder_name}_job_continuum.sh"

            # Remove existing job file if it exists
            if (track_scripts_dir / job_continuum_filename).exists():
                (track_scripts_dir / job_continuum_filename).unlink()

            print(JOB_CREATION_FUNCTIONS[clustername]['CONTINUUM_PIPE'](
                    target_name=self.track_folder_name.split('_')[0],
                    config=self.track_folder_name.split('_')[1],
                    trackname=self.track_folder_name.split('_')[2],
                    slurm_kwargs={},
                    setup_kwargs={},
                    conditional_on_jobnum=self.importsplit_jobid),
                file=open(track_scripts_dir / job_continuum_filename, 'a'))

            # Move the job script to the cluster:
            log.info(f"Moving continuum pipeline job file for {self.ebid} to {clustername}")
            result = self.connect.put(track_scripts_dir / job_continuum_filename,
                                remote=f'scratch/VLAXL_reduction/{self.track_folder_name}/')

            if continuum_time is not None:
                time_str = f"--time={continuum_time}"
            else:
                time_str = ""

            submit_cmd = f"{scheduler_cmd} {acct_str} {time_str} {job_continuum_filename}"

            log.info(f"Submitting command: {submit_cmd}")

            try:
                result = run_command(self.connect, f"{chdir_cmd} && {submit_cmd}")
            except ValueError as exc:
                raise ValueError(f"Failed to submit continuum pipeline job! See stderr: {exc}")

            # Record the job ID so we can check for completion.
            self.continuum_jobid = result.stdout.replace("\n", '').split(" ")[-1]

            log.info(f"Submitted continuum pipeline job file for {self.ebid} on {clustername} as job {self.continuum_jobid}")

            update_cell(self.ebid, f"{clustername}:{self.continuum_jobid}",
                        # num_col=22,
                        name_col="Continuum job ID",
                        sheetname=self.sheetname)

            # Continuum
            update_track_status(self.ebid,
                                message=f"Reduction running on {clustername}",
                                sheetname=self.sheetname,
                                status_col=1)

        else:
            self.continuum_jobid = None

        if submit_line_pipeline:

            log.info(f"Making line pipeline job file for {self.ebid} or {self.track_folder_name}")

            job_line_filename = f"{self.track_folder_name}_job_line.sh"

            # Remove existing job file if it exists
            if (track_scripts_dir / job_line_filename).exists():
                (track_scripts_dir / job_line_filename).unlink()

            print(JOB_CREATION_FUNCTIONS[clustername]['LINE_PIPE'](
                    target_name=self.track_folder_name.split('_')[0],
                    config=self.track_folder_name.split('_')[1],
                    trackname=self.track_folder_name.split('_')[2],
                    slurm_kwargs={},
                    setup_kwargs={},
                    conditional_on_jobnum=self.importsplit_jobid),
                file=open(track_scripts_dir / job_line_filename, 'a'))

            # Move the job script to the cluster:
            log.info(f"Moving line pipeline job file for {self.ebid} to {clustername}")
            result = self.connect.put(track_scripts_dir / job_line_filename,
                                remote=f'scratch/VLAXL_reduction/{self.track_folder_name}/')

            if line_time is not None:
                time_str = f"--time={line_time}"
            else:
                time_str = ""

            submit_cmd = f"{scheduler_cmd} {acct_str} {time_str} {job_line_filename}"

            log.info(f"Submitting command: {submit_cmd}")

            # Lines
            update_track_status(self.ebid,
                                message=f"Reduction running on {clustername}",
                                sheetname=self.sheetname,
                                status_col=2)

            try:
                result = run_command(self.connect, f"{chdir_cmd} && {submit_cmd}")
            except ValueError as exc:
                raise ValueError(f"Failed to submit line pipeline job! See stderr: {exc}")

            # Record the job ID so we can check for completion.
            self.line_jobid = result.stdout.replace("\n", '').split(" ")[-1]

            log.info(f"Submitted line pipeline job file for {self.ebid} on {clustername} as job {self.line_jobid}")

            update_cell(self.ebid, f"{clustername}:{self.line_jobid}",
                        # num_col=24,
                        name_col='Line job ID',
                        sheetname=self.sheetname)

        else:
            self.line_jobid = None

        if self.connect.is_connected:
            self.connect.close()


    async def get_job_notifications(self,
                            importsplit_jobid=None,
                            check_split_job=True,
                            check_continuum_job=True,
                            continuum_jobid=None,
                            check_line_job=True,
                            line_jobid=None,
                            sleeptime=1800):
        """
        Step 4.

        Check if the pipeline jobs completed correctly.

        If so, and if a manual flagging sheet doesn't exist, produce a new
        google sheet that the manual flagging txt file will be generated from.
        """

        # if IDs are not available, try getting from the gsheet.
        # otherwise, skip checking for those jobs to finish.

        log.info(f"Checking for job notifications on {self.ebid} or {self.track_folder_name}")

        if importsplit_jobid is None and check_split_job:
            importsplit_jobid = self.importsplit_jobid

            # If still None, pull from the spreadsheet
            if importsplit_jobid is None:
                importsplit_jobid = return_cell(self.ebid, # column=20,
                                                name_col="Split Job ID",
                                                sheetname=self.sheetname).split(":")[-1]

        if continuum_jobid is None and check_continuum_job:
            continuum_jobid = self.continuum_jobid

            # If still None, pull from the spreadsheet
            if continuum_jobid is None:
                continuum_jobid = return_cell(self.ebid, # column=22,
                                              name_col='Continuum job ID',
                                              sheetname=self.sheetname).split(":")[-1]

        if line_jobid is None and check_line_job:
            line_jobid = self.line_jobid

            # If still None, pull from the spreadsheet
            if line_jobid is None:
                line_jobid = return_cell(self.ebid, # column=24,
                                         name_col='Line job ID',
                                         sheetname=self.sheetname).split(":")[-1]

        # If the split job ID is still not defined, something has gone wrong.
        if check_split_job:
            if importsplit_jobid is None or importsplit_jobid == "":
                raise ValueError(f"Unable to identify split job ID for EB: {self.ebid}")

        log.info(f"Waiting for job notifications on {self.ebid} or {self.track_folder_name}")

        while True:
            if not check_split_job:
                is_done_split = True
                break

            # Check for a job completion email and check the final status
            job_check = check_for_job_notification(importsplit_jobid)
            # If None, it isn't done yet!
            if job_check is None:
                await asyncio.sleep(sleeptime)
                continue

            job_status_split, job_runtime =  job_check
            is_done_split = True

            log.info(f"Found import/split notification for {importsplit_jobid} with status {job_status_split}")

            update_cell(self.ebid, job_status_split,
                        # num_col=19,
                        name_col="Line/continuum split",
                        sheetname=self.sheetname)
            update_cell(self.ebid, job_runtime,
                        # num_col=25,
                        name_col="Split job wall time",
                        sheetname=self.sheetname)

            break

        # Continuum check
        while True:
            if not check_continuum_job:
                is_done_continuum = True
                break

            job_check = check_for_job_notification(continuum_jobid)

            is_done_continuum = False
            if job_check is None:
                await asyncio.sleep(sleeptime)
                continue

            is_done_continuum = True

            job_status_continuum, job_runtime =  job_check

            log.info(f"Found continuum notification for {continuum_jobid} with status {job_status_continuum}")

            update_cell(self.ebid, job_status_continuum,
                        # num_col=21,
                        name_col='Continuum reduction',
                        sheetname=self.sheetname)
            update_cell(self.ebid, job_runtime,
                        # num_col=26,
                        name_col="Continuum job wall time",
                        sheetname=self.sheetname)

            break

        # Line check
        while True:
            if not check_line_job:
                is_done_line = True
                break

            job_check = check_for_job_notification(line_jobid)
            if job_check is None:
                await asyncio.sleep(sleeptime)
                continue

            is_done_line = True

            job_status_line, job_runtime = job_check

            log.info(f"Found line notification for {line_jobid} with status {job_status_line}")

            update_cell(self.ebid, job_status_line,
                        # num_col=23,
                        name_col="Line reduction",
                        sheetname=self.sheetname)
            update_cell(self.ebid, job_runtime,
                        # num_col=27,
                        name_col="Line job wall time",
                        sheetname=self.sheetname)

            break

        # Make dictionary for restarting jobs.
        self.restarts = {'IMPORT_SPLIT': False,
                         'CONTINUUM_PIPE': False,
                         'LINE_PIPE': False,}


        if all([is_done_split, is_done_continuum, is_done_line]):

            # Check if these were successful runs:
            # Expected types of job status:
            # COMPLETED - probably a successful pipeline reduction
            # TIMEOUT - ran out of time; trigger resubmitting the job
            # CANCELLED - something happened to the job. Assumed this was for a good reason and don't resubmit

            # TODO: handle timeout and restart jobs to get the total wall time

            job_statuses = []

            if check_split_job:
                job_statuses.append(job_status_split)

            if check_continuum_job:
                job_statuses.append(job_status_continuum)

            if check_line_job:
                job_statuses.append(job_status_line)

            # Good! It worked! Move on to QA.
            if all([job_status == 'COMPLETED' for job_status in job_statuses]):

                log.info(f"Processing complete for {self.ebid}! Ready for QA.")

                update_track_status(self.ebid, message=f"Ready for QA",
                                    sheetname=self.sheetname,
                                    status_col=1)

                update_track_status(self.ebid, message=f"Ready for QA",
                                    sheetname=self.sheetname,
                                    status_col=2)

            # If the split failed, the other two will not have completed.
            # Trigger resubmitting all three:
            if check_split_job:
                if job_status_split == 'TIMEOUT':
                    # Re-add all to submission queue
                    log.info(f"Timeout for split. Needs resubmitting of all jobs")

                    self.restarts['IMPORT_SPLIT'] = True
                    self.restarts['CONTINUUM_PIPE'] = True
                    self.restarts['LINE_PIPE'] = True

                    update_track_status(self.ebid,
                                        message=f"ISSUE: job timed out",
                                        sheetname=self.sheetname,
                                        status_col=1)
                    update_track_status(self.ebid,
                                        message=f"ISSUE: job timed out",
                                        sheetname=self.sheetname,
                                        status_col=2)

                if job_status_split == 'FAILED':

                    self.restarts['IMPORT_SPLIT'] = True
                    self.restarts['CONTINUUM_PIPE'] = True
                    self.restarts['LINE_PIPE'] = True

                    update_track_status(self.ebid,
                                        message=f"ISSUE: Needs manual check of job status",
                                        sheetname=self.sheetname,
                                        status_col=1)

                    update_track_status(self.ebid,
                                        message=f"ISSUE: Needs manual check of job status",
                                        sheetname=self.sheetname,
                                        status_col=2)

            # Trigger resubmitting the continuum
            if check_continuum_job:
                if job_status_continuum == 'TIMEOUT':
                    # Add to resubmission queue
                    log.info(f"Timeout for continuum pipeline. Needs resubmitting of continuum job.")
                    self.restarts['CONTINUUM_PIPE'] = True

                    update_track_status(self.ebid,
                                        message=f"ISSUE: job timed out",
                                        sheetname=self.sheetname,
                                        status_col=1)

                if job_status_continuum == "FAILED":

                    self.restarts['CONTINUUM_PIPE'] = True

                    update_track_status(self.ebid,
                                        message=f"ISSUE: Needs manual check of job status",
                                        sheetname=self.sheetname,
                                        status_col=1)


            # Trigger resubmitting the lines
            if check_line_job:
                if job_status_line == 'TIMEOUT':
                    # Add to resubmission queue
                    log.info(f"Timeout for line pipeline. Needs resubmitting of line job.")
                    self.restarts['LINE_PIPE'] = True

                    update_track_status(self.ebid,
                                        message=f"ISSUE: job timed out",
                                        sheetname=self.sheetname,
                                        status_col=2)

                if job_status_line == "FAILED":

                    self.restarts['LINE_PIPE'] = True

                    update_track_status(self.ebid,
                                        message=f"ISSUE: Needs manual check of job status",
                                        sheetname=self.sheetname,
                                        status_col=2)

        else:
            log.info(f"Not all jobs were run. Needs manual review for {self.ebid}")

            update_track_status(self.ebid,
                                message=f"ISSUE: Not all parts of the reduction were run. Needs manual review.",
                                sheetname=self.sheetname,
                                status_col=1)
            update_track_status(self.ebid,
                                message=f"ISSUE: Not all parts of the reduction were run. Needs manual review.",
                                sheetname=self.sheetname,
                                status_col=2)


    async def restart_job_submission(self, max_resubmission=1,                                     clustername='cc-cedar',
                                     scripts_dir=Path('reduction_job_scripts/'),
                                     split_type='all',
                                     clusteracct=None,
                                     split_time=None,
                                     continuum_time=None,
                                     line_time=None,
                                     scheduler_cmd="",
                                     **ssh_kwargs):

        """
        Step 3b.

        Resubmit incomplete jobs.
        """

        # Check that the restart dictionary is defined
        if not hasattr(self, 'restarts'):
            raise ValueError("restarts is not defined. get_job_notifications must be run first.")

        if clusteracct is not None:
            acct_str = f"--account={clusteracct}"
        else:
            acct_str = ""

        chdir_cmd = f"cd scratch/VLAXL_reduction/{self.track_folder_name}/"

        # Setup connection:
        log.info(f"Starting connection to {clustername}")
        await self.setup_ssh_connection(clustername, **ssh_kwargs)

        # Restart split submission
        if self.restarts['IMPORT_SPLIT']:
            self._restart_split_count += 1

            if self._restart_split_count > max_resubmission:
                log.info("Reached maximum resubmission attempts for split jobs.")
                log.info(f"Manual review of failure is required for {self.ebid}")

                update_track_status(self.ebid,
                                    message=f"ISSUE: Reached resubmission max. Manual check needed.",
                                    sheetname=self.sheetname,
                                    status_col=1)
                update_track_status(self.ebid,
                                    message=f"ISSUE: Reached resubmission max. Manual check needed.",
                                    sheetname=self.sheetname,
                                    status_col=2)

                return

            # All files should already exist for restarts.

            job_split_filename = f"{self.track_folder_name}_{split_type}_job_import_and_split.sh"

            if split_time is not None:
                time_str = f"--time={split_time}"
            else:
                time_str = ""

            submit_cmd = f"{scheduler_cmd} {acct_str} {time_str} {job_split_filename}"

            log.info(f"Submitting command: {submit_cmd}")

            try:
                result = run_command(self.connect, f"{chdir_cmd} && {submit_cmd}")
            except ValueError as exc:
                raise ValueError(f"Failed to submit split job! See stderr: {exc}")

            # Record the job ID so we can check for completion.
            self.importsplit_jobid = result.stdout.replace("\n", '').split(" ")[-1]

            log.info(f"Re-submitted import/split job file for {self.ebid} on {clustername} as job {self.importsplit_jobid}")

            update_cell(self.ebid, f"{clustername}:{self.importsplit_jobid}",
                        name_col="Split Job ID",
                        sheetname=self.sheetname)

        # Restart continuum submission
        if self.restarts['CONTINUUM_PIPE']:
            self._restart_continuum_count += 1

            if self._restart_continuum_count > max_resubmission:
                log.info("Reached maximum resubmission attempts for continuum jobs.")
                log.info(f"Manual review of failure is required for {self.ebid}")

                update_track_status(self.ebid,
                                    message=f"ISSUE: Reached resubmission max. Manual check needed.",
                                    sheetname=self.sheetname,
                                    status_col=1)

            else:
                job_continuum_filename = f"{self.track_folder_name}_job_continuum.sh"

                if continuum_time is not None:
                    time_str = f"--time={continuum_time}"
                else:
                    time_str = ""

                submit_cmd = f"{scheduler_cmd} {acct_str} {time_str} {job_continuum_filename}"

                log.info(f"Submitting command: {submit_cmd}")

                try:
                    result = run_command(self.connect, f"{chdir_cmd} && {submit_cmd}")
                except ValueError as exc:
                    raise ValueError(f"Failed to submit continuum pipeline job! See stderr: {exc}")

                # Record the job ID so we can check for completion.
                self.continuum_jobid = result.stdout.replace("\n", '').split(" ")[-1]

                log.info(f"Resubmitted continuum pipeline job file for {self.ebid} on {clustername} as job {self.continuum_jobid}")

                update_cell(self.ebid, f"{clustername}:{self.continuum_jobid}",
                            name_col="Continuum job ID",
                            sheetname=self.sheetname)

                # Continuum
                update_track_status(self.ebid,
                                    message=f"Reduction running on {clustername}",
                                    sheetname=self.sheetname,
                                    status_col=1)

        # Restart lines submission
        if self.restarts['LINE_PIPE']:
            self._restart_line_count += 1

            if self._restart_line_count > max_resubmission:
                log.info("Reached maximum resubmission attempts for line jobs.")
                log.info(f"Manual review of failure is required for {self.ebid}")

                update_track_status(self.ebid,
                                    message=f"ISSUE: Reached resubmission max. Manual check needed.",
                                    sheetname=self.sheetname,
                                    status_col=2)

            else:

                job_line_filename = f"{self.track_folder_name}_job_line.sh"

                if line_time is not None:
                    time_str = f"--time={line_time}"
                else:
                    time_str = ""

                submit_cmd = f"{scheduler_cmd} {acct_str} {time_str} {job_line_filename}"

                log.info(f"Submitting command: {submit_cmd}")

                try:
                    result = run_command(self.connect, f"{chdir_cmd} && {submit_cmd}")
                except ValueError as exc:
                    raise ValueError(f"Failed to submit line pipeline job! See stderr: {exc}")

                # Record the job ID so we can check for completion.
                self.line_jobid = result.stdout.replace("\n", '').split(" ")[-1]

                log.info(f"Resubmitted line pipeline job file for {self.ebid} on {clustername} as job {self.line_jobid}")

                update_cell(self.ebid, f"{clustername}:{self.line_jobid}",
                            # num_col=24,
                            name_col='Line job ID',
                            sheetname=self.sheetname)

                # Lines
                update_track_status(self.ebid,
                                    message=f"Reduction running on {clustername}",
                                    sheetname=self.sheetname,
                                    status_col=2)

        if self.connect.is_connected:
            self.connect.close()


    async def transfer_pipeline_products(self, data_type='speclines',
                                         startnode='cc-cedar',
                                         endnode='ingester'):
        """
        Step 5.

        Transfer pipeline outputs to a storage system the webserver can access to host.
        """

        # Get info from the spreadsheet.

        if not data_type in ['speclines', 'continuum']:
            raise ValueError(f"Data type must be 'speclines' or 'continuum'. Received {data_type}")

        self._grab_sheetdata()

        if self.target is None or self.track_name is None:
            raise ValueError(f"Cannot find target or trackname in {self.ebid}")

        log.info(f"Transferring {self.track_folder_name} {data_type} products from {startnode} to {endnode}.")

        path_to_products = f'{self.track_folder_name}/{self.track_folder_name}_{data_type}/'

        filename = f'{path_to_products}/{self.track_folder_name}_{data_type}_products.tar'

        # Going to the ingester instance. Doesn't need an extra path.
        output_destination = "/"

        transfer_taskid = transfer_general(filename, output_destination,
                                           startnode=startnode,
                                           endnode=endnode,
                                           wait_for_completion=False,
                                           skip_if_not_existing=True)

        if transfer_taskid is None:
            return

        self.transfer_taskid = transfer_taskid

        log.info(f"The globus transfer ID is: {transfer_taskid}")

        log.info(f"Waiting for globus transfer to {endnode} to complete.")
        await globus_wait_for_completion(transfer_taskid, sleeptime=180)
        log.info(f"Globus transfer {transfer_taskid} completed!")

    async def make_flagging_sheet(self, data_type='continuum'):
        '''
        Create the flagging sheet and remember the URLs to use as links.
        '''

        from autodataingest.gsheet_tracker.gsheet_flagging import make_new_flagsheet

        new_flagsheet = make_new_flagsheet(self.track_name, self.target, self.config,
                                           data_type=data_type,
                                           template_name='TEMPLATE')

        # make_new_flagsheet already checks for continuum vs. speclines
        # Make the equiv google docs link, not the API version
        prefix = "https://docs.google.com/spreadsheets/d/"

        this_sheet_url = new_flagsheet.url.split("spreadsheets/")[1]

        if data_type == "continuum":
            self._continuum_flagsheet_url = f"{prefix}/{this_sheet_url}"
        else:
            self._speclines_flagsheet_url = f"{prefix}/{this_sheet_url}"

    @property
    def continuum_flagsheet_url(self):
        return self._continuum_flagsheet_url

    @property
    def speclines_flagsheet_url(self):
        return self._speclines_flagsheet_url

    def make_qa_products(self, data_type='speclines',
                         verbose=False):
        '''
        Create the QA products for the QA webserver.
        '''

        if not data_type in ['speclines', 'continuum']:
            raise ValueError(f"Data type must be 'speclines' or 'continuum'. Received {data_type}")

        self._grab_sheetdata()

        if self.target is None or self.track_name is None:
            raise ValueError(f"Cannot find target or trackname in {self.ebid}")

        data_path = Path(ENDPOINT_INFO['ingester']['data_path'])


        self.setup_qa_track_path()
        qa_path = self.qa_track_path / data_type

        product_tarname = f"{self.track_folder_name}_{data_type}_products.tar"
        product_file = data_path / product_tarname

        if not os.path.exists(product_file):
            log.warning(f"Unable to find products file at {product_file}")
            return

        # Make a temp folder to extract into:
        temp_path = product_file.with_suffix("")

        if os.path.exists(temp_path):
            shutil.rmtree(temp_path)

        os.mkdir(temp_path)

        # Extract weblog
        task_command = ['tar', '--strip-components=1', '-C',
                        f"{temp_path}", '-xf', f"{product_file}",
                        "products/weblog.tgz"]

        task_weblog1 = subprocess.run(task_command, capture_output=True)

        # Extract cal plots
        task_command = ['tar', '--strip-components=1', '-C',
                        f"{temp_path}", '-xf', f"{product_file}",
                        "products/finalBPcal_txt"]

        task_caltxt = subprocess.run(task_command, capture_output=True)

        task_command = ['tar', '--strip-components=1', '-C',
                        f"{temp_path}", '-xf', f"{product_file}",
                        "products/final_caltable_txt"]

        task_caltxt = subprocess.run(task_command, capture_output=True)

        # Extract scan plots
        task_command = ['tar', '--strip-components=1', '-C',
                        f"{temp_path}", '-xf', f"{product_file}",
                        "products/scan_plots_txt"]

        task_scantxt = subprocess.run(task_command, capture_output=True)

        cur_dir = os.getcwd()

        os.chdir(temp_path)

        # Extract the weblog
        os.mkdir('weblog')

        task_command = ['tar', '--strip-components=1', '-C',
                        "weblog", '-xf', "weblog.tgz"]

        task_weblog2 = subprocess.run(task_command, capture_output=True)

        if verbose:
            log.info(f"The extracted files are: {os.listdir()}")

        if os.path.exists('weblog'):
            os.remove('weblog.tgz')

        # Generate the QA products:
        import qaplotter
        if data_type == 'continuum':
            flagging_sheet_link = self.continuum_flagsheet_url
        elif data_type == 'speclines':
            flagging_sheet_link = self.speclines_flagsheet_url
        else:
            raise ValueError(f"data_type must be 'continuum' or 'speclines'. Given {data_type}")
        qaplotter.make_all_plots(flagging_sheet_link=flagging_sheet_link)

        # Return the original directory
        os.chdir(cur_dir)

        # Check if the name is already in the qa path:

        new_qa_path = qa_path / os.path.split(temp_path)[-1]
        # Add a unique 1,2,3, etc to make sure the name is unique
        new_qa_path =  uniquify_folder(new_qa_path)

        # Open permission for the webserver to read and access the files
        task_command = ['chmod', '-R', 'o+rx', temp_path]

        task_chmod = subprocess.run(task_command, capture_output=True)
        log.debug(f"The task was: {task_command}")
        task_chmod_stdout = task_chmod.stdout.decode('utf-8').replace("\n", " ")
        log.debug(f"Stdout: {task_chmod_stdout}")
        task_chmod_stderr = task_chmod.stderr.decode('utf-8').replace("\n", " ")
        log.debug(f"Stderr: {task_chmod_stderr}")

        # Move to the directory of the webserver:
        task_command = ['mv', temp_path, new_qa_path]

        task_move = subprocess.run(task_command, capture_output=True)
        log.debug(f"The task was: {task_command}")
        task_move_stdout = task_move.stdout.decode('utf-8').replace("\n", " ")
        log.debug(f"Stdout: {task_move_stdout}")
        task_move_stderr = task_move.stderr.decode('utf-8').replace("\n", " ")
        log.debug(f"Stderr: {task_move_stderr}")

        # Now move the tar file to "processed" folder:
        proced_folder = data_path / "processed"
        proced_folder.mkdir(parents=True, exist_ok=True)

        proced_file = uniquify(proced_folder / product_tarname)

        task_command = ['mv', product_file, proced_file]

        task_move = subprocess.run(task_command, capture_output=True)
        log.debug(f"The task was: {task_command}")
        task_move_stdout = task_move.stdout.decode('utf-8').replace("\n", " ")
        log.debug(f"Stdout: {task_move_stdout}")
        task_move_stderr = task_move.stderr.decode('utf-8').replace("\n", " ")
        log.debug(f"Stderr: {task_move_stderr}")

    @property
    def qa_track_path(self):
        '''
        Location for all QA products on the webserver.
        '''

        qa_path = Path(ENDPOINT_INFO['ingester']['qa_path'])

        return qa_path / self.project_code / self.track_folder_name

    def setup_qa_track_path(self):
        '''
        Create the folder structure that will be ingested into the webserver.

        PROJCODE / TRACKNAME / {continuum/speclines} /

        '''

        self.qa_track_path.mkdir(parents=True, exist_ok=True)
        (self.qa_track_path / 'continuum').mkdir(parents=True, exist_ok=True)
        (self.qa_track_path / 'speclines').mkdir(parents=True, exist_ok=True)

    async def get_flagging_files(self,
                                 clustername='cc-cedar',
                                 data_type='continuum',
                                 output_folder=os.path.expanduser('FlagRepository'),
                                 scripts_dir=Path('reduction_job_scripts/'),
                                 **ssh_kwargs,
                                 ):
        '''
        1. Download the flagging file
        TODO:
        2. Copy to git repo, make commit, push to gihub
        '''

        if not data_type in ['continuum', 'speclines']:
            raise ValueError(f"data_type must be 'continuum' or 'speclines'. Given {data_type}")

        from autodataingest.gsheet_tracker.gsheet_flagging import download_flagsheet_to_flagtxt

        flag_repo_path = Path(output_folder) / self.project_code
        flag_repo_path.mkdir(parents=True, exist_ok=True)

        flag_repo_path_type = flag_repo_path / data_type
        flag_repo_path_type.mkdir(parents=True, exist_ok=True)

        filename = download_flagsheet_to_flagtxt(self.track_name,
                                                self.target,
                                                self.config,
                                                flag_repo_path_type,
                                                data_type=data_type,
                                                raise_noflag_error=False,
                                                debug=False,
                                                test_against_previous=True)

        if filename is None:
            log.info(f"Unable to find a manual flagging sheet for {self.track_name}")
            return

        # Copy to the same folder that job scripts are/will be in
        track_scripts_dir = scripts_dir / self.track_folder_name

        if not track_scripts_dir.exists():
            track_scripts_dir.mkdir()

        newfilename = track_scripts_dir / f'manual_flagging_{data_type}.txt'

        task_command = ['cp', filename, newfilename]

        task_copy = subprocess.run(task_command, capture_output=True)

        log.info(f"Starting connection to {clustername}")

        await self.setup_ssh_connection(clustername, **ssh_kwargs)

        result = self.connect.put(newfilename,
                                  remote=f"scratch/VLAXL_reduction/{self.track_folder_name}/")

        # TODO: If changed, make git commit and push

    async def rerun_job_submission(self,
                                   clustername='cc-cedar',
                                   data_type='continuum',
                                   clusteracct=None,
                                   split_time=None,
                                   pipeline_time=None,
                                   scheduler_cmd=''):

        """
        Step 7.

        After QA, supplies an additional manual flagging script to re-run the pipeline
        calibration.
        """

        status_flag = self._qa_review_input(data_type=data_type)

        if status_flag != "RESTART":
            log.debug("No restart requested. Exiting")
            return

        update_track_status(self.ebid, message=f"Restarting pipeline for re-run",
                            sheetname=self.sheetname,
                            status_col=1 if data_type == 'continuum' else 2)

        # Need to reset the "RESTART" in the track spreadsheet to avoid multiple re-runs
        update_cell(self.ebid, "",
                    # num_col=28 if data_type == 'continuum' else 29,
                    name_col=f"Re-run\n{data_type}",
                    sheetname=self.sheetname)

        await self.cleanup_on_cluster(clustername=clustername,
                                      data_type=data_type)

        # Download manual flagging files from the google sheet.
        await self.get_flagging_files(clustername=clustername,
                                      data_type=data_type)

        await self.setup_for_reduction_pipeline(clustername=clustername)

        await self.initial_job_submission(clustername=clustername,
                                        scripts_dir=Path('reduction_job_scripts/'),
                                        split_type=data_type,
                                        submit_continuum_pipeline=True if data_type == 'continuum' else False,
                                        submit_line_pipeline=True if data_type == 'speclines' else False,
                                        clusteracct=clusteracct,
                                        split_time=split_time,
                                        continuum_time=pipeline_time,
                                        line_time=pipeline_time,
                                        scheduler_cmd=scheduler_cmd)

        update_track_status(self.ebid,
                            message=f"Reduction running on {clustername} after QA check",
                            sheetname=self.sheetname,
                            status_col=1 if data_type == 'continuum' else 2)

    async def cleanup_on_cluster(self, clustername='cc-cedar', data_type='continuum',
                                 do_remove_whole_track=False,
                                 **ssh_kwargs):
        '''
        Remove a previous run to setup for a new split and new pipeline reduction.

        NOTE: `do_remove_whole_track` deletes the whole track from scratch space!
        Use only when QA is finished.

        '''

        log.info(f"Starting connection to {clustername} for cleanup of {data_type}")

        await self.setup_ssh_connection(clustername, **ssh_kwargs)

        if not do_remove_whole_track:
            # Change to the track directory, then delete the request data type folder
            # (continuum or speclines)

            cd_command = f'cd scratch/VLAXL_reduction/{self.track_folder_name}/'

            log.info(f"Cleaning up {data_type} on {clustername} at {cd_command}")

            rm_command = f"rm -rf {self.track_folder_name}_{data_type}"
        else:

            cd_command = f'cd scratch/VLAXL_reduction/'

            log.info(f"Final clean up on {clustername} for track {cd_command}")

            rm_command = f"rm -rf {self.track_folder_name}"

        full_command = f'{cd_command} && {rm_command}'
        result = run_command(self.connect, full_command)

        if self.connect.is_connected:
            self.connect.close()


    async def export_track_for_imaging(self,
                                       clustername='cc-cedar',
                                       data_type='continuum',
                                       project_dir="/project/rrg-eros-ab/ekoch/VLAXL/calibrated/"):
        """
        Step 8.

        Move calibrated MSs to a persistent storage location for imaging.
        Clean-up scratch space.
        """

        # Status check.

        status_flag = self._qa_review_input(data_type=data_type)

        # Skip if completion is not indicated
        if status_flag != "COMPLETE":
            log.debug("No completion step requested. Exiting")
            return

        # Transfer the MS with globus to its place on project space.
        log.info(f"Transferring {self.track_folder_name} {data_type} calibrated MS to project space.")

        path_to_products = f'{self.track_folder_name}/{self.track_folder_name}_{data_type}/'

        filename = f'{path_to_products}/{self.track_folder_name}.{data_type}.ms.tar'

        # Going to the ingester instance. Doesn't need an extra path.
        output_destination = project_dir

        log.info(f"Filename to transfer is: {filename}")
        log.info(f"Transferring to: {output_destination}")

        transfer_taskid = transfer_general(filename, output_destination,
                                           startnode=clustername,
                                           endnode=clustername,
                                           wait_for_completion=False,
                                           skip_if_not_existing=True,
                                           use_startnode_datapath=True,
                                           use_endnode_datapath=False,
                                           use_rootname=True)

        if transfer_taskid is None:
            log.debug(f"No transfer task ID returned. Check existence of {filename}."
                  " Exiting completion process.")
            return

        self.transfer_taskid = transfer_taskid

        log.info(f"The globus transfer ID is: {transfer_taskid}")

        log.info(f"Waiting for globus transfer to {clustername} to complete.")
        await globus_wait_for_completion(transfer_taskid, sleeptime=180)
        log.info(f"Globus transfer {transfer_taskid} completed!")


        # Update track status. Append both data types if one has already finished
        other_data_type = "speclines" if data_type == 'continuum' else 'continuum'
        current_status = return_cell(self.ebid,
                                    #  column=1,
                                     name_col=f'Status: {other_data_type}',
                                     sheetname=self.sheetname)

        if "Ready for imaging" in current_status:
            other_part_finished = True
        else:
            other_part_finished = False

        update_track_status(self.ebid,
                            message=f"Ready for imaging",
                            sheetname=self.sheetname,
                            status_col=1 if data_type == 'continuum' else 2)

        # Clean up scratch space.
        # Need to check if both components are finished to clean up entire space.

        # If the other component is finished already, we can clean up the whole track from scratch
        do_remove_whole_track = other_part_finished

        await self.cleanup_on_cluster(clustername=clustername, data_type=data_type,
                                      do_remove_whole_track=do_remove_whole_track)

        # Last, make sure we have cleaned up the SDM on AOC:
        cleanup_source(self.track_name, node='nrao-aoc')

        # Remove completion flag to avoid re-runs
        update_cell(self.ebid, "",
                    # num_col=28 if data_type == 'continuum' else 29,
                    name_col=f"Re-run\n{data_type}",
                    sheetname=self.sheetname)


    async def label_qa_failures(self, data_type='continuum'):
        """
        Note failing tracks or those that require manual reduction attempts.
        """

        status_flag = self._qa_review_input(data_type=data_type)

        if status_flag != "MANUAL REVIEW":
            log.debug("No restart requested. Exiting")
            return

        update_track_status(self.ebid,
                        message=f"FAILED QA: Requires manual review.",
                        sheetname=self.sheetname,
                        status_col=1 if data_type == 'continuum' else 2)

        # Remove review flag to avoid re-runs
        update_cell(self.ebid, "",
                    # num_col=28 if data_type == 'continuum' else 29,
                    name_col=f"Re-run\n{data_type}",
                    sheetname=self.sheetname)
