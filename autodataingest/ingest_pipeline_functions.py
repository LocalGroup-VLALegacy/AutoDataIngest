
'''
These are top-level functions for the major steps in the ingestion pipeline.

They are meant to be called in "main.py"
'''

import sys
import os
from pathlib import Path
from glob import glob
import asyncio
import subprocess
import shutil

from autodataingest.logging import setup_logging
log = setup_logging()


from autodataingest.email_notifications.receive_gmail_notifications import (check_for_archive_notification, check_for_job_notification, add_jobtimes)

from autodataingest.gsheet_tracker.gsheet_functions import (find_new_tracks, update_track_status,
                                             update_cell, return_cell, download_refant_summsheet)

from autodataingest.gsheet_tracker.gsheet_flagging import (download_flagsheet_to_flagtxt)

from autodataingest.globus_functions import (transfer_file, transfer_pipeline,
                               cleanup_source, globus_wait_for_completion,
                               transfer_general)

from autodataingest.get_track_info import match_ebid_to_source

from autodataingest.download_vlaant_corrections import download_vla_antcorr

from autodataingest.ssh_utils import (run_command,
                                      time_limit,
                                      run_job_submission,
                                      setup_ssh_connection)

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

    def __init__(self, ebid, sheetname='20A - OpLog Summary',
                 ssh_retry_waittime=600,
                 ssh_max_connect_time=600,
                 ssh_max_retries=10):
        self.ebid = ebid

        self.sheetname = sheetname

        # Set time limits on all ssh connections.
        # We need to catch cases where the connection fails or hangs,
        # and force a retry
        self._ssh_retry_waitime = ssh_retry_waittime
        self._ssh_max_connect_time = ssh_max_connect_time
        self._ssh_max_retries = ssh_max_retries

        self._restart_split_count = 0
        self._restart_lines_count = 0
        self._restart_continuum_count = 0

        self._grab_sheetdata()

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

    async def setup_ssh_connection(self, clustername,
                                   max_retry_connection=10,
                                   connection_timeout=60,
                                   reconnect_waittime=900):
        '''
        Setup and test the ssh connection to the cluster.
        '''

        connect = setup_ssh_connection(clustername,
                                       max_retry_connection=max_retry_connection,
                                       connection_timeout=connection_timeout,
                                       reconnect_waittime=reconnect_waittime)

        log.info(f"Returned connection for {clustername} running {self.track_folder_name}")

        return connect

    async def initial_status(self):
        '''
        Set a status to stop new tracks being re-added to the new track queue.
        '''
        ebid = self.ebid

        log.info(f"Adding start status for {ebid}.")

        # Continuum
        update_track_status(ebid, message="Queued",
                            sheetname=self.sheetname,
                            status_col=1)
        # Lines
        update_track_status(ebid, message="Queued",
                            sheetname=self.sheetname,
                            status_col=2)

    def set_qa_queued_status(self, data_type='continuum'):
        '''
        Set a status to stop new tracks being re-added to the new track queue.
        '''
        ebid = self.ebid

        log.info(f"Adding start status for {ebid}.")

        # Continuum
        if data_type == "continuum":
            update_track_status(ebid, message="Queued for QA/product transfer",
                                sheetname=self.sheetname,
                                status_col=1)
        # Lines
        elif data_type == "speclines":
            update_track_status(ebid, message="Queued for QA/product transfer",
                                sheetname=self.sheetname,
                                status_col=2)
        else:
            log.error(f"Unable to set QA queued status for {data_type} with EBID: {self.ebid}")

    async def archive_request_and_transfer(self,
                                           do_archiverequest=False,
                                           archive_kwargs={},
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

        if do_archiverequest:

            # First check for an archive notification within the last
            # 48 hr. If one is found, don't re-request the track.
            out = check_for_archive_notification(ebid, timewindow=timewindow,
                                                project_id=project_code)

            if out is None:

                log.info(f'Sending archive request for {ebid}')

                if not 'project_code' in archive_kwargs:
                    archive_kwargs['project_code'] = project_code

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

        else:
            # This should now be wrapped into a prior check on the file existence.
            assert self.track_name is not None


        # Update track name in sheet:
        update_cell(ebid, self.track_name,
                    # num_col=3,
                    name_col="Trackname",
                    sheetname=self.sheetname)

        # Scrap the VLA archive for target and config w/ astroquery
        # This will query the archive for the list of targets until the output has a matching EBID.
        # target, datasize = match_ebid_to_source(ebid,
        #                                         targets=targets_to_check,
        #                                         project_code=self.project_code,
        #                                         verbose=False)

        assert self.target is not None
        # self.target = target

        # log.info(f"Found target {target} with size {datasize} for {ebid}")
        log.info(f"Found target {self.target} for {ebid}")

        # Add track target to the sheet
        # update_cell(ebid, target,
        #             # num_col=4,
        #             name_col="Target",
        #             sheetname=self.sheetname)

        # And the data size
        # update_cell(ebid, datasize.rstrip('GB'),
        #             # num_col=14,
        #             name_col="Data Size",
        #             sheetname=self.sheetname)

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
        transfer_taskid = transfer_file(self.track_name, self.track_folder_name,
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
            cleanup_source(self.track_name, node='nrao-aoc')


    async def setup_for_reduction_pipeline(self,
                                           pipeline_branch='main',
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

        # Before running any reduction, update the antenna correction files
        # and copy that folder to each folder where the pipeline is run
        log.info("Downloading updates of antenna corrections to 'VLA_antcorr_tables'")
        download_vla_antcorr(data_folder="VLA_antcorr_tables")

        ssh_retry_times = 0

        while True:
            try:
                with time_limit(self._ssh_max_connect_time):

                    cluster_key = "cedar-robot-jobsetup"
                    log.info(f"Starting connection to {cluster_key} on try {ssh_retry_times}")

                    connect = await self.setup_ssh_connection(cluster_key,
                                                              **ssh_kwargs)

                    log.info(f"Returned connection for {cluster_key}")

                    # Grab the repo; this is where we can also specify a version number, too
                    cd_location = f'scratch/VLAXL_reduction/{self.track_folder_name}/'

                    log.info(f"Cloning ReductionPipeline to {cluster_key} at {cd_location}")

                    full_command = f'{cd_location} {pipeline_branch}'
                    result = run_command(connect, full_command)

                    connect.close()

                    # Move the antenna correction folder over:
                    cluster_key = "cedar-robot-generic"
                    log.info(f"Copying antenna corrections to {cluster_key}")
                    log.info(f"Starting connection to {cluster_key} on try {ssh_retry_times}")

                    connect = await self.setup_ssh_connection(cluster_key,
                                                              **ssh_kwargs)

                    log.info(f"Returned connection for {cluster_key}")

                    antcorr_location = f"{cd_location}/VLA_antcorr_tables"
                    result = connect.run(f"mkdir -p {antcorr_location}")
                    for file in glob("VLA_antcorr_tables/*.txt"):
                        result = connect.put(file, remote=f"{antcorr_location}/")
                    connect.close()

                    del connect

                    break

            except TimeoutError:

                ssh_retry_times += 1

                if ssh_retry_times >= self._ssh_max_retries:
                    raise TimeoutError("Reached maximum number of retries.")

                await asyncio.sleep(self._ssh_retry_waitime)


    async def initial_job_submission(self,
                                     clustername='cc-cedar',
                                    scripts_dir=Path('reduction_job_scripts/'),
                                    split_type='all',
                                    reindex=False,
                                    casa_version="6.2",
                                    submit_continuum_pipeline=True,
                                    submit_line_pipeline=True,
                                    split_time=None,
                                    continuum_time=None,
                                    line_time=None,
                                    split_mem=None,
                                    continuum_mem=None,
                                    line_mem=None,
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

        cluster_key = "cedar-robot-generic"

        log.info(f"Starting job submission of {self.ebid} on {cluster_key}.")

        # Create local folder where our job submission scripts will be saved to prior to
        # transfer
        track_scripts_dir = scripts_dir / self.track_folder_name

        if not track_scripts_dir.exists():
            track_scripts_dir.mkdir()

        # Setup connection:
        log.info(f"Starting connection to {cluster_key}")

        connect = await self.setup_ssh_connection(cluster_key, **ssh_kwargs)
        log.info(f"Returned connection for {cluster_key}")

        # Create 1. job to import and split.
        log.info(f"Making import/split job file for {self.ebid} or {self.track_folder_name}")

        job_split_filename = f"{self.track_folder_name}_{split_type}_job_import_and_split.sh"

        if (track_scripts_dir / job_split_filename).exists():
            (track_scripts_dir / job_split_filename).unlink()

        slurm_split_kwargs = {}

        if split_time is not None:
            slurm_split_kwargs['job_time'] = split_time
        if split_mem is not None:
            slurm_split_kwargs['mem'] = split_mem

        # Create the job script.
        print(JOB_CREATION_FUNCTIONS[clustername]['IMPORT_SPLIT'](
                target_name=self.track_folder_name.split('_')[0],
                config=self.track_folder_name.split('_')[1],
                trackname=self.track_folder_name.split('_')[2],
                split_type=split_type,
                reindex=reindex,
                slurm_kwargs=slurm_split_kwargs,
                setup_kwargs={},
                casa_version=casa_version),
            file=open(track_scripts_dir / job_split_filename, 'a'))

        # Move the job script to the cluster:
        log.info(f"Moving import/split job file for {self.ebid} to {cluster_key}")
        result = connect.put(track_scripts_dir / job_split_filename,
                             remote=f'scratch/VLAXL_reduction/{self.track_folder_name}/')

        # Setup connection:
        cluster_key_submit = 'cedar-submitter'
        log.info(f"Starting connection to {cluster_key_submit}")

        connect_submit = await self.setup_ssh_connection(cluster_key_submit, **ssh_kwargs)
        log.info(f"Returned connection for {cluster_key_submit}")

        # arg0
        chdir_cmd = f"scratch/VLAXL_reduction/{self.track_folder_name}/"

        log.info(f"Submitting job file: {job_split_filename}")

        try:
            # Try to avoid needing an extra sacct run in run_job_submission

            result = connect_submit.run(f"{chdir_cmd} {job_split_filename}")
            split_jobid = result.stdout.replace("\n", '').split(" ")[-1]

            # result = run_command(connect, f"{chdir_cmd} && {submit_cmd}")
            # split_jobid = await run_job_submission(connect, f"{chdir_cmd} && {submit_cmd}",
            #                                       self.track_name, 'import_and_split')
        except ValueError as exc:
            split_jobid = None
            raise ValueError(f"Failed to submit split job! See stderr: {exc}")

        # Record the job ID so we can check for completion.
        self.importsplit_jobid = split_jobid

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


            slurm_continuum_kwargs = {}

            if continuum_time is not None:
                slurm_continuum_kwargs['job_time'] = continuum_time

            if continuum_mem is not None:
                slurm_continuum_kwargs['mem'] = continuum_mem

            print(JOB_CREATION_FUNCTIONS[clustername]['CONTINUUM_PIPE'](
                    target_name=self.track_folder_name.split('_')[0],
                    config=self.track_folder_name.split('_')[1],
                    trackname=self.track_folder_name.split('_')[2],
                    slurm_kwargs=slurm_continuum_kwargs,
                    setup_kwargs={},
                    conditional_on_jobnum=self.importsplit_jobid,
                    casa_version=casa_version),
                file=open(track_scripts_dir / job_continuum_filename, 'a'))

            # Move the job script to the cluster:
            log.info(f"Moving continuum pipeline job file for {self.ebid} to {clustername}")
            result = connect.put(track_scripts_dir / job_continuum_filename,
                                remote=f'scratch/VLAXL_reduction/{self.track_folder_name}/')

            log.info(f"Submitting job file: {job_continuum_filename}")

            try:
                result = connect_submit.run(f"{chdir_cmd} {job_continuum_filename}")
                continuum_jobid = result.stdout.replace("\n", '').split(" ")[-1]

                # result = run_command(connect, f"{chdir_cmd} && {submit_cmd}")
                # continuum_jobid = await run_job_submission(connect, f"{chdir_cmd} && {submit_cmd}",
                #                                            self.track_name, 'continuum_pipeline')
            except ValueError as exc:
                continuum_jobid = None
                raise ValueError(f"Failed to submit continuum pipeline job! See stderr: {exc}")

            # Record the job ID so we can check for completion.
            self.continuum_jobid = continuum_jobid

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

            slurm_line_kwargs = {}
            if line_time is not None:
                slurm_line_kwargs['job_time'] = line_time

            if line_mem is not None:
                slurm_line_kwargs['mem'] = line_mem

            print(JOB_CREATION_FUNCTIONS[clustername]['LINE_PIPE'](
                    target_name=self.track_folder_name.split('_')[0],
                    config=self.track_folder_name.split('_')[1],
                    trackname=self.track_folder_name.split('_')[2],
                    slurm_kwargs=slurm_line_kwargs,
                    setup_kwargs={},
                    conditional_on_jobnum=self.importsplit_jobid,
                    casa_version=casa_version),
                file=open(track_scripts_dir / job_line_filename, 'a'))

            # Move the job script to the cluster:
            log.info(f"Moving line pipeline job file for {self.ebid} to {clustername}")
            result = connect.put(track_scripts_dir / job_line_filename,
                                remote=f'scratch/VLAXL_reduction/{self.track_folder_name}/')

            log.info(f"Submitting job file: {job_line_filename}")

            # Lines
            update_track_status(self.ebid,
                                message=f"Reduction running on {clustername}",
                                sheetname=self.sheetname,
                                status_col=2)

            try:
                result = connect_submit.run(f"{chdir_cmd} {job_line_filename}")
                line_jobid = result.stdout.replace("\n", '').split(" ")[-1]

                # result = run_command(connect, f"{chdir_cmd} && {submit_cmd}")
                # line_jobid = await run_job_submission(connect, f"{chdir_cmd} && {submit_cmd}",
                #                                       self.track_name, 'line_pipeline')
            except ValueError as exc:
                line_jobid = None
                raise ValueError(f"Failed to submit line pipeline job! See stderr: {exc}")

            # Record the job ID so we can check for completion.
            self.line_jobid = line_jobid

            log.info(f"Submitted line pipeline job file for {self.ebid} on {clustername} as job {self.line_jobid}")

            update_cell(self.ebid, f"{clustername}:{self.line_jobid}",
                        # num_col=24,
                        name_col='Line job ID',
                        sheetname=self.sheetname)

        else:
            self.line_jobid = None

        if connect.is_connected:
            connect.close()
            del connect

        if connect_submit.is_connected:
            connect_submit.close()
            del connect_submit

        log.info(f"Finished submitting pipeline for {self.ebid} on {clustername}")

    def set_job_status(self, data_type, job_status):
        """
        Function to set the status of a job based on data type and job status.

        Parameters:
            data_type (str): The type of data being processed.
            job_status (str): The status of the job.

        Raises:
            ValueError: If an unknown data_type is passed.

        Returns:
            None
        """

        if data_type == 'continuum':
            status_col = 1
        elif data_type == 'speclines':
            status_col = 2
        else:
            raise ValueError(f"Unknown data_type passed: {data_type}")

        if job_status == 'TIMEOUT':

            update_track_status(self.ebid,
                                message=f"ISSUE: job timed out",
                                sheetname=self.sheetname,
                                status_col=status_col)

        if job_status in ["FAILED", "OUT_OF_MEMORY", "CANCELLED", "NODE_FAIL"]:

            update_track_status(self.ebid,
                                message=f"ISSUE: Failure with state {job_status}",
                                sheetname=self.sheetname,
                                status_col=status_col)

        if job_status == "COMPLETED":

            update_track_status(self.ebid, message=f"Ready for QA",
                                sheetname=self.sheetname,
                                status_col=status_col)

    def set_job_stats(self, job_id, job_type):

        job_check = check_for_job_notification(job_id)

        if job_check is None:
            log.info(f"Unable to find notification for job ID: {job_id}")
            return

        job_status, job_runtime =  job_check

        log.info(f"Found notification for {job_id}:{job_type} with status {job_status}")

        if job_type == "continuum":
            update_cell(self.ebid, job_status,
                        name_col='Continuum reduction',
                        sheetname=self.sheetname)
            update_cell(self.ebid, job_runtime,
                        name_col="Continuum job wall time",
                        sheetname=self.sheetname)
        elif job_type == "speclines":
            update_cell(self.ebid, job_status,
                        name_col='Line reduction',
                        sheetname=self.sheetname)
            update_cell(self.ebid, job_runtime,
                        name_col="Line job wall time",
                        sheetname=self.sheetname)
        elif job_type == "import_and_split":
            update_cell(self.ebid, job_status,
                        name_col='Line/continuum split',
                        sheetname=self.sheetname)
            update_cell(self.ebid, job_runtime,
                        name_col="Split Job ID",
                        sheetname=self.sheetname)
        else:
            log.error(f"Unable to interpret job_type {job_type}")


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

    async def make_flagging_sheet(self, data_type='continuum',
                                  use_flagging_template=True):
        '''
        Create the flagging sheet and remember the URLs to use as links.
        '''

        from autodataingest.gsheet_tracker.gsheet_flagging import make_new_flagsheet

        if use_flagging_template:
            if data_type == "continuum":
                template_name = "TEMPLATE-CONTINUUM"
            elif data_type == "speclines":
                template_name = "TEMPLATE-SPECLINES"
            else:
                raise ValueError(f"Unable to interpret data_type: {data_type}")
        else:
            # Otherwise use the blank template.
            template_name = "TEMPLATE"

        new_flagsheet = make_new_flagsheet(self.track_name, self.target, self.config,
                                           data_type=data_type,
                                           template_name=template_name)

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
                         verbose=False,
                         do_update_track_status=True):
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

        # Extract quicklook images
        task_command = ['tar', '--strip-components=1', '-C',
                        f"{temp_path}", '-xf', f"{product_file}",
                        "products/quicklook_imaging"]

        task_qlimages = subprocess.run(task_command, capture_output=True)

        # Extract quicklook images
        task_command = ['tar', '--strip-components=1', '-C',
                        f"{temp_path}", '-xf', f"{product_file}",
                        "products/spw_definitions.npy"]

        task_spwdict = subprocess.run(task_command, capture_output=True)

        cur_dir = os.getcwd()

        os.chdir(temp_path)

        # Extract the weblog
        os.mkdir('weblog')

        task_command = ['tar', '--strip-components=1', '-C',
                        "weblog", '-xf', "weblog.tgz"]

        task_weblog2 = subprocess.run(task_command, capture_output=True)

        # Update the weblog file permissions for the webserver
        # We restrict the permission via the webserver on transfer.
        task_command = ['chmod', '-R', '775', "weblog"]
        task_weblogchmod = subprocess.run(task_command, capture_output=True)

        if verbose:
            log.info(f"The extracted files are: {os.listdir()}")

        if os.path.exists('weblog'):
            os.remove('weblog.tgz')

        # Generate the QA products:
        if data_type == 'continuum':
            flagging_sheet_link = self.continuum_flagsheet_url
        elif data_type == 'speclines':
            flagging_sheet_link = self.speclines_flagsheet_url
        else:
            raise ValueError(f"data_type must be 'continuum' or 'speclines'. Given {data_type}")

        kwarg_strs = f"flagging_sheet_link='{flagging_sheet_link}', show_target_linesonly=True"

        task_command = ['ipython', '-c',
                        f'"import qaplotter; qaplotter.make_all_plots({kwarg_strs})"']

        log.info(f"Running qaplotting.")
        log.info(" ".join(task_command))

        task_qaplot_make = subprocess.run(task_command, capture_output=True)

        log.info(task_qaplot_make.stdout)

        # Clean up the original txt files and images. These are kept in
        # the tar files and do not need to be duplicated on the webserver.
        task_command = ['rm', '-r', "quicklook_images"]
        task_cleanup = subprocess.run(task_command, capture_output=True)
        task_command = ['rm', '-r', "final_caltable_txt"]
        task_cleanup = subprocess.run(task_command, capture_output=True)
        task_command = ['rm', '-r', "scan_plots_txt"]
        task_cleanup = subprocess.run(task_command, capture_output=True)

        # Return the original directory
        os.chdir(cur_dir)

        # Check if the name is already in the qa path:

        new_qa_path = qa_path / os.path.split(temp_path)[-1]
        # Add a unique 1,2,3, etc to make sure the name is unique
        new_qa_path =  uniquify_folder(new_qa_path)

        # Open permission for the webserver to read and access the files
        # Allow write so that the webserver's rsync can remove the source
        # files after transfer. Then we don't keep 2 copies everytime.
        task_command = ['chmod', '-R', '775', temp_path]

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

        # Update track status
        if do_update_track_status:
            update_track_status(self.ebid, message=f"Ready for QA",
                                sheetname=self.sheetname,
                                status_col=1 if data_type == 'continuum' else 2)


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

        if not data_type in ['continuum', 'speclines']:
            raise ValueError(f"data_type must be 'continuum' or 'speclines'. Given {data_type}")

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

        # Copy to the same folder that job scripts are/will be in
        track_scripts_dir = scripts_dir / self.track_folder_name

        if not track_scripts_dir.exists():
            track_scripts_dir.mkdir()

        if filename is None:
            log.info(f"Unable to find a manual flagging sheet for {self.track_name}")
        else:

            newfilename = track_scripts_dir / f'manual_flagging_{data_type}.txt'

            task_command = ['cp', filename, newfilename]

            task_copy = subprocess.run(task_command, capture_output=True)

            cluster_key = 'cedar-robot-generic'
            log.info(f"Starting connection to {cluster_key}")

            connect = await self.setup_ssh_connection(cluster_key, **ssh_kwargs)
            log.info(f"Returned connection for {cluster_key}")

            result = connect.put(newfilename,
                                 remote=f"scratch/VLAXL_reduction/{self.track_folder_name}/")

            connect.close()
            del connect


    async def get_refantignore_files(self,
                                     clustername='cc-cedar',
                                     data_type='continuum',
                                     output_folder=os.path.expanduser('FlagRepository'),
                                     scripts_dir=Path('reduction_job_scripts/'),
                                     **ssh_kwargs,
                                     ):

        flag_repo_path = Path(output_folder) / self.project_code
        flag_repo_path.mkdir(parents=True, exist_ok=True)

        flag_repo_path_type = flag_repo_path / data_type
        flag_repo_path_type.mkdir(parents=True, exist_ok=True)

        log.info(f"Downloading refant ignore request for: {self.track_name}")

        # Also grab and copy over the refant file:
        refant_filename = download_refant_summsheet(self.ebid,
                                                    flag_repo_path_type,
                                                    data_type=data_type,
                                                    sheetname=self.sheetname)

        if refant_filename is None:
            log.info(f"Unable to find a refant ignore file for {self.track_name}")
        else:

            # Copy to the same folder that job scripts are/will be in
            track_scripts_dir = scripts_dir / self.track_folder_name

            if not track_scripts_dir.exists():
                track_scripts_dir.mkdir()

            newfilename = track_scripts_dir / f'refantignore_{data_type}.txt'

            task_command = ['cp', refant_filename, newfilename]

            task_copy = subprocess.run(task_command, capture_output=True)

            cluster_key = 'cedar-robot-generic'

            log.info(f"Starting connection to {cluster_key}")

            connect = await self.setup_ssh_connection(cluster_key, **ssh_kwargs)
            log.info(f"Returned connection for {cluster_key}")

            result = connect.put(newfilename,
                                remote=f"scratch/VLAXL_reduction/{self.track_folder_name}/")

            connect.close()
            del connect


    async def rerun_job_submission(self,
                                   clustername='cc-cedar',
                                   data_type='continuum',
                                   clusteracct=None,
                                   split_time=None,
                                   continuum_time=None,
                                   line_time=None,
                                   split_mem=None,
                                   continuum_mem=None,
                                   line_mem=None,
                                   scheduler_cmd='',
                                   reindex=False,
                                   casa_version=6.2,
                                   pipeline_branch='main'):

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

        # Download manual flagging files from the google sheet.
        await self.get_refantignore_files(clustername=clustername,
                                          data_type=data_type)

        await self.setup_for_reduction_pipeline(clustername=clustername,
                                                pipeline_branch=pipeline_branch)

        await self.initial_job_submission(clustername=clustername,
                                        scripts_dir=Path('reduction_job_scripts/'),
                                        split_type=data_type,
                                        reindex=reindex,
                                        submit_continuum_pipeline=True if data_type == 'continuum' else False,
                                        submit_line_pipeline=True if data_type == 'speclines' else False,
                                        clusteracct=clusteracct,
                                        split_time=split_time,
                                        continuum_time=continuum_time,
                                        line_time=line_time,
                                        split_mem=split_mem,
                                        continuum_mem=continuum_mem,
                                        line_mem=line_mem,
                                        scheduler_cmd=scheduler_cmd,
                                        casa_version=casa_version)

        update_track_status(self.ebid,
                            message=f"Reduction running on {clustername} after QA check",
                            sheetname=self.sheetname,
                            status_col=1 if data_type == 'continuum' else 2)

    async def cleanup_on_cluster(self, clustername='cc-cedar', data_type='continuum',
                                 do_remove_whole_track=False,
                                 do_only_remove_ms=False,
                                 do_cleanup_tempstorage=False,
                                 temp_project_dir="/project/rrg-eros-ab/ekoch/VLAXL/temp_calibrated/",
                                 **ssh_kwargs):
        '''
        Remove a previous run to setup for a new split and new pipeline reduction.

        NOTE: `do_remove_whole_track` deletes the whole track from scratch space!
        Use only when QA is finished.

        '''

        log.info(f"Starting connection to {clustername} for cleanup of {data_type}")

        cluster_key = 'cedar-robot-generic'

        connect = await self.setup_ssh_connection(cluster_key, **ssh_kwargs)
        log.info(f"Returned connection for {cluster_key}")

        if not do_remove_whole_track:
            # Change to the track directory, then delete the request data type folder
            # (continuum or speclines)

            path_to_track = f"scratch/VLAXL_reduction/{self.track_folder_name}/"

            log.info(f"Cleaning up {data_type} on {clustername} at {path_to_track}")

            # A successful job will end with a .ms.tar file.
            if do_only_remove_ms:
                rm_command = f"rm -rf {path_to_track}/{self.track_folder_name}_{data_type}/*.ms*.tar"
            else:
                rm_command = f"rm -rf {path_to_track}/{self.track_folder_name}_{data_type}"

        else:

            path_to_scratch = f'cd scratch/VLAXL_reduction/'

            log.info(f"Final clean up on {clustername} for track {path_to_scratch}")

            rm_command = f"rm -rf {path_to_scratch}/{self.track_folder_name}"

        result = run_command(connect, rm_command, allow_failure=True)

        log.info(f"Finished clean up on {clustername} with {rm_command}")

        if do_cleanup_tempstorage:
            log.info(f"Cleaning up temp project space on {clustername} for track {cd_command}")

            rm_command = f"rm -r {temp_project_dir}/{self.track_folder_name}.{data_type}.ms*.tar"

            log.info(f"Running command on {clustername}: {rm_command}")

            result = run_command(connect, rm_command, allow_failure=True)

            log.info(f"Finished temp project clean up on {clustername} for track {rm_command}")

        connect.close()
        del connect

    async def transfer_calibrated_data(self,
                                       clustername='cc-cedar',
                                       data_type='continuum',
                                       project_dir="/project/rrg-eros-ab/ekoch/VLAXL/temp_calibrated/"):
        """
        Move calibrated MSs to persistent storage location.
        """

        # Transfer the MS with globus to its place on project space.
        log.info(f"Transferring {self.track_folder_name} {data_type} calibrated MS to project space.")

        path_to_products = f'{self.track_folder_name}/{self.track_folder_name}_{data_type}/'

        filename = f'{path_to_products}/{self.track_folder_name}.{data_type}.ms.split.tar'

        # Going to the ingester instance. Doesn't need an extra path.
        output_destination = project_dir

        log.info(f"Filename to transfer is: {filename}")
        log.info(f"Transferring to: {output_destination}")

        transfer_taskid = transfer_general(filename, output_destination,
                                           startnode=clustername,
                                           endnode=clustername,
                                           wait_for_completion=False,
                                           skip_if_not_existing=True,
                                           remove_existing=False,
                                           use_startnode_datapath=True,
                                           use_endnode_datapath=False,
                                           use_rootname=True)

        if transfer_taskid is None:
            log.debug(f"No transfer task ID returned. Check existence of {filename}."
                  " Exiting completion process.")
            return

        self.transfer_taskid = transfer_taskid

        log.info(f"The globus transfer ID is: {transfer_taskid}")

        # Next transfer the split calibrator MS
        filename_cals = f'{path_to_products}/{self.track_folder_name}.{data_type}.ms.split_calibrators.tar'

        log.info(f"Filename to transfer is: {filename_cals}")
        log.info(f"Transferring to: {output_destination}")

        transfer_taskid_cals = transfer_general(filename_cals, output_destination,
                                                startnode=clustername,
                                                endnode=clustername,
                                                wait_for_completion=False,
                                                skip_if_not_existing=True,
                                                remove_existing=False,
                                                use_startnode_datapath=True,
                                                use_endnode_datapath=False,
                                                use_rootname=True)

        if transfer_taskid_cals is None:
            log.debug(f"No transfer task ID returned. Check existence of {filename_cals}."
                  " Exiting completion process.")
            return

        log.info(f"The globus transfer ID for cals is: {transfer_taskid_cals}")

        log.info(f"Waiting for globus transfer to {clustername} to complete.")
        await globus_wait_for_completion(transfer_taskid, sleeptime=180)
        log.info(f"Globus transfer {transfer_taskid} completed!")

        await globus_wait_for_completion(transfer_taskid_cals, sleeptime=180)
        log.info(f"Globus transfer {transfer_taskid_cals} completed!")

        log.info("Clean-up ms file on scratch")
        await self.cleanup_on_cluster(clustername=clustername, data_type=data_type,
                                      do_remove_whole_track=False,
                                      do_only_remove_ms=True,
                                      do_cleanup_tempstorage=False)

    async def export_track_for_imaging(self,
                                       clustername='cc-cedar',
                                       data_type='continuum',
                                       staging_dir="/project/rrg-eros-ab/ekoch/VLAXL/temp_calibrated/",
                                       project_dir="/project/rrg-eros-ab/ekoch/VLAXL/calibrated/",
                                       project_cals_dir="/project/rrg-eros-ab/ekoch/VLAXL/calibrated_calsonly/",
                                       ssh_kwargs={}):
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

        # These paths point to the track space on scratch.
        # path_to_products = f'{self.track_folder_name}/{self.track_folder_name}_{data_type}/'
        # filename = f'{path_to_products}/{self.track_folder_name}.{data_type}.ms.tar'

        # These paths point to MS location on project space.
        path_to_products = staging_dir
        filename = f'{path_to_products}/{self.track_folder_name}.{data_type}.ms.split.tar'

        # Going to the ingester instance. Doesn't need an extra path.
        output_destination = project_dir

        log.info(f"Filename to transfer is: {filename}")
        log.info(f"Transferring to: {output_destination}")

        transfer_taskid = transfer_general(filename, output_destination,
                                           startnode=clustername,
                                           endnode=clustername,
                                           wait_for_completion=False,
                                           skip_if_not_existing=True,
                                           use_startnode_datapath=False,
                                           use_endnode_datapath=False,
                                           use_rootname=True)

        if transfer_taskid is None:
            log.debug(f"No transfer task ID returned. Check existence of {filename}."
                    " Exiting completion process.")
            return

        self.transfer_taskid = transfer_taskid

        log.info(f"The globus transfer ID is: {transfer_taskid}")

        filename_cals = f'{path_to_products}/{self.track_folder_name}.{data_type}.ms.split_calibrators.tar'

        log.info(f"Filename to transfer is: {filename_cals}")
        log.info(f"Transferring to: {project_cals_dir}")

        transfer_taskid_cals = transfer_general(filename_cals, project_cals_dir,
                                                startnode=clustername,
                                                endnode=clustername,
                                                wait_for_completion=False,
                                                skip_if_not_existing=True,
                                                use_startnode_datapath=False,
                                                use_endnode_datapath=False,
                                                use_rootname=True)

        if transfer_taskid_cals is None:
            log.debug(f"No transfer task ID returned. Check existence of {filename_cals}."
                    " Exiting completion process.")
            return

        log.info(f"Waiting for globus transfer to {clustername} to complete.")
        await globus_wait_for_completion(transfer_taskid, sleeptime=180)
        log.info(f"Globus transfer {transfer_taskid} completed!")

        await globus_wait_for_completion(transfer_taskid_cals, sleeptime=180)
        log.info(f"Globus transfer {transfer_taskid_cals} completed!")

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
                                      do_remove_whole_track=do_remove_whole_track,
                                      do_cleanup_tempstorage=True,
                                      temp_project_dir=staging_dir)

        # Clean up temp ms.tar file on project space.
        log.info(f"Starting connection to {clustername} for cleanup of {data_type}")

        cluster_key = 'cedar-robot-generic'
        connect = await self.setup_ssh_connection(cluster_key, **ssh_kwargs)
        log.info(f"Returned connection for {cluster_key}")

        # NOTE: Keep the original delete here for now, since failures are allowed.
        # Remove after transition to the split data products.
        for product_name in [f"{self.track_folder_name}.ms.tar",
                             f"{self.track_folder_name}.ms.split.tar",
                             f"{self.track_folder_name}.ms.split_calibrators.tar"]:

            rm_command = f"rm -rf {staging_dir}/{product_name}"

            result = run_command(connect, rm_command, allow_failure=True)
            log.info(f"Finished cleaning up temp ms file up on {clustername} with: {rm_command}")

        connect.close()
        del connect

        # Last, make sure we have cleaned up the SDM on AOC:
        cleanup_source(self.track_name, node='nrao-aoc')

        # Remove completion flag to avoid re-runs
        update_cell(self.ebid, "",
                    # num_col=28 if data_type == 'continuum' else 29,
                    name_col=f"Re-run\n{data_type}",
                    sheetname=self.sheetname)


    def label_qa_failures(self, data_type='continuum'):

        """
        Note failing tracks or those that require manual reduction attempts.
        """

        self._grab_sheetdata()
        if self.target is None or self.track_name is None:
            raise ValueError(f"Cannot find target or trackname in {self.ebid}")

        manual_review_states = ["MANUAL REVIEW", "HELP REQUESTED"]

        status_flag = self._qa_review_input(data_type=data_type)

        if status_flag not in manual_review_states:
            log.debug("No manual review requested. Exiting")
            return

        if status_flag == manual_review_states[0]:
            message=f"FAILED QA: Requires manual review."
        else:
            message=f"HELP: QA help requested."

        update_track_status(self.ebid,
                        message=message,
                        sheetname=self.sheetname,
                        status_col=1 if data_type == 'continuum' else 2)

        # Remove review flag to avoid re-runs
        update_cell(self.ebid, "",
                    # num_col=28 if data_type == 'continuum' else 29,
                    name_col=f"Re-run\n{data_type}",
                    sheetname=self.sheetname)


    async def transfer_qa_failures(self, data_type='continuum',
                                   startnode='cc-cedar',
                                   endnode='ingester',
                                   set_status=True):

        self._grab_sheetdata()
        if self.target is None or self.track_name is None:
            raise ValueError(f"Cannot find target or trackname in {self.ebid}")

        if set_status:

            status_col = 1 if data_type == 'continuum' else 2

            update_track_status(self.ebid,
                                message=f"ISSUE: Needs manual check of job status",
                                sheetname=self.sheetname,
                                status_col=status_col)

        # Attempt to transfer failed data products

        log.info(f"Transferring {self.track_folder_name} {data_type} products from {startnode} to {endnode}.")

        path_to_products = f'{self.track_folder_name}/{self.track_folder_name}_{data_type}/'

        filename = f'{path_to_products}/{self.track_folder_name}_{data_type}_products_failure.tar'

        # Going to the ingester instance. Doesn't need an extra path.
        output_destination = "pipeline_failures/"

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

        # TODO: link this into the webserver to easily view the weblog for failures

