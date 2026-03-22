
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


from autodataingest.gsheet_tracker.gsheet_functions import (find_new_tracks, update_track_status,
                                             update_cell, return_cell, download_refant_summsheet)

from autodataingest.gsheet_tracker.gsheet_flagging import (download_flagsheet_to_flagtxt,
                                                           check_imaging_trigger)

from autodataingest.globus_functions import (transfer_file, transfer_pipeline,
                               cleanup_source, globus_wait_for_completion,
                               transfer_general)

from autodataingest.download_vlaant_corrections import download_vla_antcorr

from autodataingest.ssh_utils import (run_command,
                                      time_limit,
                                      run_job_submission,
                                      setup_ssh_connection)

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

        self._restart_pipeline_count = 0

        # Initialize sheet-derived attributes with defaults.
        # Call await self._grab_sheetdata() in the first async method of the pipeline.
        self.targets = []
        self.target = None
        self.config = None
        self.track_name = None
        self.incl_rrl = False

        # Flow-control flags set by the producer in main_restarts.py.
        # Default True so the consumer processes both paths unless explicitly disabled.
        self._allow_continuum_run = True
        self._allow_speclines_run = True

    async def _grab_sheetdata(self):
        '''
        Get info from the google sheet. This is needed to allow for restarting at
        different stages.
        '''

        targets_raw = await return_cell(self.ebid,
                                  name_col="Targets",
                                  sheetname=self.sheetname)
        config = await return_cell(self.ebid,
                             name_col="Configuration",
                             sheetname=self.sheetname)
        track_name = await return_cell(self.ebid,
                                 name_col='Trackname',
                                 sheetname=self.sheetname)

        incl_rrl = await return_cell(self.ebid,
                               name_col='INCL_RRLS',
                               sheetname=self.sheetname)

        if targets_raw not in (None, "None", ""):
            # Targets is a comma-separated list; keep self.target as the first
            # for any downstream code that expects a single string
            self.targets = [t.strip() for t in targets_raw.split(',')]
            self.target = self.targets[0]
        else:
            self.targets = []
            self.target = None

        if config not in (None, "None", ""):
            self.config = config
        else:
            self.config = None

        if track_name not in (None, "None", ""):
            self.track_name = track_name
        else:
            self.track_name = None

        if incl_rrl == "TRUE":
            self.incl_rrl = True
        else:
            self.incl_rrl = False

    async def _qa_review_input(self):
        '''
        Read the re-run trigger cell for the pipeline job.
        '''

        return await return_cell(self.ebid, name_col="Re-run pipeline", sheetname=self.sheetname)

    @property
    def track_folder_name(self):
        return f"{self.config}_{self.track_name}"

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

        await self._grab_sheetdata()

        log.info(f"Adding start status for {ebid}.")

        await update_track_status(ebid, message="Queued",
                                  sheetname=self.sheetname,
                                  status_col=1)

    async def set_qa_queued_status(self, data_type='continuum'):
        '''
        Set a status to stop new tracks being re-added to the new track queue.
        '''
        ebid = self.ebid

        log.info(f"Adding start status for {ebid}.")

        await update_track_status(ebid, message="Queued for QA/product transfer",
                                  sheetname=self.sheetname,
                                  status_col=1)

    async def archive_request_and_transfer(self,
                                           do_archiverequest=False,
                                           archive_kwargs={},
                                           timewindow=48 * 3600.,
                                           sleeptime=600,
                                           clustername='dra-fir',
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

        # This should now be wrapped into a prior check on the file existence.
        assert self.track_name is not None


        # Update track name in sheet:
        await update_cell(ebid, self.track_name,
                          # num_col=3,
                          name_col="Trackname",
                          sheetname=self.sheetname)

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
        self.config = await return_cell(self.ebid,
                                        #  column=9,
                                        name_col="Configuration",
                                        sheetname=self.sheetname)

        log.info(f"This track was taken in {config} configuration.")
        log.info(f"This track can be found in the folder with name {self.track_folder_name}")

        # Do globus transfer:

        log.info(f"Transferring {self.track_folder_name} to {clustername}.")
        transfer_taskid = transfer_file(self.track_name, self.track_folder_name,
                                        # startnode='nrao-aoc',  # As of 06/2025, AOC does not have a globus node.
                                        startnode='dra-fir', startnode_key='data_path_staging',
                                        endnode=clustername,
                                        wait_for_completion=False)

        self.transfer_taskid = transfer_taskid

        log.info(f"The globus transfer ID is: {transfer_taskid}")

        await update_track_status(ebid,
                                  message=f"Data transferred to {clustername}",
                                  sheetname=self.sheetname,
                                  status_col=1)

        log.info(f"Waiting for globus transfer to {clustername} to complete.")
        await globus_wait_for_completion(transfer_taskid)
        log.info(f"Globus transfer {transfer_taskid} completed!")

        await update_cell(ebid, "TRUE",
                          # num_col=18,
                          name_col='Transferred data',
                          sheetname=self.sheetname)

        # Remove the data staged at NRAO to avoid exceeding our storage quota
        if do_cleanup:
            # start_node = 'nrao-aoc'
            # start_node_key = 'data_path_staging'  # on nrao-aoc
            start_node = 'dra-fir'
            start_node_key = 'data_path_staging'  # on dra-fir
            log.info(f"Cleaning up {ebid} on {start_node}")
            cleanup_source(self.track_name,
                           node=start_node,
                           node_key=start_node_key)


    async def setup_for_reduction_pipeline(self,
                                           clustername='dra-fir',
                                           clone_new_pipeline_repo=False,
                                           pipeline_branch='main',
                                           **ssh_kwargs):

        """
        Step 2.

        Create products and setup on the cluster running the reduction.

        1. Tests connection to cluster.
        2. Clones the ReductionPipeline repo
        TODO: Allow setting a version for the pipeline repo.
        3. Updates + transfers offline copies of the antenna positions corrections.
        """

        log.info(f"Setting up pipeline on {clustername} for EBID {self.ebid}")

        # Before running any reduction, update the antenna correction files
        # and copy that folder to each folder where the pipeline is run
        log.info("Downloading updates of antenna corrections to 'VLA_antcorr_tables'")
        download_vla_antcorr(data_folder="VLA_antcorr_tables")

        ssh_retry_times = 0

        while True:
            try:
                with time_limit(self._ssh_max_connect_time):

                    # Grab the repo; this is where we can also specify a version number, too
                    cd_location = f'{ENDPOINT_INFO[clustername]["data_path"]}/{self.track_folder_name}/'

                    if clone_new_pipeline_repo:
                        cluster_key = "cedar-robot-jobsetup"
                        log.info(f"Starting connection to {cluster_key} on try {ssh_retry_times}")

                        connect = await self.setup_ssh_connection(cluster_key,
                                                                **ssh_kwargs)

                        log.info(f"Returned connection for {cluster_key}")


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
                                    clustername='dra-fir',
                                    scripts_dir=Path('reduction_job_scripts/'),
                                    casa_version="6.6",
                                    pipeline_time=None,
                                    pipeline_mem=None,
                                    scheduler_cmd="",
                                    **ssh_kwargs):
        """
        Step 3.

        Submit the unified VLA calibration pipeline job.  The new pipeline
        (lband_reduction_pipeline.py, CASA 6.6+) handles import and
        calibration of all data types in a single pass — no separate
        import/split step is required.
        """

        cluster_key = "cedar-robot-generic"

        log.info(f"Starting job submission of {self.ebid} on {cluster_key}.")

        # Create local folder for job submission scripts prior to transfer
        track_scripts_dir = scripts_dir / self.track_folder_name

        if not track_scripts_dir.exists():
            track_scripts_dir.mkdir()

        # Setup connection:
        log.info(f"Starting connection to {cluster_key}")

        connect = await self.setup_ssh_connection(cluster_key, **ssh_kwargs)
        log.info(f"Returned connection for {cluster_key}")

        job_pipeline_filename = f"{self.track_folder_name}_job_pipeline.sh"

        if (track_scripts_dir / job_pipeline_filename).exists():
            (track_scripts_dir / job_pipeline_filename).unlink()

        slurm_pipeline_kwargs = {}
        if pipeline_time is not None:
            slurm_pipeline_kwargs['job_time'] = pipeline_time
        if pipeline_mem is not None:
            slurm_pipeline_kwargs['mem'] = pipeline_mem

        log.info(f"Making unified pipeline job file for {self.ebid}")

        print(JOB_CREATION_FUNCTIONS[clustername]['UNIFIED_PIPE'](
                config=self.config,
                trackname=self.track_name,
                slurm_kwargs=slurm_pipeline_kwargs,
                setup_kwargs={},
                casa_version=casa_version),
            file=open(track_scripts_dir / job_pipeline_filename, 'a'))

        # Move the job script to the cluster:
        log.info(f"Moving pipeline job file for {self.ebid} to {cluster_key}")
        result = connect.put(track_scripts_dir / job_pipeline_filename,
                             remote=f'{ENDPOINT_INFO[clustername]["data_path"]}/{self.track_folder_name}/')

        cluster_key_submit = 'cedar-submitter'
        log.info(f"Starting connection to {cluster_key_submit}")

        connect_submit = await self.setup_ssh_connection(cluster_key_submit, **ssh_kwargs)
        log.info(f"Returned connection for {cluster_key_submit}")

        chdir_cmd = f'{ENDPOINT_INFO[clustername]["data_path"]}/{self.track_folder_name}/'

        log.info(f"Submitting job file: {job_pipeline_filename}")

        try:
            result = connect_submit.run(f"{chdir_cmd} {job_pipeline_filename}")
            pipeline_jobid = result.stdout.replace("\n", '').split(" ")[-1]
        except ValueError as exc:
            pipeline_jobid = None

            connect_submit.close()
            del connect_submit

            raise ValueError(f"Failed to submit unified pipeline job! See stderr: {exc}")

        self.pipeline_jobid = pipeline_jobid

        log.info(f"Submitted pipeline job for {self.ebid} on {clustername} as job {self.pipeline_jobid}")

        await update_cell(self.ebid, f"{clustername}:{self.pipeline_jobid}",
                          name_col="Pipeline job ID",
                          sheetname=self.sheetname)

        await update_track_status(self.ebid,
                                  message=f"Reduction running on {clustername}",
                                  sheetname=self.sheetname,
                                  status_col=1)

        if connect.is_connected:
            connect.close()
            del connect

        if connect_submit.is_connected:
            connect_submit.close()
            del connect_submit

        log.info(f"Finished submitting pipeline for {self.ebid} on {clustername}")

    async def set_job_status(self, job_status):
        """
        Update the sheet status based on the SLURM job outcome.

        Parameters:
            job_status (str): SLURM job state string.
        """

        if job_status == 'TIMEOUT':
            await update_track_status(self.ebid,
                                      message=f"ISSUE: job timed out",
                                      sheetname=self.sheetname,
                                      status_col=1)

        elif job_status in ["FAILED", "OUT_OF_MEMORY", "CANCELLED", "NODE_FAIL"]:
            await update_track_status(self.ebid,
                                      message=f"ISSUE: Failure with state {job_status}",
                                      sheetname=self.sheetname,
                                      status_col=1)

        elif job_status == "COMPLETED":
            await update_track_status(self.ebid, message=f"Ready for QA",
                                      sheetname=self.sheetname,
                                      status_col=1)

    async def set_job_stats(self, job_id, job_type):

        job_check = check_for_job_notification(job_id)

        if job_check is None:
            log.info(f"Unable to find notification for job ID: {job_id}")
            return

        job_status, job_runtime =  job_check

        log.info(f"Found notification for {job_id}:{job_type} with status {job_status}")

        if job_type == "pipeline":
            await update_cell(self.ebid, job_status,
                              name_col='Pipeline reduction',
                              sheetname=self.sheetname)
            await update_cell(self.ebid, job_runtime,
                              name_col="Pipeline job wall time",
                              sheetname=self.sheetname)
        else:
            log.error(f"Unable to interpret job_type {job_type}")


    async def transfer_pipeline_products(self,
                                         startnode='dra-fir',
                                         endnode='ingester'):
        """
        Step 5.

        Transfer pipeline outputs to a storage system the webserver can access to host.
        """

        await self._grab_sheetdata()

        if self.track_name is None:
            raise ValueError(f"Cannot find trackname in {self.ebid}")

        log.info(f"Transferring {self.track_folder_name} products from {startnode} to {endnode}.")

        filename = f'{self.track_folder_name}/{self.track_folder_name}_pipeline_products.tar'

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

    async def make_flagging_sheet(self, template_name='TEMPLATE'):
        '''
        Create the flagging sheet (one per track) and store its URL.
        '''

        from autodataingest.gsheet_tracker.gsheet_flagging import make_new_flagsheet

        new_flagsheet = await make_new_flagsheet(self.track_name, self.config,
                                                 template_name=template_name)

        prefix = "https://docs.google.com/spreadsheets/d/"
        this_sheet_url = new_flagsheet.url.split("spreadsheets/")[1]
        self._flagsheet_url = f"{prefix}/{this_sheet_url}"

    @property
    def flagsheet_url(self):
        return self._flagsheet_url

    async def make_qa_products(self,
                               verbose=False,
                               do_update_track_status=True):
        '''
        Create the QA products for the QA webserver.
        '''

        await self._grab_sheetdata()

        if self.track_name is None:
            raise ValueError(f"Cannot find trackname in {self.ebid}")

        data_path = Path(ENDPOINT_INFO['ingester']['data_path'])

        self.setup_qa_track_path()
        qa_path = self.qa_track_path / 'pipeline'

        product_tarname = f"{self.track_folder_name}_pipeline_products.tar"
        product_file = data_path / product_tarname

        if not os.path.exists(product_file):
            log.warning(f"Unable to find products file at {product_file}")
            return False

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
        flagging_sheet_link = self.flagsheet_url

        kwarg_strs = f"flagging_sheet_link='{flagging_sheet_link}', show_target_linesonly=True"

        task_command = ['ipython', '-c',
                        f'"import qaplotter; qaplotter.make_all_plots({kwarg_strs})"']

        log.info(f"Running qaplotting.")
        log.info(" ".join(task_command))

        task_qaplot_make = subprocess.run(task_command, capture_output=True)

        if task_qaplot_make.returncode != 0:
            log.info(task_qaplot_make.stdout)
            log.info("qaplotter failure. Check products.")

            if do_update_track_status:
                await update_track_status(self.ebid, message="ISSUE: qaplotter failure",
                                          sheetname=self.sheetname,
                                          status_col=1)


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
            await update_track_status(self.ebid, message=f"Ready for QA",
                                      sheetname=self.sheetname,
                                      status_col=1)

        return True


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
        (self.qa_track_path / 'pipeline').mkdir(parents=True, exist_ok=True)

    async def get_flagging_files(self,
                                 clustername='dra-fir',
                                 output_folder=os.path.expanduser('FlagRepository'),
                                 scripts_dir=Path('reduction_job_scripts/'),
                                 **ssh_kwargs,
                                 ):

        flag_repo_path = Path(output_folder) / self.project_code
        flag_repo_path.mkdir(parents=True, exist_ok=True)

        filename = await download_flagsheet_to_flagtxt(self.track_name,
                                                       self.config,
                                                       flag_repo_path,
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

            newfilename = track_scripts_dir / 'manual_flagging.txt'

            task_command = ['cp', filename, newfilename]

            task_copy = subprocess.run(task_command, capture_output=True)

            cluster_key = 'cedar-robot-generic'
            log.info(f"Starting connection to {cluster_key}")

            connect = await self.setup_ssh_connection(cluster_key, **ssh_kwargs)
            log.info(f"Returned connection for {cluster_key}")

            result = connect.put(newfilename,
                                 remote=f'{ENDPOINT_INFO[clustername]["data_path"]}/{self.track_folder_name}/')

            connect.close()
            del connect


    async def get_refantignore_files(self,
                                     clustername='dra-fir',
                                     output_folder=os.path.expanduser('FlagRepository'),
                                     scripts_dir=Path('reduction_job_scripts/'),
                                     **ssh_kwargs,
                                     ):

        flag_repo_path = Path(output_folder) / self.project_code
        flag_repo_path.mkdir(parents=True, exist_ok=True)

        log.info(f"Downloading refant ignore request for: {self.track_name}")

        refant_filename = await download_refant_summsheet(self.ebid,
                                                          flag_repo_path,
                                                          sheetname=self.sheetname)

        if refant_filename is None:
            log.info(f"Unable to find a refant ignore file for {self.track_name}")
        else:

            # Copy to the same folder that job scripts are/will be in
            track_scripts_dir = scripts_dir / self.track_folder_name

            if not track_scripts_dir.exists():
                track_scripts_dir.mkdir()

            newfilename = track_scripts_dir / 'refant_ignore.txt'

            task_command = ['cp', refant_filename, newfilename]

            task_copy = subprocess.run(task_command, capture_output=True)

            cluster_key = 'cedar-robot-generic'

            log.info(f"Starting connection to {cluster_key}")

            connect = await self.setup_ssh_connection(cluster_key, **ssh_kwargs)
            log.info(f"Returned connection for {cluster_key}")

            result = connect.put(newfilename,
                                remote=f'{ENDPOINT_INFO[clustername]["data_path"]}/{self.track_folder_name}/')

            connect.close()
            del connect


    async def rerun_job_submission(self,
                                   clustername='dra-fir',
                                   pipeline_time=None,
                                   pipeline_mem=None,
                                   scheduler_cmd='',
                                   casa_version='6.6',
                                   pipeline_branch='main'):

        """
        Step 7.

        After QA, supplies an updated flagging file and re-submits the unified
        pipeline.
        """

        status_flag = await self._qa_review_input()

        if status_flag != "RESTART":
            log.debug("No restart requested. Exiting")
            return

        await update_track_status(self.ebid, message=f"Restarting pipeline for re-run",
                                  sheetname=self.sheetname,
                                  status_col=1)

        # Clear the re-run trigger to avoid repeated restarts
        await update_cell(self.ebid, "",
                          name_col="Re-run pipeline",
                          sheetname=self.sheetname)

        await self.cleanup_on_cluster(clustername=clustername)

        await self.get_flagging_files(clustername=clustername)

        await self.get_refantignore_files(clustername=clustername)

        await self.setup_for_reduction_pipeline(clustername=clustername,
                                                pipeline_branch=pipeline_branch)

        await self.initial_job_submission(clustername=clustername,
                                          scripts_dir=Path('reduction_job_scripts/'),
                                          pipeline_time=pipeline_time,
                                          pipeline_mem=pipeline_mem,
                                          scheduler_cmd=scheduler_cmd,
                                          casa_version=casa_version)

        await update_track_status(self.ebid,
                                  message=f"Reduction running on {clustername} after QA check",
                                  sheetname=self.sheetname,
                                  status_col=1)

    async def cleanup_on_cluster(self, clustername='dra-fir',
                                 do_remove_whole_track=False,
                                 do_only_remove_products=False,
                                 **ssh_kwargs):
        '''
        Remove pipeline outputs to prepare for a re-run.

        NOTE: `do_remove_whole_track` deletes the entire track folder from
        scratch space. Use only when QA is fully finished.
        '''

        log.info(f"Starting connection to {clustername} for cleanup of {self.track_folder_name}")

        cluster_key = 'cedar-robot-generic'

        connect = await self.setup_ssh_connection(cluster_key, **ssh_kwargs)
        log.info(f"Returned connection for {cluster_key}")

        path_to_track = f'{ENDPOINT_INFO[clustername]["data_path"]}/{self.track_folder_name}'

        if do_remove_whole_track:
            log.info(f"Removing entire track folder on {clustername}: {path_to_track}")
            rm_command = f"rm -rf {path_to_track}"
        elif do_only_remove_products:
            # Remove only the pipeline products folder and tar, leaving the SDM intact
            rm_command = (f"rm -rf {path_to_track}/products "
                          f"{path_to_track}/{self.track_folder_name}_pipeline_products*.tar")
        else:
            # Default rerun cleanup: remove products and CASA temp files but keep the SDM
            rm_command = (f"rm -rf {path_to_track}/products "
                          f"{path_to_track}/{self.track_folder_name}_pipeline_products*.tar "
                          f"{path_to_track}/*.last {path_to_track}/*.log")

        result = run_command(connect, rm_command, allow_failure=True)

        log.info(f"Finished clean up on {clustername}")

        connect.close()
        del connect

    async def submit_imaging_jobs(self,
                                  clustername='dra-fir',
                                  scripts_dir=Path('reduction_job_scripts/'),
                                  casa_version='6.6',
                                  **ssh_kwargs):
        """
        Optionally submit imaging jobs after pipeline QA is approved.

        Reads the imaging trigger flags from the flagging sheet and submits
        continuum and/or speclines imaging jobs if requested.

        NOTE: Imaging job templates are not yet defined. This method will raise
        NotImplementedError until job_imaging.py is implemented.
        """

        triggers = await check_imaging_trigger(self.track_name, self.config)

        if not triggers['continuum'] and not triggers['speclines']:
            log.info(f"No imaging jobs triggered for {self.track_folder_name}. Skipping.")
            return

        raise NotImplementedError(
            "Imaging job submission is not yet implemented. "
            "Define job_imaging.py and update this method."
        )

    async def transfer_calibrated_data(self,
                                       clustername='dra-fir',
                                       project_dir="/project/rrg-eros-ab/ekoch/VLAXL/temp_calibrated/"):
        """
        Move calibrated MSs to persistent storage location.
        """

        # Transfer the MS with globus to its place on project space.
        # TODO: update filename pattern once the new pipeline's MS output naming is confirmed.
        log.info(f"Transferring {self.track_folder_name} calibrated MS to project space.")

        filename = f'{self.track_folder_name}/{self.track_folder_name}.ms.split.tar'

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
            log.info(f"No transfer task ID returned. Check existence of {filename}."
                  " Exiting completion process.")

            await update_track_status(self.ebid,
                                      message=f"ISSUE: globus transfer failed.",
                                      sheetname=self.sheetname,
                                      status_col=1 if data_type == 'continuum' else 2)

            raise ValueError(f"No transfer task ID returned. Check existence of {filename}."
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

            await update_track_status(self.ebid,
                                      message=f"ISSUE: globus transfer failed.",
                                      sheetname=self.sheetname,
                                      status_col=1 if data_type == 'continuum' else 2)

            raise ValueError(f"No transfer task ID returned. Check existence of {filename_cals}."
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
                                       clustername='dra-fir',
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

        status_flag = await self._qa_review_input()

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
        current_status = await return_cell(self.ebid,
                                           #  column=1,
                                           name_col=f'Status: {other_data_type}',
                                           sheetname=self.sheetname)

        if "Ready for imaging" in current_status:
            other_part_finished = True
        else:
            other_part_finished = False

        await update_track_status(self.ebid,
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
        # cleanup_source(self.track_name, node='nrao-aoc')

        # Remove completion flag to avoid re-runs
        await update_cell(self.ebid, "",
                          # num_col=28 if data_type == 'continuum' else 29,
                          name_col=f"Re-run\n{data_type}",
                          sheetname=self.sheetname)


    async def label_qa_failures(self, data_type='continuum'):

        """
        Note failing tracks or those that require manual reduction attempts.
        """

        await self._grab_sheetdata()
        if self.target is None or self.track_name is None:
            raise ValueError(f"Cannot find target or trackname in {self.ebid}")

        manual_review_states = ["MANUAL REVIEW", "HELP REQUESTED"]

        status_flag = await self._qa_review_input()

        if status_flag not in manual_review_states:
            log.debug("No manual review requested. Exiting")
            return

        if status_flag == manual_review_states[0]:
            message=f"FAILED QA: Requires manual review."
        else:
            message=f"HELP: QA help requested."

        await update_track_status(self.ebid,
                                  message=message,
                                  sheetname=self.sheetname,
                                  status_col=1 if data_type == 'continuum' else 2)

        # Remove review flag to avoid re-runs
        await update_cell(self.ebid, "",
                          # num_col=28 if data_type == 'continuum' else 29,
                          name_col=f"Re-run\n{data_type}",
                          sheetname=self.sheetname)


    async def transfer_qa_failures(self, data_type='continuum',
                                   startnode='dra-fir',
                                   endnode='ingester',
                                   set_status=True):

        await self._grab_sheetdata()
        if self.target is None or self.track_name is None:
            raise ValueError(f"Cannot find target or trackname in {self.ebid}")

        if set_status:

            status_col = 1 if data_type == 'continuum' else 2

            await update_track_status(self.ebid,
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

