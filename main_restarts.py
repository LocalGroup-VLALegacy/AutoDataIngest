
'''
This script triggers re-running the pipeline after QA, and triggers the QA completion sequence.

Run as python main.py from command line.

REQUIRE python>=3.7 for asyncio.

'''

import asyncio
import time
from pathlib import Path
import astropy.units as u

from autodataingest.gsheet_tracker.gsheet_functions import (find_rerun_status_tracks)

from autodataingest.ingest_pipeline_functions import AutoPipeline

from autodataingest.ssh_utils import setup_ssh_connection

from autodataingest.job_monitor import (number_of_active_jobs,
                                        get_lustre_storage_avail,
                                        get_slurm_job_monitor)

from autodataingest.logging import setup_logging
log = setup_logging()


def allow_newjobs_check(free_space, free_filenum, num_jobs_active):
    return (free_space >= MIN_STORAGE) & (free_filenum >= MIN_NUMFILES) & \
        (num_jobs_active < MAX_NUMJOBS)


async def produce(queue, sleeptime=120, start_with_newest=False,
                  long_sleep=7200,
                  sheetnames=['20A - OpLog Summary']):
    '''
    Check for new tracks from the google sheet.
    '''

    while True:

        # Pass through completions/failures first since they don't require reprocessing
        all_complete_statuses = []

        for sheetname in sheetnames:
            try:
                sheet_all_complete_statuses = find_rerun_status_tracks(sheetname=sheetname,
                                                                       job_type="COMPLETE")

                # Append the failure cases:
                for fail_state in ["MANUAL REVIEW", "HELP REQUESTED"]:
                    fail_status = find_rerun_status_tracks(sheetname=sheetname, job_type=fail_state)
                    sheet_all_complete_statuses.extend(fail_status)

            except Exception as e:
                log.warn(f"Encountered error in find_reruns_status_tracks: {e}")
                await asyncio.sleep(long_sleep)
                continue

            for this_status in sheet_all_complete_statuses:
                all_complete_statuses.append([this_status, sheetname])

            await asyncio.sleep(120)


        for rerun_stat, this_sheetname in all_complete_statuses:

            ebid, run_types = rerun_stat

            if ebid in EBID_QUEUE_LIST:
                log.info(f'Skipping new track with ID {ebid} because it is still in the queue.')
                continue

            EBID_QUEUE_LIST.append(ebid)

            for this_run_type in run_types:
                this_data_type, this_job_type = this_run_type
                log.info(f'Found new track with ID {ebid} {this_data_type} {this_job_type}')

            this_pipe = AutoPipeline(ebid, sheetname=this_sheetname)

            # Stop other jobs from running (i.e. disable restarting one part until
            # the completion finishes first).

            # Both are completions, run on through
            if len(run_types) == 2:
                await queue.put(this_pipe)
                continue

            # Set the opposite part of the completion to just not run to avoid restarts
            this_data_type, this_job_type = run_types[0]

            if this_data_type == 'continuum':
                this_pipe._allow_speclines_run = False
            else:
                this_pipe._allow_continuum_run = False

            # put the item in the queue
            await queue.put(this_pipe)

            await asyncio.sleep(sleeptime)

        # Long wait to avoid overlaps
        if len(all_complete_statuses) > 0:
            await asyncio.sleep(long_sleep)

        # NOW go through the process of finding new restart jobs and checking if they can run
        allow_newjobs = False

        # Check the number of active jobs and storage usage.
        # Skip new runs until there are fewer jobs or more storage space.
        try:
            connect = setup_ssh_connection(CLUSTERNAME)

            df = get_slurm_job_monitor(connect)

            num_jobs_active = number_of_active_jobs(df)

            # Get storage space usage
            free_space, free_filenum = get_lustre_storage_avail(connect, username=uname,
                                                                diskname='/scratch')

            connect.close()

            log.info(f"Free storage: {free_space} Free file num: {free_filenum}")
            log.info(f"Jobs running on {CLUSTERNAME}: {num_jobs_active}")
            allow_newjobs = allow_newjobs_check(free_space, free_filenum, num_jobs_active)

            if not allow_newjobs:
                log.info("At job/storage limit. Will wait before starting new jobs.")

        except Exception as err:
            log.error(f"Encountered an error checking job/storage usage on {CLUSTERNAME}")
            log.error(f"Error is: {err}")

            await asyncio.sleep(long_sleep)
            continue


        await asyncio.sleep(sleeptime)
        # Gather all rerun jobs statuses from the google sheet.
        # If we get an API error for too many requests, just wait a bit and try again:
        all_rerun_statuses = []

        for sheetname in sheetnames:

            log.info(f"Job checking in sheet {sheetname}")

            try:
                sheet_all_rerun_statuses = find_rerun_status_tracks(sheetname=sheetname,
                                                                    job_type="RESTART")
            except Exception as e:
                log.warn(f"Encountered error in find_reruns_status_tracks: {e}")
                await asyncio.sleep(long_sleep)
                continue

            await asyncio.sleep(sleeptime)

            for this_status in sheet_all_rerun_statuses:
                all_rerun_statuses.append([this_status, sheetname])

        if start_with_newest:
            all_rerun_statuses = all_rerun_statuses[::-1]

        # Queue new jobs to run.
        for rerun_stat, this_sheetname in all_rerun_statuses:

            ebid, run_types = rerun_stat

            if ebid in EBID_QUEUE_LIST:
                log.info(f'Skipping new track with ID {ebid} because it is still in the queue.')
                continue

            EBID_QUEUE_LIST.append(ebid)

            this_pipe = AutoPipeline(ebid, sheetname=this_sheetname)

            # Disable new runs for restarts when allow_newjobs = False from above.
            for this_run_type in run_types:
                this_data_type, this_job_type = this_run_type

                log.info(f'Found new track with ID {ebid} {this_data_type} {this_job_type}')

                # Block new restarts if not allowing new jobs yet
                if not allow_newjobs and this_job_type == "RESTART":
                    log.info(f"At job/storage limit. Will wait before starting new job.")

                    if this_data_type == 'continuum':
                        this_pipe._allow_continuum_run = False
                    else:
                        this_pipe._allow_speclines_run = False

            # Update number of active jobs manually.
            num_jobs_active += len(run_types)

            # put the item in the queue
            await queue.put(this_pipe)

            # Put a small gap between starting to consume processes
            await asyncio.sleep(sleeptime)

            # If we've exceeded our allow jobs limit, break this loop.
            if not allow_newjobs_check(free_space, free_filenum, num_jobs_active):
                log.info("Queue in the spreadsheet exceeded our job limits."
                         " Holding further job starts.")
                break

        if test_case_run_newest:
            break

        await asyncio.sleep(long_sleep)


async def consume(queue, sleeptime=1800, sleeptime_finish=600):
    while True:

        # wait for an item from the producer
        auto_pipe = await queue.get()

        EBID_QUEUE_LIST.remove(auto_pipe.ebid)

        # process the item
        log.info('Processing {}...'.format(auto_pipe.ebid))
        # simulate i/o operation using sleep
        # await asyncio.sleep(1)

        if auto_pipe._allow_continuum_run:
            continuum_status = auto_pipe._qa_review_input(data_type='continuum')
        else:
            continuum_status = ""

        if auto_pipe._allow_speclines_run:
            speclines_status = auto_pipe._qa_review_input(data_type='speclines')
        else:
            speclines_status = ""

        log.info(f"{auto_pipe.ebid} Found continuum status: {continuum_status}")
        log.info(f"{auto_pipe.ebid} Found speclines status: {speclines_status}")

        # Check for completions:
        complete_continuum = continuum_status == "COMPLETE"
        complete_speclines = speclines_status == "COMPLETE"

        if complete_continuum or complete_speclines:
            log.info(f"{auto_pipe.ebid} Found a completion job")

            data_types = []
            if complete_continuum:
                data_types.append('continuum')
            if complete_speclines:
                data_types.append('speclines')

            for data_type in data_types:
                await auto_pipe.export_track_for_imaging(data_type=data_type,
                                                        clustername=CLUSTERNAME,
                                                        project_dir=COMPLETEDDATAPATH)

                await asyncio.sleep(sleeptime_finish)

        # Check for QA failures needing a full manual reduction/review, or help requested:
        manual_review_states = ["MANUAL REVIEW", "HELP REQUESTED"]
        manualcheck_continuum = continuum_status in manual_review_states
        manualcheck_speclines = speclines_status in manual_review_states

        if manualcheck_continuum or manualcheck_speclines:
            log.info(f"{auto_pipe.ebid} Found a manual review job")

            data_types = []
            if manualcheck_continuum:
                data_types.append('continuum')
            if manualcheck_speclines:
                data_types.append('speclines')

            for data_type in data_types:
                await auto_pipe.label_qa_failures(data_type=data_type)

        # Restarts

        restart_continuum = continuum_status == "RESTART"
        restart_speclines = speclines_status == "RESTART"

        if restart_continuum or restart_speclines:

            log.info(f"{auto_pipe.ebid} Found a restart job")

            data_types = []
            if restart_continuum:
                data_types.append('continuum')
            if restart_speclines:
                data_types.append('speclines')

            # Clean up, update repo/ant files, and resubmit
            for data_type in data_types:

                await auto_pipe.rerun_job_submission(clustername=CLUSTERNAME,
                                                    data_type=data_type,
                                                    clusteracct=CLUSTERACCOUNT,
                                                    split_time=CLUSTER_SPLIT_JOBTIME,
                                                    line_time=CLUSTER_LINE_JOBTIME,
                                                    continuum_time=CLUSTER_LINE_JOBTIME,
                                                    scheduler_cmd=CLUSTER_SCHEDCMD,
                                                    split_mem=CLUSTER_SPLIT_MEM,
                                                    continuum_mem=CLUSTER_CONTINUUM_MEM,
                                                    line_mem=CLUSTER_LINE_MEM,
                                                    reindex=REINDEX,
                                                    casa_version=CASA_VERSION,
                                                    pipeline_branch=PIPELINE_BRANCHNAME)

                await asyncio.sleep(sleeptime)

            # Wait for job completion
            # await auto_pipe.get_job_notifications(check_continuum_job=restart_continuum,
            #                                       check_line_job=restart_speclines,
            #                                       sleeptime=1800)

            # log.info("Received job notifications for {auto_pipe.track_folder_name}")

            # # If completed, finish off before the others are done:
            # for data_type in auto_pipe.completions:

            #     if not auto_pipe.completions[data_type]:
            #         continue

            #     await auto_pipe.transfer_pipeline_products(data_type=data_type,
            #                                                startnode=CLUSTERNAME,
            #                                                endnode='ingester')

            #     await auto_pipe.transfer_calibrated_data(data_type=data_type,
            #                                              clustername=CLUSTERNAME)

            #     # Create the flagging sheets in the google sheet
            #     await auto_pipe.make_flagging_sheet(data_type=data_type)

            #     # Create the final QA products and move to the webserver
            #     log.info("Transferring QA products to webserver")
            #     auto_pipe.make_qa_products(data_type=data_type)

            #     auto_pipe.completions[data_type] = False

            # Handle submissions
            # while any(list(auto_pipe.restarts.values())):
            #     log.info(f"Checking and resubmitting pipeline jobs to {CLUSTERNAME}")
            #     log.info(f"Resubmissions only for failed/timeout pipeline jobs ")
            #     await auto_pipe.restart_job_submission(
            #                             max_resubmission=1,
            #                             clustername=CLUSTERNAME,
            #                             scripts_dir=Path('reduction_job_scripts/'),
            #                             submit_continuum_pipeline=restart_continuum,
            #                             submit_line_pipeline=restart_speclines,
            #                             clusteracct=CLUSTERACCOUNT,
            #                             split_time=CLUSTER_SPLIT_JOBTIME,
            #                             continuum_time=CLUSTER_CONTINUUM_JOBTIME,
            #                             line_time=CLUSTER_LINE_JOBTIME,
            #                             scheduler_cmd=CLUSTER_SCHEDCMD,)

            #     log.info("Checking and waiting for job completion")
            #     # Return dictionary of jobs to restart.
            #     await auto_pipe.get_job_notifications(check_continuum_job=RUN_CONTINUUM,
            #                                           check_line_job=RUN_LINES,
            #                                           sleeptime=1800)

            # # Move pipeline products to QA webserver
            # for data_type in auto_pipe.completions:

            #     await auto_pipe.transfer_pipeline_products(data_type=data_type,
            #                                                startnode=CLUSTERNAME,
            #                                                endnode='ingester')

            #     # Create the flagging sheets in the google sheet
            #     await auto_pipe.make_flagging_sheet(data_type=data_type)

            #     # Create the final QA products and move to the webserver
            #     auto_pipe.make_qa_products(data_type=data_type)

        log.info('Completed {}...'.format(auto_pipe.ebid))

        # Notify the queue that the item has been processed
        queue.task_done()


async def run(num_consume=4,
              **produce_kwargs):

    queue = asyncio.Queue()

    # fire up the both producers and consumers
    producers = [asyncio.create_task(produce(queue, **produce_kwargs))
                 for _ in range(1)]
    consumers = [asyncio.create_task(consume(queue))
                 for _ in range(num_consume)]

    # with both producers and consumers running, wait for
    # the producers to finish
    await asyncio.gather(*producers)
    print('---- done producing')

    # wait for the remaining tasks to be processed
    await queue.join()

    # cancel the consumers, which are now idle
    for c in consumers:
        c.cancel()


if __name__ == "__main__":

    import logging
    from datetime import datetime

    LOGGER_FORMAT = '%(asctime)s [%(levelname)s] [%(module)s:%(funcName)s] %(message)s'
    DATE_FORMAT = '[%Y-%m-%d %H:%M:%S]'
    logging.basicConfig(format=LOGGER_FORMAT, datefmt=DATE_FORMAT)

    log = logging.getLogger()
    log.setLevel(logging.INFO)
    # log.setLevel(logging.DEBUG)

    handler = logging.FileHandler(filename=f'logs/main_restarts.log')
    file_formatter = logging.Formatter(fmt=LOGGER_FORMAT, datefmt=DATE_FORMAT)
    handler.setFormatter(file_formatter)
    log.addHandler(handler)

    log.info(f'Starting new execution at {datetime.now().strftime("%Y_%m_%d_%H_%M")}')

    # Name of branch or tag to use for the reduction pipeline
    PIPELINE_BRANCHNAME = 'main'
    CASA_VERSION = "6.2"

    # Configuration parameters:
    CLUSTERNAME = 'cc-cedar'
    CLUSTERACCOUNT = 'rrg-eros-ab'

    CLUSTER_SCHEDCMD = "sbatch"

    CLUSTER_SPLIT_JOBTIME = '8:00:00'
    CLUSTER_CONTINUUM_JOBTIME = '70:00:00'
    CLUSTER_LINE_JOBTIME = '70:00:00'

    CLUSTER_SPLIT_MEM = '20000M'
    CLUSTER_CONTINUUM_MEM = '24000M'
    CLUSTER_LINE_MEM = '24000M'

    JOB_TYPE = "ALL"

    RUN_CONTINUUM = True
    RUN_LINES = True

    # Set whether to reindex the SPWs. Eventually, this should be set to False
    # everywhere!
    REINDEX = False

    # Number of jobs to run simultaneously
    NUM_CONSUMERS = 4

    # Set limits allowed for new jobs to be started.
    MIN_STORAGE = 3 * u.TB
    MIN_NUMFILES = 1e5
    MAX_NUMJOBS = 15

    uname = 'ekoch'
    sname = 'ualberta.ca'
    EMAILADDR = f"{uname}@{sname}"

    NRAODATAPATH = "/lustre/aoc/projects/20A-346/data_staged/"

    COMPLETEDDATAPATH = "/project/rrg-eros-ab/ekoch/VLAXL/calibrated/"

    SHEETNAMES = ['20A - OpLog Summary', 'Archival Track Summary']

    # Ask for password that will be used for ssh connections where the key connection
    # is not working.
    from getpass import unix_getpass

    password = unix_getpass()

    test_case_run_newest = False

    run_newest_first = True

    # Specify a target to grab the QA products and process
    TARGETS = ['IC10', 'NGC6822']

    start_with_newest = False

    global EBID_QUEUE_LIST
    EBID_QUEUE_LIST = []

    print("Starting new event loop")

    loop = asyncio.new_event_loop()

    loop.set_debug(False)
    loop.slow_callback_duration = 0.001

    loop.run_until_complete(run(start_with_newest=start_with_newest,
                                num_consume=NUM_CONSUMERS,
                                sheetnames=SHEETNAMES))
    loop.close()

    del loop


    # Run purely a test
    # ebid = 38730505
    # tester = AutoPipeline(ebid)
    # asyncio.run(tester.archive_request_and_transfer())
    # asyncio.run(tester.setup_for_reduction_pipeline(clustername=CLUSTERNAME))
    # asyncio.run(tester.get_flagging_files(data_type='continuum'))
    # asyncio.run(tester.get_flagging_files(data_type='speclines'))
    # asyncio.run(tester.initial_job_submission(
    #                                 clustername=CLUSTERNAME,
    #                                 scripts_dir=Path('reduction_job_scripts/'),
    #                                 submit_continuum_pipeline=RUN_CONTINUUM,
    #                                 submit_line_pipeline=RUN_LINES,
    #                                 clusteracct=CLUSTERACCOUNT,
    #                                 split_time=CLUSTER_SPLIT_JOBTIME,
    #                                 continuum_time=CLUSTER_CONTINUUM_JOBTIME,
    #                                 line_time=CLUSTER_LINE_JOBTIME,
    #                                 scheduler_cmd=CLUSTER_SCHEDCMD,))
    # asyncio.run(tester.get_job_notifications())
    # asyncio.run(tester.transfer_pipeline_products(data_type='speclines'))
    # asyncio.run(tester.transfer_pipeline_products(data_type='continuum'))
    # tester.make_qa_products(data_type='speclines')
    # tester.make_qa_products(data_type='continuum')
