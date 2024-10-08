
'''
This is the script with the full queueing system.

Run as python main.py from command line.

REQUIRE python>=3.7 for asyncio.

'''

import asyncio
import time
from pathlib import Path

from autodataingest.gsheet_tracker.gsheet_functions import (find_new_tracks)
from autodataingest.globus_functions import globus_ebid_check_exists

from autodataingest.ingest_pipeline_functions import AutoPipeline

from autodataingest.logging import setup_logging
log = setup_logging()


async def produce(queue, sleeptime=600, test_case_run_newest=False,
                  run_newest_first=False,
                  long_sleep=3600 * 6,
                  sheetnames=['20A - OpLog Summary']):
    '''
    Check for new tracks from the google sheet.
    '''

    while True:

        log.info("Checking for new jobs")

        # If we get an API error for too many requests, just wait a bit and
        # try again:
        new_ebids = []

        for sheetname in sheetnames:
            try:
                sheet_new_ebids = find_new_tracks(sheetname=sheetname)
            except:
                await asyncio.sleep(sleeptime * 10)
                sheet_new_ebids = []
                continue

            for this_new_ebid in sheet_new_ebids:
                new_ebids.append([this_new_ebid, sheetname])

            await asyncio.sleep(120)


        # Switch order if running newest first.
        if run_newest_first:
            new_ebids = new_ebids[::-1]

        # Test case enabled will only queue two jobs.
        # This is a test of running >1 tracks concurrently.
        if test_case_run_newest:
            log.info("Test case of 2 run only has been enabled.")
            new_ebids = new_ebids[-2:]

        for ebid, sheetname in new_ebids:
            if ebid in EBID_QUEUE_LIST:
                log.info(f'Skipping new track with ID {ebid} because it is still in the queue.')
                continue

            # produce an item
            log.info(f'Found new track with ID {ebid} on sheet {sheetname}')

            track_name = globus_ebid_check_exists(ebid)
            if track_name is None:
                log.info(f"EBID {ebid} is still staging. Skipping.")
                continue

            # Put a small gap between starting to consume processes
            await asyncio.sleep(sleeptime)

            EBID_QUEUE_LIST.append(ebid)

            # put the item in the queue
            this_pipe = AutoPipeline(ebid, sheetname=sheetname)
            this_pipe.track_name = track_name
            await this_pipe.initial_status()

            await queue.put(this_pipe)

            log.info(f"There are now {queue.qsize()} items in the queue.")

        if test_case_run_newest:
            break

        await asyncio.sleep(long_sleep)


async def consume(queue):
    while True:
        log.info("Starting consume")

        log.info(f"consume: There are {queue.qsize()} items in the queue.")

        # wait for an item from the producer
        auto_pipe = await queue.get()

        # process the item
        log.info('Processing {}...'.format(auto_pipe.ebid))
        # simulate i/o operation using sleep
        # await asyncio.sleep(120)

        EBID_QUEUE_LIST.remove(auto_pipe.ebid)


        log.info(f'Starting archive request for {auto_pipe.ebid}')
        # 1.
        await auto_pipe.archive_request_and_transfer(archive_kwargs={'emailaddr': EMAILADDR,
                                                                     'lustre_path': NRAODATAPATH},
                                                    sleeptime=600,
                                                    clustername=CLUSTERNAME,
                                                    do_cleanup=True)

        log.info(f"Setting up scripts for reduction.")
        await auto_pipe.setup_for_reduction_pipeline(clustername=CLUSTERNAME,
                                                     pipeline_branch=PIPELINE_BRANCHNAME)

        log.info("Create the flagging sheets in the google sheet (if they exist)")
        # Create the flagging sheets in the google sheet
        await auto_pipe.make_flagging_sheet(data_type='continuum')
        await auto_pipe.make_flagging_sheet(data_type='speclines')

        await auto_pipe.get_flagging_files(data_type='continuum')
        await auto_pipe.get_flagging_files(data_type='speclines')

        # Grab any refantignore files as specified in the summary sheet
        await auto_pipe.get_refantignore_files(data_type='continuum')
        await auto_pipe.get_refantignore_files(data_type='speclines')

        log.info(f"Submitting pipeline jobs to {CLUSTERNAME}")
        await auto_pipe.initial_job_submission(
                                clustername=CLUSTERNAME,
                                scripts_dir=Path('reduction_job_scripts/'),
                                submit_continuum_pipeline=RUN_CONTINUUM,
                                submit_line_pipeline=RUN_LINES,
                                # clusteracct=CLUSTERACCOUNT,
                                split_time=CLUSTER_SPLIT_JOBTIME,
                                continuum_time=CLUSTER_CONTINUUM_JOBTIME,
                                line_time=CLUSTER_LINE_JOBTIME,
                                split_mem=CLUSTER_SPLIT_MEM,
                                continuum_mem=CLUSTER_CONTINUUM_MEM,
                                line_mem=CLUSTER_LINE_MEM,
                                scheduler_cmd=CLUSTER_SCHEDCMD,
                                reindex=False,
                                casa_version=CASA_VERSION,)

        log.info("Checking and waiting for job completion")

        # Notify the queue that the item has been processed
        queue.task_done()
        log.info('Completed {}...'.format(auto_pipe.ebid))
        del auto_pipe


async def run(num_produce=1, num_consume=4,
              **produce_kwargs):

    log.info(f"Creating queue given {num_produce} producers and {num_consume} consumers.")

    queue = asyncio.Queue()

    # fire up the both producers and consumers
    producers = [asyncio.create_task(produce(queue, **produce_kwargs))
                 for _ in range(num_produce)]
    consumers = [asyncio.create_task(consume(queue))
                 for _ in range(num_consume)]

    log.info("Created producers and consumers.")

    # with both producers and consumers running, wait for
    # the producers to finish
    await asyncio.gather(*producers)
    log.info('---- done producing')

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

    # Add file logger
    handler = logging.FileHandler(filename=f'logs/main.log')
    file_formatter = logging.Formatter(fmt=LOGGER_FORMAT, datefmt=DATE_FORMAT)
    handler.setFormatter(file_formatter)
    log.addHandler(handler)

    # Add stream logger
    # stream = logging.StreamHandler()
    # streamformat = logging.Formatter(fmt=LOGGER_FORMAT, datefmt=DATE_FORMAT)
    # stream.setLevel(logging.DEBUG)
    # stream.setFormatter(streamformat)

    # log.addHandler(stream)

    log.info(f'Starting new execution at {datetime.now().strftime("%Y_%m_%d_%H_%M")}')

    # Name of branch or tag to use for the reduction pipeline
    PIPELINE_BRANCHNAME = 'main'
    CASA_VERSION = "6.5"

    # Configuration parameters:
    CLUSTERNAME = 'cc-cedar'
    CLUSTERACCOUNT = 'rrg-eros-ab'

    CLUSTER_SCHEDCMD = "sbatch"

    CLUSTER_SPLIT_JOBTIME = '12:00:00'
    CLUSTER_CONTINUUM_JOBTIME = '72:00:00'
    CLUSTER_LINE_JOBTIME = '72:00:00'

    CLUSTER_SPLIT_MEM = '32000M'
    CLUSTER_CONTINUUM_MEM = '32000M'
    CLUSTER_LINE_MEM = '40000M'

    RUN_CONTINUUM = True
    RUN_LINES = True

    NUM_CONSUMERS = 1

    uname = 'ekoch'
    sname = 'ualberta.ca'
    EMAILADDR = f"{uname}@{sname}"

    NRAODATAPATH = "/lustre/aoc/projects/20A-346/data_staged/"

    SHEETNAMES = ['20A - OpLog Summary', 'Archival Track Summary']

    # Ask for password that will be used for ssh connections where the key connection
    # is not working.
    # from getpass import unix_getpass

    # password = unix_getpass()

    test_case_run_newest = False

    run_newest_first = False

    global EBID_QUEUE_LIST
    EBID_QUEUE_LIST = []

    print("Starting new event loop")

    loop = asyncio.new_event_loop()

    loop.set_debug(False)
    loop.slow_callback_duration = 0.001

    loop.run_until_complete(run(num_consume=NUM_CONSUMERS,
                                sheetnames=SHEETNAMES))
    loop.close()

    del loop

    # Run purely a test
    # ebid = 39992361
    # tester = AutoPipeline(ebid)
    # asyncio.run(tester.archive_request_and_transfer(do_cleanup=False, timewindow=1.))
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