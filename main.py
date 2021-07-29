
'''
This is the script with the full queueing system.

Run as python main.py from command line.

REQUIRE python>=3.7 for asyncio.

'''

import asyncio
import time
from pathlib import Path

from autodataingest.gsheet_tracker.gsheet_functions import (find_new_tracks)

from autodataingest.ingest_pipeline_functions import AutoPipeline

from autodataingest.logging import setup_logging
log = setup_logging()


async def produce(queue, sleeptime=600, test_case_run_newest=False,
                  run_newest_first=False,
                  long_sleep=7200):
    '''
    Check for new tracks from the google sheet.
    '''

    while True:

        # If we get an API error for too many requests, just wait a bit and
        # try again:
        try:
            new_ebids = find_new_tracks(sheetname=SHEETNAME)
        except:
            await asyncio.sleep(sleeptime * 10)
            continue

        # Switch order if running newest first.
        if run_newest_first:
            new_ebids = new_ebids[::-1]

        # Test case enabled will only queue two jobs.
        # This is a test of running >1 tracks concurrently.
        if test_case_run_newest:
            log.info("Test case of 2 run only has been enabled.")
            new_ebids = new_ebids[-2:]

        for ebid in new_ebids:
            if ebid in EBID_QUEUE_LIST:
                log.info(f'Skipping new track with ID {ebid} because it is still in the queue.')
                continue

            # produce an item
            log.info(f'Found new track with ID {ebid}')

            # Put a small gap between starting to consume processes
            await asyncio.sleep(sleeptime)

            EBID_QUEUE_LIST.append(ebid)

            # put the item in the queue
            this_pipe = AutoPipeline(ebid, sheetname=SHEETNAME)
            this_pipe.initial_status()

            await queue.put(this_pipe)

        if test_case_run_newest:
            break

        await asyncio.sleep(long_sleep)


async def consume(queue):
    while True:
        # wait for an item from the producer
        auto_pipe = await queue.get()

        EBID_QUEUE_LIST.remove(auto_pipe.ebid)

        # process the item
        log.info('Processing {}...'.format(auto_pipe.ebid))
        # simulate i/o operation using sleep
        # await asyncio.sleep(1)

        log.info(f'Starting archive request for {auto_pipe.ebid}')
        # 1.
        await auto_pipe.archive_request_and_transfer(archive_kwargs={'emailaddr': EMAILADDR,
                                                                     'lustre_path': NRAODATAPATH},
                                                    sleeptime=600,
                                                    clustername=CLUSTERNAME,
                                                    do_cleanup=False)

        log.info(f"Setting up scripts for reduction.")
        await auto_pipe.setup_for_reduction_pipeline(clustername=CLUSTERNAME)

        log.info("Create the flagging sheets in the google sheet (if they exist)")
        await auto_pipe.get_flagging_files(data_type='continuum')
        await auto_pipe.get_flagging_files(data_type='speclines')

        log.info(f"Submitting pipeline jobs to {CLUSTERNAME}")
        await auto_pipe.initial_job_submission(
                                clustername=CLUSTERNAME,
                                scripts_dir=Path('reduction_job_scripts/'),
                                submit_continuum_pipeline=RUN_CONTINUUM,
                                submit_line_pipeline=RUN_LINES,
                                clusteracct=CLUSTERACCOUNT,
                                split_time=CLUSTER_SPLIT_JOBTIME,
                                continuum_time=CLUSTER_CONTINUUM_JOBTIME,
                                line_time=CLUSTER_LINE_JOBTIME,
                                scheduler_cmd=CLUSTER_SCHEDCMD,)

        log.info("Checking and waiting for job completion")
        # Return dictionary of jobs to restart.
        await auto_pipe.get_job_notifications(check_continuum_job=RUN_CONTINUUM,
                                              check_line_job=RUN_LINES,
                                              sleeptime=1800)

        # Handle submissions
        # while any(list(auto_pipe.restarts.values())):
        #     log.info(f"Checking and resubmitting pipeline jobs to {CLUSTERNAME}")
        #     log.info(f"Resubmissions only for failed/timeout pipeline jobs ")
        #     await auto_pipe.restart_job_submission(
        #                             max_resubmission=1,
        #                             clustername=CLUSTERNAME,
        #                             scripts_dir=Path('reduction_job_scripts/'),
        #                             submit_continuum_pipeline=RUN_CONTINUUM,
        #                             submit_line_pipeline=RUN_LINES,
        #                             clusteracct=CLUSTERACCOUNT,
        #                             split_time=CLUSTER_SPLIT_JOBTIME,
        #                             continuum_time=CLUSTER_CONTINUUM_JOBTIME,
        #                             line_time=CLUSTER_LINE_JOBTIME,
        #                             scheduler_cmd=CLUSTER_SCHEDCMD,)

        #     log.info("Checking and waiting for job completion")
        #     # Return dictionary of jobs to restart.
        #     await auto_pipe.get_job_notifications(check_continuum_job=RUN_CONTINUUM,
        #                                         check_line_job=RUN_LINES,
        #                                         sleeptime=1800)

        log.info("Transferring pipeline products")
        # Move pipeline products to QA webserver
        await auto_pipe.transfer_pipeline_products(data_type='speclines',
                                                   startnode='cc-cedar',
                                                   endnode='ingester')

        await auto_pipe.transfer_pipeline_products(data_type='continuum',
                                                   startnode='cc-cedar',
                                                   endnode='ingester')

        # Create the flagging sheets in the google sheet
        log.info("Creating flagging sheets")
        await auto_pipe.make_flagging_sheet(data_type='continuum')
        await auto_pipe.make_flagging_sheet(data_type='speclines')


        # Create the final QA products and move to the webserver
        log.info("Transferring QA products to webserver")
        auto_pipe.make_qa_products(data_type='speclines')
        auto_pipe.make_qa_products(data_type='continuum')

        # Notify the queue that the item has been processed
        log.info('Completed {}...'.format(auto_pipe.ebid))
        queue.task_done()


async def run(num_produce=1, num_consume=4,
              **produce_kwargs):

    queue = asyncio.Queue()

    # fire up the both producers and consumers
    producers = [asyncio.create_task(produce(queue, **produce_kwargs))
                 for _ in range(num_produce)]
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

    # Configuration parameters:
    CLUSTERNAME = 'cc-cedar'
    CLUSTERACCOUNT = 'rrg-eros-ab'

    CLUSTER_SCHEDCMD = "sbatch"

    CLUSTER_SPLIT_JOBTIME = '8:00:00'
    CLUSTER_CONTINUUM_JOBTIME = '48:00:00'
    CLUSTER_LINE_JOBTIME = '48:00:00'

    RUN_CONTINUUM = True
    RUN_LINES = True

    NUM_CONSUMERS = 6

    uname = 'ekoch'
    sname = 'ualberta.ca'
    EMAILADDR = f"{uname}@{sname}"

    NRAODATAPATH = "/lustre/aoc/projects/20A-346/data_staged/"

    SHEETNAME = '20A - OpLog Summary'

    # Ask for password that will be used for ssh connections where the key connection
    # is not working.
    from getpass import unix_getpass

    password = unix_getpass()

    test_case_run_newest = False

    run_newest_first = False

    global EBID_QUEUE_LIST
    EBID_QUEUE_LIST = []

    print("Starting new event loop")

    loop = asyncio.new_event_loop()

    loop.set_debug(False)
    loop.slow_callback_duration = 0.001

    loop.run_until_complete(run(test_case_run_newest=test_case_run_newest,
                                run_newest_first=run_newest_first,
                                num_consume=NUM_CONSUMERS))
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
