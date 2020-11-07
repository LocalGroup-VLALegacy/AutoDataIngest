
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


async def produce(queue, sleeptime=10, test_case_run_newest=False):
    '''
    Check for new tracks from the google sheet.
    '''

    new_ebids = find_new_tracks()

    # Test case enabled will only queue two jobs.
    # This is a test of running >1 tracks concurrently.
    if test_case_run_newest:
        print("Test case of 2 run only has been enabled.")
        new_ebids = new_ebids[-2:]

    for ebid in new_ebids:
        # produce an item
        print(f'Found new track with ID {ebid}')

        # Put a small gap between starting to consume processes
        await asyncio.sleep(sleeptime)

        # put the item in the queue
        await queue.put(AutoPipeline(ebid))


async def consume(queue):
    while True:
        # wait for an item from the producer
        auto_pipe = await queue.get()

        # process the item
        print('Processing {}...'.format(auto_pipe.ebid))
        # simulate i/o operation using sleep
        # await asyncio.sleep(1)

        print(f'Starting archive request for {auto_pipe.ebid}')
        # 1.
        await auto_pipe.archive_request_and_transfer(archive_kwargs={'emailaddr': EMAILADDR,
                                                                     'lustre_path': NRAODATAPATH},
                                                    sleeptime=600,
                                                    clustername=CLUSTERNAME,
                                                    do_cleanup=False)

        print(f"Setting up scripts for reduction.")
        await auto_pipe.setup_for_reduction_pipeline(clustername=CLUSTERNAME)

        print(f"Submitting pipeline jobs to {CLUSTERNAME}")
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

        print("Checking and waiting for job completion")
        # Return dictionary of jobs to restart.
        await auto_pipe.get_job_notifications(check_continuum_job=RUN_CONTINUUM,
                                              check_line_job=RUN_LINES,
                                              sleeptime=1800)

        # TODO: Add job restarting when timeouts occur.

        # TODO: Move pipeline products to QA webserver

        # Notify the queue that the item has been processed
        queue.task_done()


async def run(test_case_run_newest=False):
    queue = asyncio.Queue()
    # schedule the consumer
    consumer = asyncio.ensure_future(consume(queue))
    # run the producer and wait for completion
    await produce(queue, test_case_run_newest=test_case_run_newest)
    # wait until the consumer has processed all items
    await queue.join()
    # the consumer is still awaiting for an item, cancel it
    consumer.cancel()


if __name__ == "__main__":

    # Configuration parameters:
    CLUSTERNAME = 'cc-cedar'
    CLUSTERACCOUNT = 'rrg-eros-ab'

    CLUSTER_SCHEDCMD = "sbatch"

    CLUSTER_SPLIT_JOBTIME = '8:00:00'
    CLUSTER_CONTINUUM_JOBTIME = '48:00:00'
    CLUSTER_LINE_JOBTIME = '48:00:00'

    RUN_CONTINUUM = True
    RUN_LINES = True

    uname = 'ekoch'
    sname = 'ualberta.ca'
    EMAILADDR = f"{uname}@{sname}"

    NRAODATAPATH = "/lustre/aoc/projects/20A-346/data_staged/"

    # Ask for password that will be used for ssh connections where the key connection
    # is not working.
    from getpass import unix_getpass

    password = unix_getpass()

    test_case_run_newest = True

    while True:

        print("Starting new event loop")

        loop = asyncio.new_event_loop()
        loop.run_until_complete(run(test_case_run_newest=test_case_run_newest))
        loop.close()

        del loop

        if test_case_run_newest:
            print('Completed test case. Stoping.')
            break

        # In production, comment out "break" and uncomment the sleep
        print("Completed current event loop.")
        time.sleep(3600)

    # Run purely a test
    # ebid = 38730505
    # tester = AutoPipeline(ebid)
    # asyncio.run(tester.archive_request_and_transfer())
    # asyncio.run(tester.setup_for_reduction_pipeline(clustername=CLUSTERNAME))
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
