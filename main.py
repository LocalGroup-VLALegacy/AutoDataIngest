
'''
This is the script with the full queueing system.

Run as python main.py from command line.

REQUIRE python>=3.7 for asyncio.

'''

import asyncio
import time
from pathlib import Path

from autodataingest.gsheet_tracker.gsheet_functions import (find_new_tracks)

from autodataingest.ingest_pipeline_functions import (archive_request_and_transfer,
                                                      setup_for_reduction_pipeline,
                                                      initial_job_submission,
                                                      get_job_notifications)


async def produce(queue, sleeptime=10, test_case_run_newest=False):
    '''
    Check for new tracks from the google sheet.
    '''

    new_ebids = find_new_tracks()

    # Test case enabled will only queue a single job.
    if test_case_run_newest:
        print("Test case of 1 run only has been enabled.")
        new_ebids = new_ebids[-1:]

    for ebid in new_ebids:
        # produce an item
        print(f'Found new track with ID {ebid}')

        # Put a small gap between starting to consume processes
        await asyncio.sleep(sleeptime)

        # put the item in the queue
        await queue.put(ebid)


async def consume(queue):
    while True:
        # wait for an item from the producer
        ebid = await queue.get()

        # process the item
        print('Processing {}...'.format(ebid))
        # simulate i/o operation using sleep
        # await asyncio.sleep(1)

        print(f'Starting archive request for {ebid}')
        # 1.
        track_folder_name = \
            archive_request_and_transfer(ebid,
                                         emailaddr=EMAILADDR,
                                         lustre_path=NRAODATAPATH,
                                         do_cleanup=True)

        print(f"Setting up scripts for reduction.")
        setup_for_reduction_pipeline(track_folder_name,
                                     clustername=CLUSTERNAME)

        print(f"Submitting pipeline jobs to {CLUSTERNAME}")
        importsplit_jobid, continuum_jobid, line_jobid = \
            initial_job_submission(ebid,
                                track_folder_name,
                                clustername=CLUSTERNAME,
                                scripts_dir=Path('reduction_job_scripts/'),
                                scheduler_cmd=CLUSTER_SCHEDCMD,
                                submit_continuum_pipeline=RUN_CONTINUUM,
                                submit_line_pipeline=RUN_LINES,
                                clusteracct=CLUSTERACCOUNT,
                                split_time=CLUSTER_SPLIT_JOBTIME,
                                continuum_time=CLUSTER_CONTINUUM_JOBTIME,
                                line_time=CLUSTER_LINE_JOBTIME)

        print("Checking and waiting for job completion")
        # Return dictionary of jobs to restart.
        restarts = get_job_notifications(ebid,
                                         importsplit_jobid=importsplit_jobid,
                                         check_continuum_job=RUN_CONTINUUM,
                                         continuum_jobid=continuum_jobid,
                                         check_line_job=RUN_LINES,
                                         line_jobid=line_jobid,
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
    CLUSTER_CONTINUUM_JOBTIME = '30:00:00'
    CLUSTER_LINE_JOBTIME = '30:00:00'

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
