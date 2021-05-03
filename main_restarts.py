
'''
This script triggers re-running the pipeline after QA, and triggers the QA completion sequence.

Run as python main.py from command line.

REQUIRE python>=3.7 for asyncio.

'''

from AutoDataIngest.main import SHEETNAME
import asyncio
import time
from pathlib import Path

from autodataingest.gsheet_tracker.gsheet_functions import (find_rerun_status_tracks)

from autodataingest.ingest_pipeline_functions import AutoPipeline


async def produce(queue, sleeptime=10, start_with_newest=False,
                  ebid_list=None,
                  long_sleep=7200):
    '''
    Check for new tracks from the google sheet.
    '''

    while True:

        if ebid_list is None:
            all_ebids = find_rerun_status_tracks(sheetname=SHEETNAME)
        else:
            all_ebids = ebid_list

        if start_with_newest:
            all_ebids = all_ebids[::-1]

        for ebid in all_ebids:
            # produce an item
            print(f'Found new track with ID {ebid}')

            # Put a small gap between starting to consume processes
            await asyncio.sleep(sleeptime)

            # put the item in the queue
            await queue.put(AutoPipeline(ebid, sheetname=SHEETNAME))

        if test_case_run_newest:
            break

        await asyncio.sleep(long_sleep)


async def consume(queue):
    while True:
        # wait for an item from the producer
        auto_pipe = await queue.get()

        # process the item
        print('Processing {}...'.format(auto_pipe.ebid))
        # simulate i/o operation using sleep
        # await asyncio.sleep(1)

        restart_continuum = auto_pipe._qa_review_input(data_type='continuum') == "RESTART"
        restart_speclines = auto_pipe._qa_review_input(data_type='speclines') == "RESTART"

        if restart_continuum or restart_speclines:

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
                                                    pipeline_time=CLUSTER_LINE_JOBTIME,
                                                    scheduler_cmd=CLUSTER_SCHEDCMD,)

            # Wait for job completion
            await auto_pipe.get_job_notifications(check_continuum_job=True,
                                                  check_line_job=True,
                                                  sleeptime=1800)

            # Move pipeline products to QA webserver
            for data_type in data_types:

                await auto_pipe.transfer_pipeline_products(data_type=data_type,
                                                           startnode=CLUSTERNAME,
                                                           endnode='ingester')

                # Create the flagging sheets in the google sheet
                await auto_pipe.make_flagging_sheet(data_type=data_type)

                # Create the final QA products and move to the webserver
                auto_pipe.make_qa_products(data_type=data_type)

        print('Completed {}...'.format(auto_pipe.ebid))

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

    SHEETNAME = '20A - OpLog Summary'

    # Ask for password that will be used for ssh connections where the key connection
    # is not working.
    from getpass import unix_getpass

    password = unix_getpass()

    test_case_run_newest = False

    run_newest_first = True

    # Specify a target to grab the QA products and process
    TARGETS = ['IC10', 'NGC6822']

    # MANUAL_EBID_LIST = [39591025]
    MANUAL_EBID_LIST = None

    start_with_newest = True

    print("Starting new event loop")

    loop = asyncio.new_event_loop()

    loop.set_debug(False)
    loop.slow_callback_duration = 0.001

    loop.run_until_complete(run(start_with_newest=start_with_newest,
                                ebid_list=MANUAL_EBID_LIST))
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
