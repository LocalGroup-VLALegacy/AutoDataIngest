
'''
Extract QA products and create HTML files for the webserver.
'''


'''
This is the script with the full queueing system.

Run as python main.py from command line.

REQUIRE python>=3.7 for asyncio.

'''

import asyncio
import time
from pathlib import Path

from autodataingest.gsheet_tracker.gsheet_functions import (return_all_ebids)

from autodataingest.ingest_pipeline_functions import AutoPipeline


async def produce(queue, sleeptime=10, start_with_newest=False):
    '''
    Check for new tracks from the google sheet.
    '''

    all_ebids = return_all_ebids()

    if start_with_newest:
        all_ebids = all_ebids[::-1]

    for ebid in all_ebids:
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

        if auto_pipe.target in TARGETS:

        # Move pipeline products to QA webserver
            await auto_pipe.transfer_pipeline_products(data_type='speclines',
                                                    startnode=CLUSTERNAME,
                                                    endnode='ingester')

            await auto_pipe.transfer_pipeline_products(data_type='continuum',
                                                    startnode=CLUSTERNAME,
                                                    endnode='ingester')

            # Create the final QA products and move to the webserver
            auto_pipe.make_qa_products(data_type='speclines')
            auto_pipe.make_qa_products(data_type='continuum')

        # Notify the queue that the item has been processed
        queue.task_done()


async def run(**produce_kwargs):
    queue = asyncio.Queue()
    # schedule the consumer
    consumer = asyncio.ensure_future(consume(queue))
    # run the producer and wait for completion
    await produce(queue, **produce_kwargs)
    # wait until the consumer has processed all items
    await queue.join()
    # the consumer is still awaiting for an item, cancel it
    consumer.cancel()


if __name__ == "__main__":

    # Configuration parameters:
    CLUSTERNAME = 'cc-cedar'

    RUN_CONTINUUM = True
    RUN_LINES = True

    uname = 'ekoch'

    # Specify a target to grab the QA products and process
    TARGETS = ['IC10', 'NGC6822']

    start_with_newest = True

    while True:

        print("Starting new event loop")

        loop = asyncio.new_event_loop()

        loop.set_debug(True)
        loop.slow_callback_duration = 0.001

        loop.run_until_complete(run(start_with_newest=start_with_newest))
        loop.close()

        del loop

        break
        # In production, comment out "break" and uncomment the sleep
        print("Completed current event loop.")
        time.sleep(3600)

    # Run purely a test
    # ebid = 38730505
    # tester = AutoPipeline(ebid)
    # asyncio.run(tester.transfer_pipeline_products(data_type='speclines'))
    # asyncio.run(tester.transfer_pipeline_products(data_type='continuum'))
