
'''
This is the script with the full queueing system.

Run as python main.py from command line.

REQUIRE python>=3.7 for asyncio.

'''

import asyncio
import time

from autodataingest.gsheet_tracker.gsheet_functions import (find_new_tracks)

async def produce(queue, sleeptime=10):
    '''
    Check for new tracks from the google sheet.
    '''
    for ebid in find_new_tracks():
        # produce an item
        print(f'Found new track with ID {ebid}')

        # Put a small gap between starting to consume processes
        await asyncio.sleep(sleeptime)

        # put the item in the queue
        await queue.put(ebid)


async def consume(queue):
    while True:
        # wait for an item from the producer
        item = await queue.get()

        # process the item
        print('consuming {}...'.format(item))
        # simulate i/o operation using sleep
        await asyncio.sleep(1)

        # Notify the queue that the item has been processed
        queue.task_done()


async def run():
    queue = asyncio.Queue()
    # schedule the consumer
    consumer = asyncio.ensure_future(consume(queue))
    # run the producer and wait for completion
    await produce(queue)
    # wait until the consumer has processed all items
    await queue.join()
    # the consumer is still awaiting for an item, cancel it
    consumer.cancel()


if __name__ == "__main__":

    while True:

        print("Starting new event loop")

        # loop = asyncio.get_event_loop()
        loop = asyncio.new_event_loop()
        loop.run_until_complete(run())
        loop.close()

        del loop

        # In production, comment out "break" and uncomment the sleep
        print("Completed current event loop.")
        time.sleep(30)
