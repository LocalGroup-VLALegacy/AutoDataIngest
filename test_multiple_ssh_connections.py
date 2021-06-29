
'''
This script triggers re-running the pipeline after QA, and triggers the QA completion sequence.

Run as python main.py from command line.

REQUIRE python>=3.7 for asyncio.

'''

import asyncio
import time
from pathlib import Path

from autodataingest.ssh_utils import (try_run_command, run_command,
                                      time_limit, TimeoutException,
                                      run_job_submission)
import fabric
import socket

from autodataingest.logging import setup_logging
log = setup_logging()


async def produce(queue, sleeptime=600,
                  long_sleep=10):
    '''
    Check for new tracks from the google sheet.
    '''

    i = 0
    while True:

        # put the item in the queue
        await queue.put(i)

        i += 1

        await asyncio.sleep(long_sleep)

        if i >= 10:
            break

async def setup_ssh_connection(user='ekoch',
                               max_retry_connection=10,
                               connection_timeout=60,
                               reconnect_waittime=900):
    '''
    Setup and test the ssh connection to the cluster.
    '''

    retry_times = 0
    while True:
        try:
            with time_limit(connection_timeout):
                connect = fabric.Connection('cedar.computecanada.ca',
                                            user=user,
                                            connect_kwargs={'passphrase': globals()['password'] if 'password' in globals() else ""},
                                            connect_timeout=20)
            break

        except (socket.gaierror, TimeoutException) as e:
            log.info(f"SSH connection reached exception {e}")
            log.info("Waiting {reconnect_waittime} sec before trying again")

        retry_times += 1

        if retry_times >= max_retry_connection:
            raise Exception(f"Reached maximum retries to connect to cedar")

        log.info("Waiting to retry connection")
        await asyncio.sleep(reconnect_waittime)

    # Test the connection:
    if not try_run_command(connect):
        raise ValueError(f"Cannot login to cedar. Requires password.")

    return connect

async def consume(queue, sleeptime=1800, sleeptime_finish=600):
    while True:

        # wait for an item from the producer
        num = await queue.get()

        log.info('Starting {}...'.format(num))

        connect = await setup_ssh_connection()

        connect.open()

        connect.run("echo HELLO_WORLD ")

        log.info('Completed {}...'.format(num))

        connect.close()
        del connect

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

    LOGGER_FORMAT = '%(asctime)s %(message)s'
    DATE_FORMAT = '[%Y-%m-%d %H:%M:%S]'
    logging.basicConfig(format=LOGGER_FORMAT, datefmt=DATE_FORMAT)

    log = logging.getLogger()
    # log.setLevel(logging.INFO)
    log.setLevel(logging.DEBUG)

    handler = logging.FileHandler(filename=f'ssh_testing.log')
    file_formatter = logging.Formatter(fmt=LOGGER_FORMAT, datefmt=DATE_FORMAT)
    handler.setFormatter(file_formatter)
    log.addHandler(handler)

    log.info(f'Starting new execution at {datetime.now().strftime("%Y_%m_%d_%H_%M")}')

    # Configuration parameters:

    print("Starting new event loop")

    loop = asyncio.new_event_loop()

    loop.set_debug(False)
    loop.slow_callback_duration = 0.001

    loop.run_until_complete(run())
    loop.close()

    del loop

