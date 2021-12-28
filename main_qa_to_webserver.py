
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
from autodataingest.gsheet_tracker.gsheet_functions import (update_track_status)

from autodataingest.ingest_pipeline_functions import AutoPipeline

from autodataingest.logging import setup_logging
log = setup_logging()


async def produce(queue, sleeptime=60, start_with_newest=False,
                  ebid_list=None):
    '''
    Check for new tracks from the google sheet.
    '''

    if ebid_list is None:
        all_ebids = return_all_ebids(sheetname=SHEETNAME)
    else:
        all_ebids = ebid_list

    if start_with_newest:
        all_ebids = all_ebids[::-1]

    for ebid_plus_types in all_ebids:
        # produce an item
        ebid, run_continuum, run_lines = ebid_plus_types

        print(f'Found new track with ID {ebid}')

        # Put a small gap between starting to consume processes
        await asyncio.sleep(sleeptime)

        # put the item in the queue
        await queue.put([AutoPipeline(ebid, sheetname=SHEETNAME), run_continuum, run_lines])


async def consume(queue, sleeptime=60):
    while True:
        # wait for an item from the producer
        auto_pipe, RUN_CONTINUUM, RUN_LINES = await queue.get()

        # process the item
        log.info('Processing {}...'.format(auto_pipe.ebid))
        # simulate i/o operation using sleep
        # await asyncio.sleep(1)

        if auto_pipe.target in TARGETS:

            data_types = []

            if RUN_CONTINUUM:
                data_types.append('continuum')

            if RUN_LINES:
                data_types.append('speclines')

            for data_type in data_types:

                log.info(f"Transferring {data_type}")

                if DO_DATA_TRANSFER:
                    await auto_pipe.transfer_calibrated_data(data_type=data_type,
                                                            clustername='cc-cedar')
                    # continue

                # Move pipeline products to QA webserver
                await auto_pipe.transfer_pipeline_products(data_type=data_type,
                                                        startnode=CLUSTERNAME,
                                                        endnode='ingester')

                await asyncio.sleep(sleeptime)

                log.info(f"Creating flagging sheet for {data_type} (if needed)")

                # Create the flagging sheets in the google sheet
                # await auto_pipe.make_flagging_sheet(data_type='continuum')
                await auto_pipe.make_flagging_sheet(data_type=data_type)

                # Create the final QA products and move to the webserver
                log.info(f"Creating QA products")
                auto_pipe.make_qa_products(data_type=data_type)

                log.info(f"Updating track status")
                update_track_status(auto_pipe.ebid, message=f"Ready for QA",
                                    sheetname=auto_pipe.sheetname,
                                    status_col=1 if data_type == 'continuum' else 2)

                await asyncio.sleep(sleeptime)

                log.info(f"Finished {data_type}")

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

    import logging
    from datetime import datetime

    LOGGER_FORMAT = '%(asctime)s %(message)s'
    DATE_FORMAT = '[%Y-%m-%d %H:%M:%S]'
    logging.basicConfig(format=LOGGER_FORMAT, datefmt=DATE_FORMAT)

    log = logging.getLogger()
    log.setLevel(logging.INFO)

    handler = logging.FileHandler(filename=f'logs/main_qa_to_webserver.log')
    file_formatter = logging.Formatter(fmt=LOGGER_FORMAT, datefmt=DATE_FORMAT)
    handler.setFormatter(file_formatter)
    log.addHandler(handler)

    log.info(f'Starting new execution at {datetime.now().strftime("%Y_%m_%d_%H_%M")}')

    # Configuration parameters:
    CLUSTERNAME = 'cc-cedar'

    uname = 'ekoch'

    SHEETNAME = '20A - OpLog Summary'
    # SHEETNAME = 'Archival Track Summary'

    DO_DATA_TRANSFER = True

    # Specify a target to grab the QA products and process
    TARGETS = ['IC10', 'NGC6822', 'M31', 'M33', 'IC1613', 'WLM']


    MANUAL_EBID_LIST = []

    # ebid, continuum, lines

    MANUAL_EBID_LIST.append([40264132, True, False])

    # MANUAL_EBID_LIST.append([41031266, False, True])

    # MANUAL_EBID_LIST.append([40729151, True, True])


    start_with_newest = False

    while True:

        print("Starting new event loop")

        loop = asyncio.new_event_loop()

        loop.set_debug(True)
        loop.slow_callback_duration = 0.001

        loop.run_until_complete(run(start_with_newest=start_with_newest,
                                    ebid_list=MANUAL_EBID_LIST))
        loop.close()

        del loop

        break
        # In production, comment out "break" and uncomment the sleep
        print("Completed current event loop.")
        time.sleep(3600)

    # Run purely a test
    # ebid = 39549030
    # tester = AutoPipeline(ebid)
    # asyncio.run(tester.transfer_pipeline_products(data_type='continuum'))
    # asyncio.run(tester.make_flagging_sheet(data_type='continuum'))
    # tester.make_qa_products(data_type='continuum')
    # asyncio.run(tester.transfer_pipeline_products(data_type='speclines'))
    # asyncio.run(tester.make_flagging_sheet(data_type='speclines'))
    # tester.make_qa_products(data_type='speclines')

