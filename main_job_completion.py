
import asyncio
import time
from pathlib import Path
import pandas as pd


from autodataingest.ingest_pipeline_functions import AutoPipeline

from autodataingest.ssh_utils import setup_ssh_connection

from autodataingest.gsheet_tracker.gsheet_functions import find_running_tracks

from autodataingest.job_monitor import get_slurm_job_monitor, identify_completions

from autodataingest.logging import setup_logging
log = setup_logging()

def return_job_type(row):
    if row['JobType'] == "continuum_pipeline_default":
        return "continuum"
    elif row['JobType'] == "line_pipeline_default":
        return "speclines"
    else:
        raise ValueError(f"Unable to interpret job type {row['JobType']}")


async def produce(queue, sleeptime=60, longsleeptime=3600,
                  clustername='cc-cedar',
                  sheetnames=['20A - OpLog Summary']):
    '''
    Check for new tracks from the google sheet.
    '''

    log.info(f"Checking job status from {clustername}")

    while True:

        running_tracks = dict.fromkeys(sheetnames)

        for sheetname in sheetnames:

            sheet_running_tracks = find_running_tracks(sheetname=sheetname)

            running_tracks[sheetname] = sheet_running_tracks

            await asyncio.sleep(120)

        cluster_key = 'cedar-robot-jobstatus'
        connect = setup_ssh_connection(cluster_key)
        df = get_slurm_job_monitor(connect, time_range_days=TIME_RANGE_DAYS)
        connect.close()

        log.info("Checking for completed jobs")

        for sheetname in sheetnames:

            log.info(f"Job searches for {sheetname}")

            df_comp, df_fail = identify_completions(df, running_tracks[sheetname])

            if len(df_comp) > 0:

                log.info(f"Found completions for: {df_comp['EBID']}")

                for index, row in df_comp.iterrows():

                    ebid = int(row['EBID'])
                    job_id = int(row['JobID'])
                    data_type = return_job_type(row)

                    auto_pipe = AutoPipeline(ebid, sheetname=sheetname)
                    auto_pipe.set_qa_queued_status(data_type=data_type)
                    auto_pipe.set_job_stats(job_id, data_type)

                    log.info(f"Adding to queue {ebid}:{data_type} for completed job {job_id}")
                    await queue.put([auto_pipe, data_type])

                    await asyncio.sleep(sleeptime)
            else:
                log.info("No completions found.")

            if len(df_fail) > 0:

                log.info(f"Found failures for: {df_fail['EBID']}")

                for index, row in df_fail.iterrows():

                    ebid = int(row['EBID'])
                    job_status = row['State']
                    job_id = int(row['JobID'])

                    log.info(f"Failure on {ebid}, {job_status}, {job_id}")

                    auto_pipe = AutoPipeline(ebid, sheetname=sheetname)

                    if row['JobType'] == "import_and_split":
                        auto_pipe.set_job_status('continuum', job_status)
                        auto_pipe.set_job_status('speclines', job_status)
                        auto_pipe.set_job_stats(job_id, "import_and_split")

                    else:
                        data_type = return_job_type(row)
                        auto_pipe.set_job_status(data_type, job_status)
                        auto_pipe.set_job_stats(job_id, data_type)

                    await asyncio.sleep(sleeptime)
            else:
                log.info("No failures found.")

            await asyncio.sleep(120)

        log.info("Finished parsing job statuses.")

        await asyncio.sleep(longsleeptime)


async def consume(queue, sleeptime=60):
    while True:
        # wait for an item from the producer
        auto_pipe, data_type = await queue.get()

        # process the item
        log.info(f'Processing {auto_pipe.ebid} {data_type}')

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
        await auto_pipe.make_flagging_sheet(data_type=data_type)

        # Create the final QA products and move to the webserver
        log.info(f"Creating QA products")
        auto_pipe.make_qa_products(data_type=data_type)

        log.info(f"Updating track status")
        auto_pipe.set_job_status(data_type, "COMPLETED")

        await asyncio.sleep(sleeptime)

        log.info(f"Finished {auto_pipe.ebid} {data_type}")

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

    handler = logging.FileHandler(filename=f'logs/main_job_completion.log')
    file_formatter = logging.Formatter(fmt=LOGGER_FORMAT, datefmt=DATE_FORMAT)
    handler.setFormatter(file_formatter)
    log.addHandler(handler)

    log.info(f'Starting new execution at {datetime.now().strftime("%Y_%m_%d_%H_%M")}')

    # Configuration parameters:
    CLUSTERNAME = 'cc-cedar'

    uname = 'ekoch'

    SHEETNAMES = ['20A - OpLog Summary', 'Archival Track Summary']

    DO_DATA_TRANSFER = True

    # Time range to check for job completion
    TIME_RANGE_DAYS = 14

    while True:

        print("Starting new event loop")

        loop = asyncio.new_event_loop()

        loop.set_debug(False)
        loop.slow_callback_duration = 0.001

        loop.run_until_complete(run(clustername=CLUSTERNAME,
                                    sheetnames=SHEETNAMES))
        loop.close()

        del loop

        break
        # In production, comment out "break" and uncomment the sleep
        print("Completed current event loop.")
        time.sleep(3600)

