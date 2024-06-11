
'''
Functions for ssh loging and run using the fabric and paramiko paxkages.
'''

import time
import fabric
import paramiko
import signal
from contextlib import contextmanager
import asyncio
import socket

from .cluster_configs import CLUSTERADDRS

from .logging import setup_logging
log = setup_logging()


def try_run_command(connect, test_cmd='ls', timeout=600):
    """
    Try running a command on the given connection as a test of whether a password is
    required.
    """

    try:
        result = connect.run(test_cmd, hide=True, timeout=timeout)
        return True
    except paramiko.PasswordRequiredException:
        return False


def run_command(connect, cmd, test_connection=False, timeout=600, allow_failure=False):
    """
    Run a command on the given connection.
    """

    if test_connection:
        if try_run_command(connect) is False:
            raise ValueError("Connection requires a password.")

    result = connect.run(cmd, hide=True, timeout=timeout, warn=True)

    if result.failed and not allow_failure:
        raise ValueError(f"Failed to run {cmd}! See stderr: {result.stderr}")

    return result

class TimeoutException(Exception):
    pass

@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise TimeoutException("Timed out!")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


async def run_job_submission(connect, cmd, track_name, job_name, test_connection=False, timeout=600,
                             retry_attempts=5):
    '''
    This wraps `run_command` specifically for submitting slurm jobs.
    In several cases, the run function hangs, but the job is submitted. In
    those cases, we catch the hanging run and instead return the job number by using
    `sacct` to link the track name to the job ID.
    '''

    tries = 0

    while True:

        job_id = None

        try:
            with time_limit(timeout):

                result = run_command(connect, cmd, test_connection=test_connection,
                                     timeout=timeout)

                job_id = result.stdout.replace("\n", '').split(" ")[-1]
                break

        except TimeoutException:
            log.info(f"Timed out on attempt {tries}. Waiting 1 min before checking job status.")

            await asyncio.sleep(60)

            # Try connecting again and checking submitted jobs to see if the job was submitted
            try:
                with time_limit(timeout):
                    sched_cmd = 'sacct --format="JobID,JobName%100"'
                    result = run_command(connect, sched_cmd, test_connection=test_connection,
                                        timeout=timeout)

                    job_list = result.stdout.split('\n')

                    # No jobs == 2 list of 2. Need to retry submission
                    if len(job_list) == 2:
                        log.info("No submitted jobs. Retrying.")
                    else:
                        # Match track name in the job name:
                        for job_desc in job_list[2:]:
                            if track_name in job_desc and job_name in job_desc:
                                job_id = job_desc.split(' ')[0]
                                log.info(f"Successfully identified job ID {job_id} in queue.")
                                break
            except TimeoutException:
                log.info("Job queue check failed to connect and return. Retrying...")
                pass

        if job_id is not None:
            break

        tries += 1

        if tries >= retry_attempts:
            raise ValueError(f"Unable to submit job after {tries} attempts. Submission failed.")

        log.info(f"Waiting {timeout} s before re-attempting job submission")
        await asyncio.sleep(timeout)

    return job_id


def setup_ssh_connection(clustername, user='ekoch',
                               max_retry_connection=10,
                               connection_timeout=60,
                               reconnect_waittime=900):
    '''
    Setup and test the ssh connection to the cluster.
    '''

    if not clustername in CLUSTERADDRS:
        raise ValueError(f"Given cluster name {clustername} is not defined in CLUSTERADDRS. "
                         f"Valid names are: {list(CLUSTERADDRS.keys())}")

    retry_times = 0
    while True:
        try:
            # with time_limit(connection_timeout):
            connect = fabric.Connection(CLUSTERADDRS[clustername],
                                        user=user,
                                        connect_timeout=20)
            # connect_kwargs={'passphrase': globals()['password'] if 'password' in globals() else ""},

            # I'm getting intermittent DNS issues on the CC cloud.
            # This is to handle waiting until the DNS problem goes away
            # connect.open()

            # connect.open()
            log.info(f"Opened connection to {clustername}")

            break

        # except (socket.gaierror, TimeoutException) as e:
        except Exception as e:
            log.info(f"SSH connection reached exception {e}")
            log.info(f"Waiting {reconnect_waittime} sec before trying again")

        retry_times += 1

        if retry_times >= max_retry_connection:
            raise Exception(f"Reached maximum retries to connect to {clustername}")

        log.info("Waiting to retry connection")
        time.sleep(reconnect_waittime)

    # Test the connection:
    # if not try_run_command(connect):
    #     raise ValueError(f"Cannot login to {CLUSTERADDRS[clustername]}. Requires password.")

    return connect
