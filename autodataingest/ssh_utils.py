
'''
Functions for ssh loging and run using the fabric and paramiko paxkages.
'''

import fabric
import paramiko
import signal
from contextlib import contextmanager


def try_run_command(connect, test_cmd='ls', timeout=600):
    """
    Try running a command on the given connection as a test of whether a password is
    required.
    """

    try:
        result = connect.run(test_cmd, hide=True, timeout=timeout, command_timeout=timeout)
        return True
    except paramiko.PasswordRequiredException:
        return False


def run_command(connect, cmd, test_connection=False, timeout=600,):
    """
    Run a command on the given connection.
    """

    if test_connection:
        if try_run_command(connect) is False:
            raise ValueError("Connection requires a password.")

    result = connect.run(cmd, hide=True, timeout=timeout)

    if result.failed:
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
