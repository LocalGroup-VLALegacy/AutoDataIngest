
'''
Functions for ssh loging and run using the fabric and paramiko paxkages.
'''

import fabric
import paramiko


def try_run_command(connect, test_cmd='ls'):
    """
    Try running a command on the given connection as a test of whether a password is
    required.
    """

    try:
        result = connect.run(test_cmd, hide=True)
        return True
    except paramiko.PasswordRequiredException:
        return False


def run_command(connect, cmd, test_connection=False):
    """
    Run a command on the given connection.
    """

    if test_connection:
        if try_run_command(connect) is False:
            raise ValueError("Connection requires a password.")

    result = connect.run(cmd, hide=True)

    if result.failed:
        raise ValueError(f"Failed to clone pipeline! See stderr: {result.stderr}")

    return result
