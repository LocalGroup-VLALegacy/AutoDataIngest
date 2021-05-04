
'''
It seems easiest to just wrap the command line tools.
'''

import os
import subprocess
import time
import asyncio
import async_timeout

from ..cluster_configs import ENDPOINT_INFO

USERNAME = "ekoch"


def do_authenticate_globus():
    """
    Check that we can login.
    """

    out = subprocess.run(['globus', 'login'], capture_output=True)

    if not len(out.stderr) == 0:

        raise ValueError("Login failed with {out.stderr}")


    # Check who is logged in:

    user = subprocess.run(['globus', 'whoami'], capture_output=True).stdout
    user = user.decode('utf-8')

    if user.split('@')[0] != USERNAME:
        raise ValueError("Unexpected login user? {user}")


def do_manual_login(nodename, verbose=True):
    '''
    Some transfer nodes aren't automated yet (we need some keys and
    stuff setup). For now just do some parts manually.

    '''

    from getpass import unix_getpass

    id_number = ENDPOINT_INFO[nodename]['endpoint_id']

    # Check if logged in.
    act_cmd = ['globus', 'endpoint', 'is-activated', id_number]
    out = subprocess.run(act_cmd, capture_output=True)

    # If logged in, don't bother with another login.
    if 'is activated' in out.stdout.decode('utf-8'):
        return True

    if verbose:
        print(f"Require manual login to {nodename}")

    username = input("Username:")
    password = unix_getpass()

    cmd = ['globus', 'endpoint', 'activate', '--myproxy', id_number,
           '--myproxy-username', username, '--myproxy-password', password]

    out = subprocess.run(cmd, capture_output=True)

    return True


async def globus_wait_for_completion(task_id, sleeptime=900,
                                     timeout=86400):
    '''
    Asynchronously poll if the transfer is completed.
    '''

    do_authenticate_globus()

    async with async_timeout.timeout(timeout):

        while True:
            # Will not return until the task is completed.
            out = subprocess.run(['globus', 'task', 'show', f"{task_id}", '--jmespath', 'status'],
                                capture_output=True)

            result = out.stdout.decode('utf-8')

            if "SUCCEEDED" in result:
                break
            elif "CANCELLED" in result:
                raise ValueError('Transfer has been cancelled.')
            else:
                # Wait
                await asyncio.sleep(sleeptime)


def transfer_file(track_name, track_folder_name, startnode='nrao-aoc',
                  endnode='cc-cedar',
                  wait_for_completion=False):
    """
    Start a globus transfer from `startnode` to `endnode`.
    """

    try:
        do_authenticate_globus()
    except ValueError:
        print(f"Auto authentication of {endnode} failed. Try manual login.")
        do_manual_login(endnode)

    # May have to change this ordering for both nodes in general.
    do_manual_login(startnode)

    # Make a new folder on `endnode` for the data to go to:
    mkdir_command = ["globus", "mkdir",
                     f"{ENDPOINT_INFO[endnode]['endpoint_id']}:{ENDPOINT_INFO[endnode]['data_path']}/{track_folder_name}"]

    out = subprocess.run(mkdir_command, capture_output=True)

    # Want to return the task_id in the command line output.
    input_cmd = f"{ENDPOINT_INFO[startnode]['endpoint_id']}:{ENDPOINT_INFO[startnode]['data_path']}/{track_name}.tar"
    output_cmd = f"{ENDPOINT_INFO[endnode]['endpoint_id']}:{ENDPOINT_INFO[endnode]['data_path']}/{track_folder_name}/{track_name}.tar"

    # task_command = f"$(globus transfer {input_cmd} {output_cmd} --jmes path 'task_id' --format=UNIX)"
    task_command = ['globus', 'transfer', input_cmd, output_cmd]

    task_transfer = subprocess.run(task_command, capture_output=True)

    # Extract the task ID from the stdout
    task_transfer_stdout = task_transfer.stdout.decode('utf-8').replace("\n", " ")

    if not 'accepted' in task_transfer_stdout:
        print(task_transfer_stdout)
        print(task_transfer.stderr.decode('utf-8'))

        raise ValueError("Transfer was not accepted Check the above messages.")

    task_id = task_transfer_stdout.split('Task ID:')[-1].replace(" ", '')


    # Wait for 30 seconds to allow the transfer to get started.
    time.sleep(30)

    if wait_for_completion:
        globus_wait_for_completion(task_id)

    return task_id


def transfer_pipeline(track_name, track_folder_name, endnode='cc-cedar'):
    """
    Grab a fresh pipeline repo version and transfer
    """

    foldername = f'{track_name}_reduction_pipeline'

    if not os.path.exists(foldername):
        os.mkdir(foldername)

    os.chdir(foldername)
    out = subprocess.run(['git', 'clone',
                          'https://github.com/LocalGroup-VLALegacy/ReductionPipeline.git'],
                         capture_output=True)

    out = subprocess.run(['tar', '-cf', 'ReductionPipeline.tar', 'ReductionPipeline'])

    os.chdir('..')

    # Transfer to the endnode
    input_cmd = f"{ENDPOINT_INFO['ingester']['endpoint_id']}:{ENDPOINT_INFO['ingester']['data_path']}/{foldername}/ReductionPipeline.tar"
    output_cmd = f"{ENDPOINT_INFO[endnode]['endpoint_id']}:{ENDPOINT_INFO[endnode]['data_path']}/{track_folder_name}/ReductionPipeline.tar"

    task_command = ["globus", "transfer", input_cmd, output_cmd]

    out = subprocess.run(task_command, capture_output=True)

    return True


def cleanup_source(track_name, node='nrao-aoc'):
    """
    Run after a transfer finishes to remove the track from the initial location.
    This is needed to not overwhelm our project storage limit on AOC.
    """

    do_manual_login(node)

    input_cmd = f"{ENDPOINT_INFO[node]['endpoint_id']}:{ENDPOINT_INFO[node]['data_path']}/{track_name}.tar"

    out = subprocess.run(['globus', 'rm', input_cmd], capture_output=True)

    time.sleep(30)

    return True


def transfer_general(filename, output_destination,
                    startnode='cc-cedar',
                    endnode='ingester',
                    wait_for_completion=False,
                    use_rootname=True,
                    skip_if_not_existing=True,
                    use_startnode_datapath=True,
                    use_endnode_datapath=True):

    """
    Start a globus transfer from `startnode` to `endnode`.
    """

    try:
        do_authenticate_globus()
    except ValueError:
        print(f"Auto authentication of {endnode} failed. Try manual login.")
        do_manual_login(endnode)

    # May have to change this ordering for both nodes in general.
    do_manual_login(startnode)

    if use_rootname:
        output_filename = filename.split('/')[-1]
    else:
        output_filename = filename

    # Want to return the task_id in the command line output.
    if use_startnode_datapath:
        input_cmd = f"{ENDPOINT_INFO[startnode]['endpoint_id']}:{ENDPOINT_INFO[startnode]['data_path']}/{filename}"
    else:
        input_cmd = f"{ENDPOINT_INFO[startnode]['endpoint_id']}:{filename}"

    if use_endnode_datapath:
        output_cmd = f"{ENDPOINT_INFO[endnode]['endpoint_id']}:{ENDPOINT_INFO[endnode]['data_path']}/{output_destination}/{output_filename}"
    else:
        output_cmd = f"{ENDPOINT_INFO[endnode]['endpoint_id']}:{output_destination}/{output_filename}"

    # Check if the input file/folder exists:
    task_command = ['globus', 'ls', "/".join(input_cmd.split("/")[:-1])]

    task_check = subprocess.run(task_command, capture_output=True)

    base_filename = filename.split('/')[0]

    if base_filename not in task_check.stdout.decode('utf-8'):
        if skip_if_not_existing:
            print(f"The file {base_filename} does not exist at {input_cmd}. Skipping.")
            return None

        raise ValueError(f"The file {base_filename} does not exist at {input_cmd}.")

    # task_command = f"$(globus transfer {input_cmd} {output_cmd} --jmes path 'task_id' --format=UNIX)"
    task_command = ['globus', 'transfer', input_cmd, output_cmd]

    task_transfer = subprocess.run(task_command, capture_output=True)

    # Extract the task ID from the stdout
    task_transfer_stdout = task_transfer.stdout.decode('utf-8').replace("\n", " ")

    if not 'accepted' in task_transfer_stdout:
        print(task_transfer_stdout)
        print(task_transfer.stderr.decode('utf-8'))

        raise ValueError("Transfer was not accepted Check the above messages.")

    task_id = task_transfer_stdout.split('Task ID:')[-1].replace(" ", '')

    # Wait for 30 seconds to allow the transfer to get started.
    time.sleep(30)

    if wait_for_completion:
        globus_wait_for_completion(task_id)

    return task_id
