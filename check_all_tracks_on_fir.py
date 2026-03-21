
import subprocess

from autodataingest.gsheet_tracker.gsheet_functions import get_tracknames

from autodataingest.cluster_configs import ENDPOINT_INFO


startnode = 'dra-fir'

# this_path = Path("/home/datamanager/work/vlaxl/calibrated_archive/")
data_paths = ['/home/ekoch/projects/rrg-eros-ab/ekoch/VLAXL/calibrated',
              '/home/ekoch/projects/rrg-eros-ab/ekoch/VLAXL/calibrated/archive/',
              '/home/ekoch/nearline/rrg-eros-ab/ekoch/calibrated',
              '/home/ekoch/nearline/rrg-eros-ab/ekoch/calibrated/archive/']


# Check existence on Fir project and archive spaces.

archive_files = []

# this_data_path = data_paths[0]

for this_data_path in data_paths:

    print(this_data_path)

    input_cmd = f"{ENDPOINT_INFO[startnode]['endpoint_id']}:{this_data_path}"

    # Check if the input file/folder exists:
    task_command = ['globus', 'ls', input_cmd] # , '--filter', f"{source_name}*"]

    print(f"Full globus command is: {task_command}")
    task_check = subprocess.run(task_command, capture_output=True)

    stdout_list = task_check.stdout.decode('utf-8').split('\n')
    if len(stdout_list) < 2:
        raise ValueError("Check output for success")

    archive_files.extend(stdout_list)

# Remove duplicates
archive_files = list(set(archive_files))

# Remove empty strings
archive_files = [this_file for this_file in archive_files if len(this_file) > 0]


# if base_filename not in task_check.stdout.decode('utf-8'):
#     if skip_if_not_existing:
#         log.warning(f"The file {base_filename} does not exist at {input_cmd}. Skipping.")


missing_in_archive = {}

all_source_names = ['M31', 'M33', 'NGC6822', 'IC10', 'IC1613', 'WLM']

# source_name = 'M31'
sheetnames = ['20A - OpLog Summary', 'Archival Track Summary']
config = 'all'
completed_status = True


for source_name in all_source_names:

    print(f"On {source_name}")

    track_names = get_tracknames(source_name,
                                sheetnames=sheetnames,
                                config=config,
                                completed_status=completed_status)

    source_archive_files = [name for name in archive_files if source_name in name]

    source_missing_in_archive = []

    # Loop through all files

    for track_name in track_names:

        matches = [this_filename for this_filename in source_archive_files if track_name in this_filename]

        # This shouldn't happen...
        if len(matches) > 2:
            raise ValueError(f"Found too many matches: {matches}")
        # There should be 2 matches: continuum and speclines.
        elif len(matches) == 2:
            continue
        # Otherwise note the name and which is missing
        elif len(matches) == 0:
            source_missing_in_archive.append([track_name, 'all'])
        # Only 1 part missing
        else:
            if "speclines" in matches[0]:
                source_missing_in_archive.append([track_name, 'continuum'])
            elif "continuum" in matches[0]:
                source_missing_in_archive.append([track_name, 'speclines'])
            else:
                raise ValueError(f"Check match name: {matches}")

    missing_in_archive[source_name] = source_missing_in_archive

print("Resulting archive check:")
for this_name in missing_in_archive:
    print(this_name)

    if len(missing_in_archive[this_name]) == 0:
        print("No missing tracks!")
    else:
        for this_item in missing_in_archive[this_name]:
            print(this_item)
    print("")

