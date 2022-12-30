
'''
Simple script to check for new MS files in two text files.

See usage in sync_to_gdrive.py.
'''

import sys

cedar_files = set(open('cedar_files.txt', 'r').readlines())
drive_files = set(open('drive_files.txt', 'r').readlines())

diff_files = list(cedar_files - drive_files)

if len(diff_files) == 0:
    print("No diff files found. Exiting.")
    sys.exit(0)

# Create two text files:
# 1. List of files to iterate through
# 2. Batch source/dest formatted file for globus to use.

for filename in diff_files:
    # the archive folder is where we have older MS files to reduce the number
    # of items in the calibrated folder. This avoids timeouts with globus.
    if "archive" in filename:
        continue
    print(filename, file=open('new_ms_files.txt', 'a'))

# 2. Now create the batch format:
# This DOESN'T WORK. Despite end='', a newline is created. This might have to do with
# some internal setting for the maximum line length. We're doing this in the shell now
# for filename in diff_files:
    # Force no line end
    # print(f"{filename} {filename}", end='', file=open('batch_files.txt', 'a'))
    # print(filename, file=open('batch_files.txt', 'a'))

