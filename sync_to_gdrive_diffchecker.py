
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
    print(filename, file=open('new_ms_files.txt', 'a'))

# 2. Now create the batch format:
for filename in diff_files:
    # Force no line end
    print(filename, end=' ', file=open('batch_files.txt', 'a'))
    print(f" {filename}", file=open('batch_files.txt', 'a'))

