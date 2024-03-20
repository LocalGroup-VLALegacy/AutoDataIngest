#!/bin/bash
arguments=($SSH_ORIGINAL_COMMAND)
# arg0 = start_time_str

sacct --format="JobID,JobName%110,State%20" --starttime=${arguments[0]} | grep -v "^[0-9]*\."
