
'''
Helper script to quickly check the tracks on disk vs. the spreadsheet.
'''

from pathlib import Path

from autodataingest.gsheet_tracker.gsheet_functions import check_tracks_on_disk

this_path = Path("/home/datamanager/work/vlaxl/calibrated_archive/")

on_disk, unmatched_files = check_tracks_on_disk("IC10", this_path)
print(on_disk)
print(unmatched_files)

on_disk, unmatched_files = check_tracks_on_disk("IC1613", this_path)
print(on_disk)
print(unmatched_files)

on_disk, unmatched_files = check_tracks_on_disk("WLM", this_path)
print(on_disk)
print(unmatched_files)


this_path_phangs = Path("/home/datamanager/work/phangs_vla/calibrated_archive/")

on_disk, unmatched_files = check_tracks_on_disk("NGC628", this_path_phangs)
print(on_disk)
print(unmatched_files)

on_disk, unmatched_files = check_tracks_on_disk("NGC3627", this_path_phangs)
print(on_disk)
print(unmatched_files)

on_disk, unmatched_files = check_tracks_on_disk("NGC4254", this_path_phangs)
print(on_disk)
print(unmatched_files)
