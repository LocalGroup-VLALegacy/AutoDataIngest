
'''
Some clusters (like cedar) don't allow internet access to jobs. This
routine below will check a pre-downloaded text file for antenna corrections that
is updated prior to the pipeline run. `gencal` is then used to create the correction
cal table.
'''

import datetime
import os


def download_vla_antcorr(data_folder="VLA_antcorr_tables"):
    '''
    Download the VLA antenna correction tables since 2010 into text files within
    the `data_folder` directory.
    '''

    import urllib

    URL_BASE = 'http://www.vla.nrao.edu/cgi-bin/evlais_blines.cgi?Year='

    if not os.path.exists(data_folder):

        os.mkdir(data_folder)

    current_year = datetime.datetime.now().year

    # Loop through old years, if they exist:
    for year in range(2010, current_year):

        # If an older year file already exists, skip
        if os.path.exists(f"{data_folder}/{year}.txt"):
            continue

        response = urllib.request.urlopen(URL_BASE + str(year))

        html = response.read().decode('utf-8')
        html_lines = html.split('\n')

        response.close()

        with open(f"{data_folder}/{year}.txt", 'w') as f:
            for line in html_lines:
                f.write(line + "\n")

    # Always update the current year for recent changes:

    response = urllib.request.urlopen(URL_BASE + str(current_year))

    html = response.read().decode('utf-8')
    html_lines = html.split('\n')

    response.close()

    # If an older year file already exists, skip
    if os.path.exists(f"{data_folder}/{current_year}.txt"):
        os.remove(f"{data_folder}/{current_year}.txt")

    with open(f"{data_folder}/{current_year}.txt", 'w') as f:
        for line in html_lines:
            f.write(line + "\n")
