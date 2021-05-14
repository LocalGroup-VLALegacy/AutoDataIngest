
'''
Use astroquery to pull out info on the track.
'''

import time
import astropy.units as u
from astroquery.nrao import Nrao
from astroquery.ned import Ned
from astropy.coordinates import SkyCoord

from .logging import setup_logging
log = setup_logging()


def match_ebid_to_source(ebid,
                         targets=['M31', 'M33', 'NGC6822', 'IC10', 'IC1613', 'WLM'],
                         project_code='20A-346',
                         verbose=True):
    """
    Query the NRAO archive and match the EBID to figure out
    the target observed.
    """

    # TODO: Handle multiple targets per track? Need to revisit depending on
    # observing strategy

    for target in targets:

        if verbose:
            log.info(f"Searching for target match {target}")

        # Lookup location in Ned b/c I'm encountering failures for the name resolve
        out = Ned.query_object(target)

        coord = SkyCoord(float(out['RA']) * u.deg, float(out['DEC']) * u.deg, frame='fk5')


        result_table = None

        # Queries seem to often fail...
        j = 0
        while True:

            try:
                result_table = Nrao.query_region(coord,
                                                telescope='jansky_vla',
                                                radius=1 * u.deg,
                                                obs_band='L',
                                                project_code=project_code,
                                                querytype='ARCHIVE',
                                                cache=False,
                                                retry=1)
            except Exception as e:
                log.warning(f"Query failed with {e}. Waiting then trying again.")
                # Wait some time before trying the query again
                time.sleep(30)

            if j >= 5:
                break

            j += 1

        if result_table is None:
            raise ValueError("Archive query failed.")

        log.debug(f"Found table {result_table}")

        log.info(f"Result table length {len(result_table)}")

        # Skip if empty
        if len(result_table) == 0 or "Archive File" not in result_table.colnames:
            if verbose:
                log.info("No tracks found for this target.")
            continue

        # Loop through MS names
        found_source = False
        for i, name in enumerate(result_table['Archive File']):

            if str(ebid) in name:
                found_source = True
                break

        if not found_source:
            continue

        data_size = result_table['File Size'][i]

        return target, data_size