
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import TimeoutException

import os
import time

import logging
LOGGER_FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=LOGGER_FORMAT, datefmt='[%H:%M:%S]')
log = logging.getLogger()
log.setLevel(logging.INFO)


def archive_copy_SDM(eb, emailaddr="ekoch@ualberta.ca",
                     lustre_path="/lustre/aoc/projects/20A-346/data_staged/",
                     projectaccess_key_file=os.path.expanduser('~/20A-346_accesskey.txt'),
                     retry_times=5, retry_wait=30,
                     save_screenshot=True):
    '''
    eb : execution block, e.g., "TCAL0003_sb3369507_1_001.55601.74787561342"
    emailaddr : email address
    lustre_path : path to staging area on lustre. directory must be world writable

    Note that the project access key is not critical for security. It only enables us to
    download locked data from the NRAO archive without logging in with a NRAO account.
    Hence it being locally saved in a text file.
    '''

    # If given a key file, read in the project access key.
    # expected to be a text file with ONLY the key
    access_code = None
    if projectaccess_key_file is not None:
        with open(projectaccess_key_file, 'r') as f:
            access_code = f.read().rstrip("\n")

    # Force a Xvfb buffer to run. This avoids some of the headless run issues
    # on the canfar instance
    # if not os.getenv("DISPLAY") is None:
    if os.getenv("DISPLAY") is None:
        os.system("Xvfb :1 &")
        os.system("export DISPLAY=:1")

    # path to a non-default firefox profile used only for this purpose.
    # firefox may refuse to start if an instance is already running with the default profile
    # fp = webdriver.FirefoxProfile('/path/to/.mozilla/firefox/nmvisyfj.headless-profile')
    # If one running, use default profile
    fp = webdriver.FirefoxProfile()

    # the following should start firefox in headless (no-GUI) mode, but this is not working at NRAO
    # also not working on canfar cloud instance
    options = Options()
    options.headless = True
    # driver = webdriver.Firefox(fp, firefox_options=options, executable_path='/home/ekoch/geckodriver')

    num_tries = 0

    while True:

        driver = None

        try:
            # Requires geckodriver
            driver = webdriver.Firefox(fp, options=options)

            # I haven't needed this window size option but it was included in an example I was working with.
            # It may be useful in headless mode.
            #driver.set_window_size(1024, 768)

            # load advanced search page
            driver.get('https://archive.nrao.edu/archive/advquery.jsp')

            elem = driver.find_element_by_name('ARCHIVE_VOLUME')
            # Distinguish each track by its unique EB name.
            elem.send_keys(eb)

            # Give access code if needed
            if access_code is not None:
                log.info(f"Giving access code: {access_code}")
                elem = driver.find_element_by_name('PASSWD')
                elem.send_keys(access_code)

            elem.send_keys(Keys.RETURN)

            # load search results page
            elem = WebDriverWait(driver, 300, poll_frequency=2).until(lambda x: x.find_element_by_name('EMAILADDR'))
            elem.send_keys(emailaddr)

            log.info("Made it to archive result page")

            # Select SDM
            elem = driver.find_element_by_xpath("//input[@name='CONVERT2FORMAT' and @value='SDM']")
            elem.click()

            elem = driver.find_element_by_name('TARFILE')
            if not elem.get_attribute('checked'):
                elem.click()

            # We only take the first search result.
            elem = driver.find_element_by_name('FTPCHECKED')
            if not elem.get_attribute('checked'):
                elem.click()

            # Set path to send to
            elem = driver.find_element_by_name('COPYFILEROOT')
            elem.clear()
            elem.send_keys(lustre_path)
            elem.send_keys(Keys.RETURN)

            # accept popup
            # EK: I've removed this as of 04/17/2021 as the behaviour changed
            # and selenium crashes trying to find a non-existent user file.
            # It's possible the pop-up no longer exists and selenium crashes somewhere
            # else unrelated to the FileNotFoundError.
            # Also can't just try/except wrap this b/c it's async

            # WebDriverWait(driver, 5).until(EC.alert_is_present())
            # alert = driver.switch_to.alert
            # alert.accept()

            # load download page
            elem = WebDriverWait(driver, 300, poll_frequency=2).until(lambda x: x.find_element_by_xpath("//input[@name='DOWNLOADFTPCHK' and @value='Retrieve over internet']"))
            elem.click()

            log.info("Made it to archive download page")

            if save_screenshot:
                driver.save_screenshot('{}.archive_request.png'.format(eb))

            driver.close()
            driver.quit()

            break

        except TimeoutException:

            if driver is not None:
                driver.close()
                driver.quit()

            num_tries += 1

            # Wait some period of time before trying request again.
            time.sleep(retry_wait)

        if num_tries >= retry_times:
            raise TimeoutException("Continual timeout from NRAO archive.")

if __name__ == "__main__":

    # archive_copy_SDM( "TCAL0004.sb33611499.eb33624971.57833.33474003473", 'jmarvil@nrao.edu','/lustre/aoc/users/jmarvil/archive/new')


    import argparse

    parser = argparse.ArgumentParser(description='Options for creating slurm job files.')

    parser.add_argument('eb', type=str,
                        help='Execution block number')

    parser.add_argument('--lustre_path', type=str,
                        default='/lustre/aoc/projects/20A-346/data_staged/',
                        help='VLA configuration')

    parser.add_argument('--email_address', type=str,
                        default='ekoch@ualberta.ca',
                        help='Email to send archive notification email to.')


    args = parser.parse_args()

    # Testing parameters
    # Adapting for one of the 20A-346 scripts
    # eb = 38216745
    # One of our 14B-088 tracks
    # eb = 29882607
    # lustre_path = "/lustre/aoc/observers/nm-7669/data/"
    # lustre_path = "/lustre/aoc/projects/20A-346/data_staged/"

    archive_copy_SDM(args.eb, emailaddr=args.email_address,
                     lustre_path=args.lustre_path)