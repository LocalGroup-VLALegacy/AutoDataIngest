
import ezgmail
from datetime import datetime

PROJECTID = "20A-346"

USERNAME = "ekoch"
EMAILADDR = "ualberta.ca"


def do_authentication_gmail(emailaddress=f"{USERNAME}@{EMAILADDR}"):
    """
    Check authentication for email address

    Parameters
    ----------
    emailladdress: str, optional
        Email to check login for.
    """

    if ezgmail.init() == emailaddress:
        return True
    else:
        return False


def check_for_archive_notification(ebid,
                                   labelname='Telescope Notifications/20A-346 Archive Notifications',
                                   project_id=PROJECTID,
                                   timewindow=2 * 24 * 3600,
                                   verbose=True,
                                   markasread=True,
                                   **kwargs):
    """
    Given en execution block ID, search for a notification that the archive has
    sent an email for the staged data. Return the whole MS name and lustre path on AOC.

    Parameters
    ----------
    timewindow: float or int, optional
        Time window to consider notification within. Default is 48 hr.
    """

    if not do_authentication_gmail(**kwargs):
        raise ValueError("Cannot login with ezgmail. Check credentials.")

    notification_emails = ezgmail.search(f"label:{labelname}")

    # Loop through and see if we find that EBID:
    for email in notification_emails:

        # Emails can have multiple messages (replies/grouped emails)
        for message in email.messages:

            if str(ebid) not in message.originalBody:
                continue

            if (datetime.now() - message.timestamp).total_seconds() > timewindow:
                if verbose:
                    print(f"Found notification older than {timewindow / 3600.} hr. Skipping.")
                continue

            # Grab and return the full MS name from the archive email:
            path_to_data, ms_name = extract_path_and_name(message.originalBody,
                                                          project_id=project_id)

            if markasread:
                message.markasRead()

            return path_to_data, ms_name

    return None


def extract_path_and_name(message, project_id=PROJECTID):
    """
    docstring
    """

    path_to_data = message.split("ftp://ftp.aoc.nrao.edu/")[-1].strip("\n").strip("\r")

    ms_name = f"{project_id}{path_to_data.split(PROJECTID)[-1].rstrip('.tar')}"

    return path_to_data, ms_name


def check_for_job_notification(jobid,
                               labelname='Telescope Notifications/20A-346 Cedar Jobs'):

    if not do_authentication_gmail():
        raise ValueError("Cannot login with ezgmail. Check credentials.")

    notification_emails = ezgmail.search(f"label:{labelname}")

    # Loop through and see if we find that job ID:
    for email in notification_emails:

        # Emails can have multiple messages (replies/grouped emails)
        for message in email.messages:

            if str(jobid) not in message.subject:
                continue

            # Grab completion, run time for job.
            jobinfo = message.subject.split(",")

            status = jobinfo[2].replace(' ', '')

            runtime = jobinfo[1].split(" ")[-1]

            return status, runtime

    return None


def add_jobtimes(times):

    from datetime import timedelta

    tdeltas = []

    for timestr in times:

        # Check for days
        nday = float(timestr.split('-')[0])

        subday_str = timestr.split('-')[-1].split(':')
        if len(subday_str) == 3:
            nhr = float(subday_str[0])
            nmin = float(subday_str[1])
            nsec = float(subday_str[2])
        elif len(subday_str) == 2:
            nhr = 0
            nmin = float(subday_str[0])
            nsec = float(subday_str[1])
        elif len(subday_str) == 1:
            nhr = 0
            nmin = 0
            nsec = float(subday_str[0])
        else:
            raise ValueError(f"Input time string {timestr} could not be understood.")

        tdeltas.append(timedelta(days=nday, hours=nhr, minutes=nmin, seconds=nsec))

    total_time = sum(tdeltas, timedelta())

    # Convert to original format:
    days = int(total_time.total_seconds() // (24 * 3600))
    remainder = total_time.total_seconds() % (24 * 3600)
    hrs = int(remainder // 3600)
    remainder = remainder % 3600
    mins = int(remainder // 60)
    secs = int(remainder % 60)

    total_timestr = f"{days}-{hrs:02d}:{mins:02d}:{secs:02d}"

    return total_timestr
