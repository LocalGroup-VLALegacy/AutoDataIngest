
import ezgmail

PROJECTID = "20A-346"

USERNAME = "ekoch"
EMAILADDR = "ualberta.ca"


def do_authentication(emailaddress=f"{USERNAME}@{EMAILADDR}"):
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


def check_for_archive_notification(ebid):
    """
    Given en execution block ID, search for a notification that the archive has
    sent an email for the staged data. Return the whole MS name and lustre path on AOC.
    """

    if not do_authentication():
        raise ValueError("Cannot login with ezgmail. Check credentials.")

    notification_emails = ezgmail.search("label:Telescope Notifications/20A-346 Archive Notifications")

    # Loop through and see if we find that EBID:
    for email in notification_emails:

        # Emails can have multiple messages (replies/grouped emails)
        for message in email.messages:

            if str(ebid) not in message.originalBody:
                continue

            # Grab and return the full MS name from the archive email:
            path_to_data, ms_name = extract_path_and_name(message.originalBody)

            return path_to_data, ms_name

    return None


def extract_path_and_name(message):
    """
    docstring
    """

    path_to_data = message.split("ftp://ftp.aoc.nrao.edu/")[-1].strip("\n").strip("\r")

    ms_name = f"{PROJECTID}{path_to_data.split(PROJECTID)[-1].rstrip('.tar')}"

    return path_to_data, ms_name