
import ezgmail

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


def check_for_archive_notification(ebid, parameter_list):
    """
    Given en execution block ID, search for a notification that the archive has
    sent an email for the staged data.
    """

    if not do_authentication():
        raise ValueError("Cannot login with ezgmail. Check credentials.")

    notification_emails = ezgmail.search("label:Telescope Notifications/20A-346")

    # Loop through and see if we find that EBID:
    pass
