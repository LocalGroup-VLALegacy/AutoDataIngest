
def setup_logging(datefmt='[%Y-%m-%d %H:%M:%S]'):

    import logging

    LOGGER_FORMAT = '%(asctime)s %(message)s'
    logging.basicConfig(format=LOGGER_FORMAT, datefmt=datefmt)
    log = logging.getLogger()

    return log
