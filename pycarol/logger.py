import os
import logging


class CarolHandler(logging.Handler):
    """ Handler to organize Carol logging
    This implementation send logs to the Google Stackdriver.

    Required OS Environment variables:
        * GOOGLE_APPLICATION_CREDENTIALS
        * CAROLPROCESSTYPE
        * CAROLONLINENAME | CAROLBATCHNAME
        * CAROLTENANT
        * CAROLAPPNAME
        * CAROLAPPVERSION

    Usage:
        from logger import Logger
        logger = Logger()
        logger.log.warning('It`s a warning message')
        logger.log.error('It`s an error message')
        logger.log.critical('It`s a critical message')
        ## It's possible add another handler
        # logger.log.addHandler(<handle obj>)
    """

    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
        process_type = os.environ.get('CAROLPROCESSTYPE', '')
        tenant = os.environ.get('CAROLTENANT', '')
        app_name = os.environ.get('CAROLAPPNAME', '')
        app_version = os.environ.get('CAROLAPPVERSION', '')
        process_name = ''
        if process_type.lower() == 'online':
            process_name = os.environ.get('CAROLONLINENAME', '')
        elif process_type.lower() == 'batch':
            process_name = os.environ.get('CAROLBATCHNAME', '')
        self.formatter = logging.Formatter('{} > {} > {} > {} > {} > %(message)s'.format(
            tenant, app_name, app_version, process_type, process_name))

        if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
            from google.cloud import logging as google_logging
            client = google_logging.Client()
            self.handler = client.get_default_handler()
        else:
            raise ValueError('Tried to create a CarolHandler but no credentials for a service were found.')
        self.handler.setFormatter(self.formatter)

    def emit(self, record):
        return self.handler.emit(record)