import os
import logging

class Logger():
    """ It implements google.cloud.logging and return a logging object in order to send logs to the Stackdriver.
    If the $GOOGLE_APPLICATION_CREDENTIALS is not defined, this class will provide a default 'logging' and the
    logs will not be send to Stackdriver.

    Usage:
        from logger import Logger
        logger = Logger()
        logger.log.warning('It`s a warning message')
        logger.log.error('It`s an error message')
        logger.log.critical('It`s a critical message')

        ## It's possible add another handler
        # logger.log.addHandler(<handle obj>)
    """

    def __init__(self, name=None):
        if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
            process_type = os.environ.get('CAROLPROCESSTYPE', '')

            process_name = ''
            if process_type.lower() == 'online':
                process_name = os.environ.get('CAROLONLINENAME', '')
            elif process_type.lower() == 'batch':
                process_name = os.environ.get('CAROLBATCHNAME', '')

            tenant = os.environ.get('CAROLTENANT', '')
            app_name = os.environ.get('CAROLAPPNAME', '')
            app_version = os.environ.get('CAROLAPPVERSION', '')

            from google.cloud import logging as google_logging
            client = google_logging.Client()

            formatter = logging.Formatter('{} > {} > {} > {} > {} > %(message)s'.format(tenant, app_name, app_version, process_type, process_name))
            handler = client.get_default_handler()
            handler.setFormatter(formatter)

            self.log = logging.getLogger(name)
            self.log.addHandler(handler)

        else:
            self.log = logging.getLogger(name)