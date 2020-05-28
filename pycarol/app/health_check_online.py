import os

from pycarol.tasks import Tasks
from pycarol.carol import Carol


class HealthCheckOnline():

    def __init__(self, logs):
        self.domain = os.environ.get('CAROLDOMAIN')
        self.app_name = os.environ.get('CAROLAPPNAME')
        self.app_version = os.environ.get('CAROLAPPVERSION')
        self.online_name = os.environ.get('CAROLONLINENAME')
        self.connector_id = os.environ.get('CAROLCONNECTORID')
        self.app_oauth = os.environ.get('CAROLAPPOAUTH')
        self.long_task_id = os.environ.get('LONGTASKID')

        self.logs = logs


    def carol_auth(self):
        try:
            carol = Carol()
            # pyCarol should return a error if the credentials
            # are invalid, so for now we try to validate if
            # those are valid by another method
            carol.api_key_details(self.app_oauth,self.connector_id)
            return carol
        except Exception as e:
            raise Exception("Carol authentication failed.", e)


    def send_status_carol(self):
        if self.domain:
            carol = self.carol_auth()
            tasks = Tasks(carol)
            task = tasks.get_task(task_id=self.long_task_id)

            if self.logs == []:
                task.set_progress(80)
            else:
                task.set_progress(50)
                task.add_log(str(self.logs), log_level='ERROR')
