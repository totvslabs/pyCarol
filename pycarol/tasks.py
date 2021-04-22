import os


class Tasks:
    def __init__(self, carol, task_id=None):
        self.carol = carol

        if task_id is not None:
            self.get_task(task_id=task_id)
        else:
            self.task_id = os.environ.get('LONGTASKID')

    def _set_task_by_json(self, json_task):
        self.task_id = json_task.get('mdmId')
        self.user_id = json_task.get('mdmUserId')
        self.connector_id = json_task.get('mdmConnectorId')
        self.task_ready = json_task.get('mdmTaskReady')
        self.task_processing = json_task.get('mdmTaskProcessing')
        self.task_status = json_task.get('mdmTaskStatus')
        self.task_owner = json_task.get('mdmTaskOwner')
        self.task_progress = json_task.get('mdmTaskProgress')
        self.distribution_value = json_task.get('mdmDistributionValue')
        self.task_priority = json_task.get('mdmTaskPreference')
        self.number_of_steps = json_task.get('mdmNumberOfSteps')
        self.number_of_steps_executed = json_task.get('mdmNumberOfStepsExecuted')
        self.entity_type = json_task.get('mdmEntityType')
        self.created_date = json_task.get('mdmCreated')
        self.start_date = json_task.get('mdmStartDate')
        self.created_user = json_task.get('mdmCreatedUser')
        self.updated_user = json_task.get('mdmUpdatedUser')
        self.last_updated = json_task.get('mdmLastUpdated')
        self.tenant_id = json_task.get('mdmTenantId')
        self.process_after = json_task.get('mdmProcessAfter')
        self.data = json_task.get('mdmData')
        self.raw = json_task
        self.task_progress_data = json_task.get('mdmTaskProgressData', {})

    def get_by_task_id_in_env(self):
        task_id = os.getenv('LONGTASKID')
        assert task_id, "The task id has not been set in env by Carol."
        return self.get_task(task_id)

    def get_current_task_id(self):
        return os.getenv('LONGTASKID')

    def set_as_current_task(self):
        os.environ['LONGTASKID'] = self.task_id

    def get_task(self, task_id=None):
        """
        Get Task
        :param task_id: task id
        :return: Task
        """

        if task_id is None:
            task_id = self.task_id
            assert task_id, "Task ID should be set because it has not been set in env by Carol."

        json_task = self.carol.call_api(
            'v1/tasks/{}'.format(task_id), status_forcelist=(500, 502, 503, 504, 524))
        self._set_task_by_json(json_task)
        return self

    def trace(self, log_message, task_id=None):
        self.add_log(log_message, "TRACE", task_id)

    def debug(self, log_message, task_id=None):
        self.add_log(log_message, "DEBUG", task_id)

    def info(self, log_message, task_id=None):
        self.add_log(log_message, "INFO", task_id)

    def warn(self, log_message, task_id=None):
        self.add_log(log_message, "WARN", task_id)

    def error(self, log_message, task_id=None):
        self.add_log(log_message, "ERROR", task_id)

    def add_log(self, log_message, log_level="INFO", task_id=None):
        """
        Add a log
        :param log_message: commonly used tenandId
        :param log_level: options: ERROR, WARN, INFO, DEBUG, TRACE
        :param task_id: it's not necessary if self.id is defined or if we are running from Carol as a batch app
        :return: boolean
        """

        if task_id is None:
            task_id = self.task_id
            assert task_id, "Task ID should be set because it has not been set in env by Carol."

        log = [{
            "mdmTaskId": task_id,
            "mdmLogMessage": log_message,
            "mdmLogLevel": log_level.upper()
        }]
        return self.add_logs(log)

    def add_logs(self, logs, task_id=None):
        """
        Add more than one log

        Args:
            logs: `list`
                list of logs objects [{"task_id":"", "log_message": "", "log_level": ""}]
            task_id `str` default `None`
                    The task ID. it's not necessary if self.task_id is defined

        :return: Task
        """

        if task_id is None:
            task_id = self.task_id
            assert task_id, "Task ID should be set because it has not been set in env by Carol."

        resp = self.carol.call_api('v1/tasks/{}/logs'.format(task_id), data=logs, status_forcelist=(500, 502, 503,
                                                                                                    504, 524))
        return resp['success']

    def get_logs(self, task_id=None):
        """
        Get all logs

        Args:
           task_id `str` default `None`
                The task ID. it's not necessary if self.task_id is defined

        :return:
            list of logs
        """

        if task_id is None:
            task_id = self.task_id
            assert task_id, "Task ID should be set because it has not been set in env by Carol."

        return self.carol.call_api('v1/tasks/{}/logs'.format(task_id), status_forcelist=(500, 502, 503, 504, 524))

    def set_progress(self, progress, progress_data=None, task_id=None):
        """
        Set Task Progress

        Args:
            progress: `int`
                Number relative to progress
            progress_data: 'dict` default `None`
                Json payload to be sent to Carol
            task_id `str` default `None`
                The task ID. It's not necessary if self.task_id is defined

        :return:
            Task response.
        """

        if progress_data is None:
            progress_data = {}
        else:
            assert isinstance(progress_data, dict)

        if task_id is None:
            task_id = self.task_id
            assert task_id, "Task ID should be set because it has not been set in env by Carol."

        return self.carol.call_api('v1/tasks/{}/progress/{}'.format(task_id, progress), data=progress_data,
                                   status_forcelist=(500, 502, 503, 504, 524))

    def cancel(self, task_id=None, force=False):
        """
        Cancel the task

        Args:
            task_id: `str` default `None`
                The task ID. It's not necessary if self.task_id is defined
            force: `bool` default `False`
                Force cancel

        :return:
            boolean
        """

        if task_id is None:
            task_id = self.task_id
            assert task_id, "Task ID should be set because it has not been set in env by Carol."

        params = {"force": force}

        resp = self.carol.call_api(
            'v1/tasks/{}/cancel'.format(task_id), method="POST", params=params)
        return resp['mdmTaskStatus'] == 'CANCELED'

    def fail(self, task_id=None, message=''):
        """
        Fail the task

        Args:
            task_id: `str` default `None`
                The task Id. It's not necessary if self.task_id is defined
            :param message: `str` default ``
                message to log

        :return:
            boolean
        """

        if task_id is None:
            task_id = self.task_id
            assert task_id, "Task ID should be set because it has not been set in env by Carol."

        params = {"message": message}

        resp = self.carol.call_api(
            'v1/tasks/{}/fail'.format(task_id), method="POST", params=params)
        return resp['mdmTaskStatus'] == 'FAILED'
