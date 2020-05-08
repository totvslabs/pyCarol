class Compute:
    """
    Compute.

    """

    def __init__(self, carol):
        self.carol = carol

    def get_machine_types(self):
        response = self.carol.call_api(path=f'v1/compute/machineTypes',
                                       method='GET')

        return response

    def get_app_logs(self, app_name=None, app_type='carolApp', severity='INFO', filters=None, page_size=10, ):
        """
        Get execution logs for an app.

        Args:
            app_name: `str` default `None`
                Carol app name. It will overwrite the app name used in Carol() initialization.
            app_type: `str` default `carolApp`
                Possible values `carolApp` or `carolConnect`
            severity: `str` default `INFO`
                Minimal log to fetch.
            filters: `list` default `None`
                List with extra filters in string format. e.g,.
                filters = ['''"insertId"="n7qvjkx9se0erdgi1"''', "OR", '''"severity"="ERROR"''']
            page_size: `int` default 10
                Number of records to return.
        Returns:

        """

        if app_name is None:
            app_name = self.carol.app_name

        filters = filters if filters else []

        filters = '\n'.join(filters)

        params = {
            "appType": app_type,
            "appName": app_name,
            "pageSize": page_size,
            "severity": severity
        }

        response = self.carol.call_api(path=f'v1/log/retrieveLogs', data=filters,
                                       method='POST', params=params, content_type='text/plain')

        return response
