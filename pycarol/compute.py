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

    def get_app_logs(self, app_name=None, app_type='carolApp', severity='INFO', page_token=None,  filters=None,
                     page_size=10, ):
        """
        Get execution logs for an app.

        Args:
            app_name: `str` default `None`
                Carol app name. It will overwrite the app name used in Carol() initialization.
            app_type: `str` default `carolApp`
                Possible values `carolApp` or `carolConnect`
            severity: `str` default `INFO`
                Minimal log to fetch.
            page_token: `str` default `None`
                Pagination token for logs.
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

        if page_token is None:
            url = f'v1/log/retrieveLogs'
        else:
            url = f'v1/log/retrieveLogs/nextPage'
            params.update({"pageToken": page_token})

        response = self.carol.call_api(path=url, data=filters, backoff_factor=1,
                                       status_forcelist=[502, 429, 524, 408, 504, 598, 520, 503],
                                       method='POST', params=params, content_type='text/plain')

        return response
