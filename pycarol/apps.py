"""
Carol app funtionalities.


"""


import zipfile, io


class Apps:
    """
    Carol App instance.

        Args:

            carol: `pycarol.Carol`
                Carol() instance


    """
    def __init__(self, carol):
        """
        Initialize Class

        Args:

            carol: `pycarol.Carol`
                Carol() instance
        """
        self.carol = carol

    def _build_query_params(self, entity_space="PRODUCTION", offset=0, page_size=100, sort_by=None, sort_order=None):
        if sort_by is None:
            return {"offset": offset, "pageSize": page_size, "entitySpace": entity_space}
        else:
            return {"offset": offset, "pageSize": page_size, "sortOrder": sort_order,
                    "sortBy": sort_by, "entitySpace": entity_space}

    def _define_current_run(self, query):
        self.current_app_id = query.get('mdmId')
        self.current_app_name = query.get('mdmName')
        self.current_app = {self.current_app_name: query}

    def all(self, entity_space='PRODUCTION', page_size=50, offset=0, sort_order='ASC', sort_by=None):
        """
        Get all app information in this environment.

        Args:

            entity_space: `str` default `PRODUCTION`
                Space to get the app from. Possible values

                    1. PRODUCTION: For production
                    2. WORKING: For Draft apps.

        offset: `int`, default 0
            Offset for pagination. Only used when `scrollable=False`
        page_size: `int`, default 100
            Number of records downloaded in each pagination. The maximum value is 1000
        sort_order: `str`, default 'ASC'
            Sort ascending ('ASC') vs. descending ('DESC').
        sort_by: `str`,  default `None`
            Name to sort by.

        Returns: List of Dicts.
            All apps json definition

        """


        query_string = self._build_query_params(offset=offset, page_size=page_size, entity_space=entity_space,
                                                sort_by=sort_by, sort_order=sort_order)
        query = self.carol.call_api('v1/tenantApps', method='GET', params=query_string)

        self.all_apps = {app['mdmName']: app for app in query['hits']}
        self.total_hits = query["totalHits"]
        return self.all_apps

    def get_by_name(self, app_name, entity_space='PRODUCTION'):
        """
        Get app information by app name

        Args:

            app_name: `str`
                App name.
            entity_space: `str` default `PRODUCTION`
                Space to get the app information from. Possible values

                    1. PRODUCTION: For production
                    2. WORKING: For Draft apps.

        Returns: `dict`
            App info json definition.

        """

        query_string = {"entitySpace": entity_space}
        query = self.carol.call_api(f'v1/tenantApps/name/{app_name}', method='GET', params=query_string)

        self._define_current_run(query)

        return query

    def get_by_id(self, app_id, entity_space='PRODUCTION'):
        """
        Get app information by ID

        Args:

            app_id: `str`
                App id.
            entity_space: `str` default `PRODUCTION`
                Space to get the app information from. Possible values

                    1. PRODUCTION: For production
                    2. WORKING: For Draft apps.

        Returns: `dict`
            App info json definition.

        """

        query_string = {"entitySpace": entity_space}
        query = self.carol.call_api(f'v1/tenantApps/{app_id}', method='GET', params=query_string)

        self._define_current_run(query)

        return query

    def get_settings(self, app_name=None, app_id=None, entity_space='PRODUCTION', check_all_spaces=False):
        """
        Get settings from app

        Settings are the settings available in Carol's UI.

        Args:

            app_name: `str` default `None`
                App name, if None will get from  `app_id`
            app_id: `str` default `None`
                App id. Either app_name or app_id must be set.
            entity_space: `str` default `PRODUCTION`
                Space to get the app settings from. Possible values

                    1. PRODUCTION: For production
                    2. WORKING: For Draft apps.

            check_all_spaces: `bool` default `False`
                Check all entity spaces.

        Returns: `dict`
            Dictionary with the settings
        """

        assert app_name or app_id or self.carol.app_name

        if app_id is not None:
            self.get_by_id(app_id, entity_space)
        elif app_name is not None:
            self.get_by_name(app_name, entity_space)
        else:
            self.get_by_name(self.carol.app_name, entity_space)

        query_string = {"entitySpace": entity_space, "checkAllSpaces": check_all_spaces}

        query = self.carol.call_api(f'v1/tenantApps/{self.current_app_id}/settings', method='GET',
                                    params=query_string)

        self.app_settings = {}
        self.full_settings = {}

        if not isinstance(query, list):
            query = [query]
        for query_list in query:
            self.app_settings.update({i['mdmName']: i.get('mdmParameterValue')
                                      for i in query_list.get('mdmTenantAppSettingValues')})
            self.full_settings.update({i['mdmName']: i for i in query_list.get('mdmTenantAppSettingValues')})

        return self.app_settings

    def download_app(self, carolappname, carolappversion, file_path, extract=False):
        """
        Download App artifacts.

        Args:

            carolappname: `str`
                App Name.
            carolappversion: `str`
                App Version
            file_path:  `os.PathLike`
                Path to save the zip file.
            extract: `bool` default `False`
                Either extract the zip files or not.

        Returns: None

        """

        url = f'v1/carolApps/download/{carolappname}/version/{carolappversion}'

        r = self.carol.call_api(url, method='GET', stream=True, downloadable=True)

        if extract:
            r = zipfile.ZipFile(io.BytesIO(r.content))
            r.extractall(file_path)
        else:
            with open(file_path, 'wb') as out:  ## Open temporary file as bytes
                out.write(io.BytesIO(r.content).read())

    def get_manifest(self, app_name):
        """

        Args: `str`
            app_name: Carol app name

        Returns: `dict`
            Dictionary with the manifest file.

        """

        url = f'v1/tenantApps/manifest/{app_name}'

        r = self.carol.call_api(url, method='GET')
        return r

    def edit_manifest(self, app_name, manifest):
        """

        Args:
            app_name: `str`
                Carol app name
            manifest: `dict`
                Dictionary with the manifest

        Returns: `dict`
            {"success": True}

        """

        url = f'v1/tenantApps/manifest/{app_name}'

        r = self.carol.call_api(url, method='PUT', data=manifest)
        return r
