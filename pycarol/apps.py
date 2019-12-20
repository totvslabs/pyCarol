import zipfile, io


class Apps:
    def __init__(self, carol):
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
        '''
        :param entity_space:
        :param page_size:
        :param offset:
        :param sort_order:
        :param sort_by:
        :return:
        '''

        query_string = self._build_query_params(offset=offset, page_size=page_size, entity_space=entity_space,
                                                sort_by=sort_by, sort_order=sort_order)
        query = self.carol.call_api('v1/tenantApps', method='GET', params=query_string)

        self.all_apps = {app['mdmName']: app for app in query['hits']}
        self.total_hits = query["totalHits"]
        return self.all_apps

    def get_by_name(self, app_name, entity_space='PRODUCTION'):
        '''

        :param app_name:
        :param entity_space:
        :return:
        '''
        query_string = {"entitySpace": entity_space}
        query = self.carol.call_api(f'v1/tenantApps/name/{app_name}', method='GET', params=query_string)

        self._define_current_run(query)

        return query

    def get_by_id(self, app_id, entity_space='PRODUCTION'):
        '''

        :param app_id:
        :param entity_space:
        :return:
        '''
        query_string = {"entitySpace": entity_space}
        query = self.carol.call_api(f'v1/tenantApps/{app_id}', method='GET', params=query_string)

        self._define_current_run(query)

        return query

    def get_settings(self, app_name=None, app_id=None, entity_space='PRODUCTION', check_all_spaces=False):
        '''

        :param app_name:
        :param app_id:
        :param entity_space:
        :param check_all_spaces:
        :return:
        '''
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
