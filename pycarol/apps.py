import json


# Port this to the new architecture!
class Apps:
    def __init__(self, carol):
        self.carol = carol

        self.offset = 0
        self.page_size = 100
        self.sort_order = 'ASC'
        self.entity_space = 'PRODUCTION'
        self.sort_by = None
        self.current_app = None
        self.current_app = None
        self.current_app_name = None

        self._build_query_params()

    def _build_query_params(self):
        if self.sort_by is None:
            self.querystring = {"offset": self.offset, "pageSize": self.page_size, "sortOrder": self.sort_order,
                                "entitySpace": self.entity_space}
        else:
            self.querystring = {"offset": self.offset, "pageSize": self.page_size, "sortOrder": self.sort_order,
                                "sortBy": self.sort_by, "entitySpace": self.entity_space}

    def _define_current_run(self, query):
        self.current_app_id = query.get('mdmId')
        self.current_app = {self.app_name: query}
        self.current_app_name = query.get('mdmName')

    def all(self, entity_space='PRODUCTION', page_size=50, offset=0, sort_order='ASC', sort_by=None):
        '''
        :param entity_space:
        :param page_size:
        :param offset:
        :param sort_order:
        :param sort_by:
        :return:
        '''
        self.offset = offset
        self.page_size = page_size
        self.sort_order = sort_order
        self.entity_spacepace = entity_space
        self.sort_by = sort_by


        self._build_query_params()
        query=  self.carol.call_api('v1/tenantApps', method='GET', params=self.querystring)

        self.all_apps = {app['mdmName']: app for app in query['hits']}
        self.total_hits = query["totalHits"]
        return self.all_apps

    def get_by_name(self, app_name, entity_space='PRODUCTION'):
        '''
        :param app_name:
        :param entity_space:
        :return:
        '''

        self.app_name = app_name
        self.entity_space = entity_space
        self.querystring = {"entitySpace": entity_space}

        query = self.carol.call_api(f'v1/tenantApps/name/{self.app_name}', method='GET', params=self.querystring)

        self._define_current_run(query)

        return self.current_app

    def get_by_id(self, app_id, entity_space='PRODUCTION'):
        '''
        :param app_id:
        :param entity_space:
        :return:
        '''
        self.app_id = app_id
        self.entity_space = entity_space
        self.querystring = {"entitySpace": self.entity_space}

        query = self.carol.call_api(f'v1/tenantApps/{self.app_id}', method='GET', params=self.querystring)

        self._define_current_run(query)

        return self.current_app

    def get_settings(self, app_name=None, app_id=None, entity_space='PRODUCTION', check_all_spaces=False):
        '''
        :param app_name:
        :param app_id:
        :param entity_space:
        :param check_all_spaces:
        :return:
        '''
        assert app_name or app_id
        self.app_id = app_id
        self.app_name = app_name
        self.entity_space = entity_space
        self.check_all_spaces = check_all_spaces

        if self.app_id is not None:
            self.get_by_id(self.app_id, self.entity_space)
        else:
            self.get_by_name(self.app_name, self.entity_space)

        self.querystring = {"entitySpace": self.entity_space, "checkAllSpaces": self.check_all_spaces}

        query = self.carol.call_api(f'v1/tenantApps/{self.current_app_id}/settings', method='GET',
                                    params=self.querystring)

        self.app_settings = {}
        self.full_settings = {}

        if not isinstance(query, list):
            query = [query]
        for query_list in query:
            self.app_settings.update({i['mdmName']: i.get('mdmParameterValue')
                                     for i in query_list.get('mdmTenantAppSettingValues')})
            self.full_settings.update({i['mdmName']: i for i in query_list.get('mdmTenantAppSettingValues')})

        return self.app_settings
