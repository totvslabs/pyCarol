"""
Carol app funtionalities.


"""

import zipfile, io, os
from .utils.deprecation_msgs import _deprecation_msgs
from .utils.miscellaneous import zip_folder
import tempfile

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
                                      for i in query_list.get('mdmTenantAppSettingValues',{})})
            self.full_settings.update({i['mdmName']: i for i in query_list.get('mdmTenantAppSettingValues',{})})

        return self.app_settings

    def download_app(self, app_name=None, app_version=None, carolappname=None, carolappversion=None,
                     file_path='carol.zip', extract=False):
        """
        Download App artifacts.

        Args:

            app_name: `str` default `None`
                Carol app name. It will overwrite the app name used in Carol() initialization.
            app_version: `str`
                App Version
            carolappname: `str`
                App Name. Deprecated. Use app_name
            carolappversion: `str`
                App Version. Deprecated. Use app_version
            file_path:  `os.PathLike`
                Path to save the zip file.
            extract: `bool` default `False`
                Either extract the zip files or not.

        Returns: None

        """

        if carolappname is not None:
            app_name = carolappname
            _deprecation_msgs("`carolappname` is deprecated use `app_name`.")
        if carolappversion is not None:
            app_version = carolappversion
            _deprecation_msgs("`carolappversion` is deprecated use `app_version`.")

        if app_name is None:
            app_name = self.carol.app_name

        if app_version is None:
            raise ValueError('app_version must be set.')

        url = f'v1/carolApps/download/{app_name}/version/{app_version}'

        r = self.carol.call_api(url, method='GET', stream=True, downloadable=True)

        if extract:
            r = zipfile.ZipFile(io.BytesIO(r.content))
            r.extractall(file_path)
        else:
            with open(file_path, 'wb') as out:  ## Open temporary file as bytes
                out.write(io.BytesIO(r.content).read())

    def get_manifest(self, app_name=None):
        """

        Args: `str`

            app_name: `str` default `None`
                Carol app name. It will overwrite the app name used in Carol() initialization.

        Returns: `dict`
            Dictionary with the manifest file.

        """

        if app_name is None:
            app_name = self.carol.app_name

        url = f'v1/tenantApps/manifest/{app_name}'

        r = self.carol.call_api(url, method='GET')
        return r

    def edit_manifest(self, manifest, app_name=None):
        """

        Args:

            manifest: `dict`
                Dictionary with the manifest
            app_name: `str` default `None`
                Carol app name. It will overwrite the app name used in Carol() initialization.

        Returns: `dict`
            {"success": True}

        """

        if app_name is None:
            app_name = self.carol.app_name

        url = f'v1/tenantApps/manifest/{app_name}'

        r = self.carol.call_api(url, method='PUT', data=manifest)
        return r

    def get_git_process(self, app_name=None):
        """
        Get Git processes definid in the manifest file.

        Args:

            app_name: `str` default `None`
                Carol app name. It will overwrite the app name used in Carol() initialization.

        Returns: List of Dicts

        """

        if app_name is None:
            app_name = self.carol.app_name

        response = self.carol.call_api(path=f'v1/compute/{app_name}/getProcessesByGitRepo',
                                       method='POST')

        return response

    def build_docker_git(self, git_token, app_name=None, ):
        """
        Build App image using manifest definition.

        Args:
            git_token: `str`
                Git token to be used to pull the files.

            app_name: `str` default `None`
                Carol app name. It will overwrite the app name used in Carol() initialization.

        Returns:
            List of task ids.
        """

        if app_name is None:
            app_name = self.carol.app_name

        manifest = self.get_git_process(app_name)

        self._assert_manifest_fields(manifest)

        tasks = []
        for build in manifest:
            docker_name = build['dockerName']
            docker_tag = build['dockerTag']
            instance_type = build['instanceType']
            git_token = git_token
            params = {
                'dockerName': docker_name,
                'tagName': docker_tag,
                'gitToken': git_token,
                'instanceType': instance_type,
            }
            response = self.carol.call_api(path=f'v1/compute/{app_name}/buildGitDocker',
                                           method='POST', params=params)

            tasks.append(response)

        return tasks

    @staticmethod
    def _assert_manifest_fields(manifest):
        """
        Assert that the fields needed to build the image exist.
        Args:
            manifest: `list of dict`
                list of docker definition in the manifest file.

        Returns:
            None
        """

        fields = {'dockerName', 'dockerTag', 'instanceType'}
        for build in manifest:
            set_diff = fields - set(build)
            if len(set_diff) >= 1:
                raise ValueError(f'Missing docker definition {set_diff}')

    def update_setting_values(self, settings, app_name=None):
        """
        Change Settings values in Carol.

        Args:
            settings: `dict`
                dict with settings:
                {"param1": "value1", "param2": "value2"}
            app_name: `str` default None
                App name to change the settings.

        Returns: `dict`
            Carol Response.


        """

        if app_name is None:
            app_name = self.carol.app_name

        if not isinstance(settings, dict):
            ValueError(f"settings should be a dictionary,")

        app_id = self.get_by_name(app_name)['mdmId']
        settings_id = self._get_app_settings_config(app_id=app_id)['mdmId']
        data = [{"mdmName": i, "mdmParameterValue": j} for i, j in settings.items()]

        return self.carol.call_api(path=f'v1/tenantApps/{app_id}/settings/{settings_id}?publish=true', method='PUT',
                                   data=data)

    def _get_app_settings_config(self, app_name=None, app_id=None):

        if app_id is not None:
            pass
        elif app_name is None:
            app_name = self.carol.app_name
            app_id = self.get_by_name(app_name)['mdmId']

        return self.carol.call_api(path=f'v1/tenantApps/{app_id}/settings', )

    def start_app_process(self, process_name, app_name=None, ):

        """
        Start a carol process by process name.

        Args:
            process_name: `str`
                Process name.
            app_name: `str` default None
                App name to change the settings.

        Returns: `dict`
            task information.

        """

        if app_name is None:
            app_name = self.carol.app_name

        app_id = self.get_by_name(app_name)['mdmId']
        process_id = self.get_processes_info(app_name=app_name)["mdmId"]
        return self.carol.call_api(f"v1/tenantApps/{app_id}/aiprocesses/{process_id}/execute/{process_name}",
                                   method='POST')

    def get_processes_info(self, app_name=None, entity_space='WORKING', check_all_spaces=True):
        """
        Get app processes information.
        Args:
            app_name: `str` default None
                App name to change the settings.
            entity_space: `str` default `WORKING`
                WORKING or PRODUCTION
            check_all_spaces: `bool`
                Check all spaces.

        Returns:
            Process informations
        """

        params = {"entitySpace": entity_space, "checkAllSpaces": check_all_spaces}
        if app_name is None:
            app_name = self.carol.app_name
        app_id = self.get_by_name(app_name)['mdmId']
        return self.carol.call_api(f"v1/tenantApps/{app_id}/aiprocesses", method='GET', params=params)

    def get_subscribable_carol_apps(self):
        """
        Find all available apps to install in this env.

        Returns: `list`
            list of apps.

        """

        return self.carol.call_api("v1/tenantApps/subscribableCarolApps", method='GET')['hits']

    def install_carol_app(self, app_name=None, app_version=None, connector_group=None, publish=True):

        """
        Install a carol app in an env.

        Args:
            app_name: `str` default None
                App name to change the settings.
            app_version: `str` default None
                App version to install. If not specified, it will install the most recent.
            connector_group: `str` default None
                Connector Group to install.
            publish: `bool` default True
                If publish the update.

        Returns:
            Carol task.

        """

        if app_name is None:
            app_name = self.carol.app_name

        to_install = self.get_subscribable_carol_apps()
        to_install = [i for i in to_install if i["mdmName"] == app_name]


        if app_version == None:
            to_install = sorted(to_install, key=lambda x: x['mdmAppVersion'])
        else:
            to_install = [i for i in to_install if i["mdmAppVersion"] == app_version]

        if to_install:
            to_install = to_install[0]
        else:
            return

        to_install_id = to_install['mdmId']

        updated = self.carol.call_api(f"v1/tenantApps/subscribe/carolApps/{to_install_id}", method='POST')
        params = {"publish": publish, "connectorGroup": connector_group}
        return self.carol.call_api(f"v1/tenantApps/{updated['mdmId']}/install", method='POST', params=params)

    def get_app_details(self, app_name=None, entity_space='PRODUCTION'):
        """

        Find all information about an app. This will fetch information about connector groups, AI process, descriptions,
        data models, etc.

        Args:
            app_name: `str` default None
                App name to change the settings.
            entity_space: `str` default `PRODUCTION`
                WORKING or PRODUCTION

        Returns: `dict`
            Carol response.
        """

        if app_name is None:
            app_name = self.carol.app_name

        #check if it exists as a subscribable apps
        to_install = self.get_subscribable_carol_apps()
        to_install = [i for i in to_install if i["mdmName"] == app_name]

        if to_install:
            to_install = to_install[0]
            app_id = to_install['mdmId']
        else:
            # try installed app.
            app_id = self.get_by_name(app_name)['mdmCarolAppId']

        params = {"entitySpace": entity_space}
        return self.carol.call_api(f"v1/carolApps/{app_id}/details", method='GET', params=params)

    def upload_file(self, filepath,  app_name=None,):
        """
        Upload the app artifacts

        Args:
            filepath: `str`
                Folder path with artifacts (manifest.json or site)

            app_name: `str` default None
                App name

        Returns: `dict`
            Carol response.
        """

        if app_name is None:
            app_name = self.carol.app_name

        app_id = self.get_by_name(app_name)['mdmCarolAppId']

        if filepath.endswith(('.zip', '.json')):
            file = filepath
        elif os.path.isdir(filepath):
            file = zip_folder(filepath)
        uri = f'v1/carolApps/{app_id}/files/upload'

        with open(file, 'rb') as f:
            files = {'file': f}
            r = self.carol.call_api(path=uri, method='POST', files=files, content_type=None)

        return r




