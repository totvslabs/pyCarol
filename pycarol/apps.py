"""Carol app funtionalities."""
import io
from pathlib import Path
import typing as T
import zipfile

import requests

from . import exceptions
from .carol import Carol
from .utils.deprecation_msgs import _deprecation_msgs
from .utils.miscellaneous import zip_folder


class Apps:

    """Carol App instance.

    Args:
        carol: Carol() instance
    """

    def __init__(self, carol: Carol):
        self.carol = carol

        self.all_apps: T.Optional[T.Dict] = None
        self.app_settings: T.Optional[T.Dict] = None
        self.current_app: T.Optional[T.Dict] = None
        self.current_app_id: T.Optional[str] = None
        self.current_app_name: T.Optional[str] = None
        self.full_settings: T.Optional[T.Dict] = None
        self.total_hits: T.Optional[int] = None

    def _build_query_params(
        self,
        entity_space="PRODUCTION",
        offset=0,
        page_size=100,
        sort_by=None,
        sort_order=None,
    ) -> T.Dict[str, T.Any]:
        if sort_by is None:
            return {
                "offset": offset,
                "pageSize": page_size,
                "entitySpace": entity_space,
            }
        return {
            "offset": offset,
            "pageSize": page_size,
            "sortOrder": sort_order,
            "sortBy": sort_by,
            "entitySpace": entity_space,
        }

    def _define_current_run(self, query: T.Dict) -> None:
        self.current_app_id = query.get("mdmId")
        self.current_app_name = query.get("mdmName")
        self.current_app = {self.current_app_name: query}

    def all(
        self,
        entity_space: str = "PRODUCTION",
        page_size: int = 50,
        offset: int = 0,
        sort_order: str = "ASC",
        sort_by: T.Optional[str] = None,
    ) -> T.Dict:
        """Get all app information in this environment.

        Args:
            entity_space: Space to get the app from. Possible values:

                    1. PRODUCTION: For production.
                    2. WORKING: For Draft apps.

            offset: Offset for pagination. Only used when `scrollable=False`.
            page_size: Number of records downloaded in each pagination. The maximum
                value is 1000.
            sort_order: Sort ascending ('ASC') vs. descending ('DESC').
            sort_by: Name to sort by.

        Returns:
            All apps json definition

        Raises:
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        query_string = self._build_query_params(
            offset=offset,
            page_size=page_size,
            entity_space=entity_space,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        query = self.carol.call_api("v1/tenantApps", method="GET", params=query_string)
        if not isinstance(query, dict):
            raise exceptions.NotMapAsCallResponseException

        self.all_apps = {app["mdmName"]: app for app in query["hits"]}
        self.total_hits = query["totalHits"]
        return self.all_apps

    def get_by_name(self, app_name: str, entity_space: str = "PRODUCTION") -> T.Dict:
        """Get app information by app name.

        Args:
            app_name: App name.
            entity_space: Space to get the app information from. Possible values

                    1. PRODUCTION: For production.
                    2. WORKING: For Draft apps.

        Returns:
            App info json definition.

        Raises:
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        query_string = {"entitySpace": entity_space}
        url = f"v1/tenantApps/name/{app_name}"
        query = self.carol.call_api(url, method="GET", params=query_string)
        if not isinstance(query, dict):
            raise exceptions.NotMapAsCallResponseException

        self._define_current_run(query)

        return query

    def get_by_id(self, app_id: str, entity_space: str = "PRODUCTION") -> T.Dict:
        """Get app information by ID.

        Args:
            app_id: App id.
            entity_space: Space to get the app information from. Possible values

                    1. PRODUCTION: For production.
                    2. WORKING: For Draft apps.

        Returns:
            App info json definition.

        Raises:
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        query_string = {"entitySpace": entity_space}
        url = f"v1/tenantApps/{app_id}"
        query = self.carol.call_api(url, method="GET", params=query_string)
        if not isinstance(query, dict):
            raise exceptions.NotMapAsCallResponseException

        self._define_current_run(query)

        return query

    def get_settings(
        self,
        app_name: T.Optional[str] = None,
        app_id: T.Optional[str] = None,
        entity_space: str = "PRODUCTION",
        check_all_spaces: bool = False,
    ) -> T.Dict:
        """Get settings from app.

        Settings are the settings available in Carol's UI.

        Args:
            app_name: App name, if None will get from `app_id`.
            app_id: App id. Either app_name or app_id must be set.
            entity_space: Space to get the app settings from. Possible values

                    1. PRODUCTION: For production.
                    2. WORKING: For Draft apps.

            check_all_spaces: Check all entity spaces.

        Returns:
            Settings.

        Raises:
            Exception if carol.app_name or parameter app_name are both None.
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        assert app_name or app_id or self.carol.app_name

        if app_id is not None:
            self.get_by_id(app_id, entity_space)
        elif app_name is not None:
            self.get_by_name(app_name, entity_space)
        elif self.carol.app_name is not None:
            self.get_by_name(self.carol.app_name, entity_space)
        else:
            raise Exception("carol.app_name is None.")

        query_string = {"entitySpace": entity_space, "checkAllSpaces": check_all_spaces}

        query = self.carol.call_api(
            f"v1/tenantApps/{self.current_app_id}/settings",
            method="GET",
            params=query_string,
        )
        if not isinstance(query, dict):
            raise exceptions.NotMapAsCallResponseException

        self.app_settings = {}
        self.full_settings = {}

        if not isinstance(query, list):
            query = [query]

        for query_el in query:
            self.app_settings.update(
                {
                    i["mdmName"]: i.get("mdmParameterValue")
                    for i in query_el.get("mdmTenantAppSettingValues", {})
                }
            )
            self.full_settings.update(
                {i["mdmName"]: i for i in query_el.get("mdmTenantAppSettingValues", {})}
            )

        return self.app_settings

    def download_app(
        self,
        app_version: str,
        app_name: T.Optional[str] = None,
        carolappname: T.Optional[str] = None,
        carolappversion: T.Optional[str] = None,
        file_path: str = "carol.zip",
        extract: bool = False,
    ) -> None:
        """Download App artifacts.

        Args:
            app_name: Carol app name. It will overwrite the app name used in Carol()
                initialization.
            app_version: App Version
            carolappname: App Name. Deprecated. Use app_name
            carolappversion: App Version. Deprecated. Use app_version
            file_path: Path to save the zip file.
            extract: Either extract the zip files or not.

        Raises:
            ValueError when app_version is not set.
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
            raise ValueError("app_version must be set.")

        url = f"v1/carolApps/download/{app_name}/version/{app_version}"

        response = self.carol.call_api(
            url, method="GET", stream=True, downloadable=True
        )
        if not isinstance(response, requests.Response):
            raise exceptions.NotResponseAsCallResponseException

        if extract is True:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                zip_file.extractall(file_path)
        else:
            with open(file_path, "wb") as out:  # Open temporary file as bytes
                out.write(io.BytesIO(response.content).read())

    def get_manifest(self, app_name: T.Optional[str] = None) -> T.Dict:
        """Get manifest file.

        Args:
            app_name: Carol app name. It will overwrite the app name used in Carol()
                initialization.

        Returns:
            Manifest file.

        Raises:
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        if app_name is None:
            app_name = self.carol.app_name

        url = f"v1/tenantApps/manifest/{app_name}"

        response = self.carol.call_api(url, method="GET")
        if not isinstance(response, dict):
            raise exceptions.NotMapAsCallResponseException

        return response

    def edit_manifest(
        self, manifest: T.Dict, app_name: T.Optional[str] = None
    ) -> T.Dict:
        """Edit manifest file.

        Args:
            manifest: Dictionary with the manifest
            app_name: Carol app name. It will overwrite the app name used in Carol()
                initialization.

        Returns:
            Carol API response.

        Raises:
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        if app_name is None:
            app_name = self.carol.app_name

        url = f"v1/tenantApps/manifest/{app_name}"

        response = self.carol.call_api(url, method="PUT", data=manifest)
        if not isinstance(response, dict):
            raise exceptions.NotMapAsCallResponseException

        return response

    def get_git_process(self, app_name: T.Optional[str] = None) -> T.List[T.Dict]:
        """Get Git processes defined in the manifest file.

        Args:
            app_name: Carol app name. It will overwrite the app name used in Carol()
                initialization.

        Returns:
            List of Dicts.

        Raises:
            NotListAsCallResponseException if call_api return is not a list.
        """
        if app_name is None:
            app_name = self.carol.app_name

        url = f"v1/compute/{app_name}/getProcessesByGitRepo"
        response = self.carol.call_api(url, method="POST")
        if not isinstance(response, list):
            raise exceptions.NotListAsCallResponseException

        return response

    def build_docker_git(
        self,
        git_token: str,
        app_name: T.Optional[str] = None,
    ) -> T.List:
        """Build all images listed in the manifest definition.

        Args:
            git_token: Git token to be used to pull the files.
            app_name: Carol app name. It will overwrite the app name used in Carol()
                initialization.

        Returns:
            List of task ids.
        """
        if app_name is None:
            app_name = self.carol.app_name

        manifest = self.get_git_process(app_name)

        self._assert_manifest_fields(manifest)

        tasks = []
        url = f"v1/compute/{app_name}/buildGitDocker"
        for build in manifest:
            docker_name = build["dockerName"]
            docker_tag = build["dockerTag"]
            instance_type = build["instanceType"]
            params = {
                "dockerName": docker_name,
                "tagName": docker_tag,
                "gitToken": git_token,
                "instanceType": instance_type,
            }
            response = self.carol.call_api(url, method="POST", params=params)

            tasks.append(response)

        return tasks

    @staticmethod
    def _assert_manifest_fields(manifest: T.List[T.Dict]) -> None:
        """Assert that the fields needed to build the image exist.

        Args:
            manifest: list of docker definition in the manifest file.

        Raises:
            ValueError when docker definition is missing.

        Raises:
            ValueError when docker definition is missing.
        """
        fields = {"dockerName", "dockerTag", "instanceType"}
        for build in manifest:
            set_diff = fields - set(build)
            if len(set_diff) >= 1:
                raise ValueError(f"Missing docker definition {set_diff}")

    def update_setting_values(
        self, settings: T.Dict, app_name: T.Optional[str] = None
    ) -> T.Dict:
        """Change Settings values in Carol.

        Args:
            settings: dict with settings: {"param1": "value1", "param2": "value2"}
            app_name: App name to change the settings.

        Returns:
            Carol Response.

        Raises:
            ValueError if settings type is not a dictionary.
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        app_name = app_name or self.carol.app_name
        if app_name is None:
            raise Exception("carol.app_name is None.")

        if not isinstance(settings, dict):
            ValueError("settings should be a dictionary.")

        app_id = self.get_by_name(app_name)["mdmId"]
        settings_id = self._get_app_settings_config(app_id=app_id)["mdmId"]
        data = [{"mdmName": i, "mdmParameterValue": j} for i, j in settings.items()]

        url = f"v1/tenantApps/{app_id}/settings/{settings_id}?publish=true"
        response = self.carol.call_api(url, method="PUT", data=data)
        if not isinstance(response, dict):
            raise exceptions.NotMapAsCallResponseException

        return response

    def _get_app_settings_config(
        self, app_name: T.Optional[str] = None, app_id: T.Optional[str] = None
    ) -> T.Dict:
        if app_id is None:
            app_name = app_name or self.carol.app_name
            if app_name is None:
                raise Exception("carol.app_name is None.")

            app_id = self.get_by_name(app_name)["mdmId"]

        response = self.carol.call_api(path=f"v1/tenantApps/{app_id}/settings")
        if not isinstance(response, dict):
            raise exceptions.NotMapAsCallResponseException

        return response

    def start_app_process(
        self,
        process_name: str,
        app_name: T.Optional[str] = None,
    ) -> T.Dict:
        """Start a carol process by process name.

        Args:
            process_name: Process name.
            app_name: App name to change the settings.

        Returns:
            task information.

        Raises:
            Exception if carol.app_name or parameter app_name are both None.
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        app_name = app_name or self.carol.app_name
        if app_name is None:
            raise Exception("carol.app_name is None.")

        app_id = self.get_by_name(app_name)["mdmId"]
        process_id = self.get_processes_info(app_name=app_name)["mdmId"]
        url = f"v1/tenantApps/{app_id}/aiprocesses/{process_id}/execute/{process_name}"
        response = self.carol.call_api(url, method="POST")
        if not isinstance(response, dict):
            raise exceptions.NotMapAsCallResponseException

        return response

    def get_processes_info(
        self,
        app_name: T.Optional[str] = None,
        entity_space: str = "WORKING",
        check_all_spaces: bool = True,
    ) -> T.Dict:
        """Get app processes information.

        Args:
            app_name: App name to change the settings.
            entity_space: WORKING or PRODUCTION
            check_all_spaces: Check all spaces.

        Returns:
            Process informations

        Raises:
            Exception if carol.app_name or parameter app_name are both None.
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        params = {"entitySpace": entity_space, "checkAllSpaces": check_all_spaces}
        app_name = app_name or self.carol.app_name
        if app_name is None:
            raise Exception("carol.app_name is None.")
        app_id = self.get_by_name(app_name)["mdmId"]
        url = f"v1/tenantApps/{app_id}/aiprocesses"
        response = self.carol.call_api(url, method="GET", params=params)
        if not isinstance(response, dict):
            raise exceptions.NotMapAsCallResponseException

        return response

    def get_subscribable_carol_apps(self) -> T.List:
        """Find all available apps to install in this env.

        Returns:
            list of apps.

        Raises:
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        url = "v1/tenantApps/subscribableCarolApps"
        response = self.carol.call_api(url, method="GET")
        if not isinstance(response, dict):
            raise exceptions.NotMapAsCallResponseException

        return response["hits"]

    def install_carol_app(
        self,
        app_name: str = None,
        app_version: str = None,
        connector_group: str = None,
        publish: bool = True,
    ) -> T.Optional[T.Dict]:
        """Install a carol app in an env.

        Args:
            app_name:  App name to change the settings.
            app_version: App version to install. If not specified, it will install the
                most recent.
            connector_group: Connector Group to install.
            publish: If publish the update.

        Returns:
            Carol task.

        Raises:
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        if app_name is None:
            app_name = self.carol.app_name

        to_install = self.get_subscribable_carol_apps()
        to_install = [i for i in to_install if i["mdmName"] == app_name]

        if app_version is None:
            to_install = sorted(to_install, key=lambda x: x["mdmAppVersion"])
        else:
            to_install = [i for i in to_install if i["mdmAppVersion"] == app_version]

        if len(to_install) > 0:
            to_install_first = to_install[0]
        else:
            return None

        to_install_id = to_install_first["mdmId"]

        url = f"v1/tenantApps/subscribe/carolApps/{to_install_id}"
        updated = self.carol.call_api(url, method="POST")
        if not isinstance(updated, dict):
            raise exceptions.NotMapAsCallResponseException

        params = {"publish": publish, "connectorGroup": connector_group}
        url = f"v1/tenantApps/{updated['mdmId']}/install"
        response = self.carol.call_api(url, method="POST", params=params)
        if not isinstance(response, dict):
            raise exceptions.NotMapAsCallResponseException
        return response

    def get_app_details(
        self, app_name: str = None, entity_space: str = "PRODUCTION"
    ) -> T.Dict:
        """Find all information about an app.

        This will fetch information about connector groups, AI process, descriptions,
        data models, etc.

        Args:
            app_name: App name to change the settings.
            entity_space: WORKING or PRODUCTION

        Returns:
            Carol response.

        Raises:
            Exception if carol.app_name or parameter app_name are both None.
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        app_name = app_name or self.carol.app_name
        if app_name is None:
            raise Exception("carol.app_name is None.")

        # check if it exists as a subscribable apps
        to_install = self.get_subscribable_carol_apps()
        to_install = [i for i in to_install if i["mdmName"] == app_name]

        if len(to_install) > 0:
            to_install_first = to_install[0]
            app_id = to_install_first["mdmId"]
        else:
            # try installed app.
            app_id = self.get_by_name(app_name)["mdmCarolAppId"]

        params = {"entitySpace": entity_space}
        url = f"v1/carolApps/{app_id}/details"
        response = self.carol.call_api(url, method="GET", params=params)
        if not isinstance(response, dict):
            raise exceptions.NotMapAsCallResponseException

        return response

    def upload_file(
        self,
        filepath: str,
        app_name: str = None,
    ) -> T.Dict:
        """Upload the app artifacts.

        Args:
            filepath: Folder path with artifacts (manifest.json or site)
            app_name: App name

        Returns:
            Carol response.

        Raises:
            Exception if carol.app_name or parameter app_name are both None.
            NotMapAsCallResponseException if call_api return is not a dict.
        """
        app_name = app_name or self.carol.app_name
        if app_name is None:
            raise Exception("carol.app_name is None.")

        app_id = self.get_by_name(app_name)["mdmCarolAppId"]

        if Path(filepath).is_dir():
            filepath = zip_folder(filepath)

        if not filepath.endswith((".zip", ".json")):
            raise Exception("File must be a .zip or .json.")

        with open(filepath, "rb") as file_:
            files = {"file": file_}

        url = f"v1/carolApps/{app_id}/files/upload"
        response = self.carol.call_api(
            url, method="POST", files=files, content_type=None
        )
        if not isinstance(response, dict):
            raise exceptions.NotMapAsCallResponseException

        return response
