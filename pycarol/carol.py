import copy
from pathlib import Path
import json
import os
import typing as T
import warnings

from dotenv import load_dotenv
import requests
from urllib3.util.retry import Retry

from . import __CONNECTOR_PYCAROL__, __version__
from . import exceptions
from .auth.ApiKeyAuth import ApiKeyAuth
from .auth.PwdAuth import PwdAuth
from .organization import Organization
from .tenant import Tenant


ResponseType = T.Union[requests.Response, T.Dict[str, T.Any], T.List]


class SwitchContext:
    def __init__(
        self,
        parent_context,
        env_name=None,
        env_id=None,
        org_id=None,
        org_name=None,
        app_name=None,
    ):
        self.parent_context = parent_context
        self.env_name = env_name
        self.env_id = env_id
        self.app_name = app_name
        self.org_id = org_id
        self.org_name = org_name

    def __enter__(self):
        self.parent_context.switch_environment(
            env_name=self.env_name,
            env_id=self.env_id,
            app_name=self.app_name,
            org_name=self.org_name,
            org_id=self.org_id,
        )
        return self.parent_context

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self.parent_context


class Carol:

    """Handles all Carol`s API calls and log user in Carol.

    In case all parameters are `None`, pycarol will try yo find their values in the
    environment variables.
    The values are:

        1. `CAROLTENANT` for domain
        2. `CAROLAPPNAME` for app_name
        3. `CAROLAPPOAUTH` for auth
        4. `CAROLORGANIZATION` for organization
        5. `CAROLCONNECTORID` for connector_id
        6. `CAROL_DOMAIN` for environment
        7. `CAROLUSER` for carol user email
        8. `CAROLPWD` for user password.

    Args:
        domain: Tenant name. e.x., domain.carol.ai.
        app_name: Carol app name.
        auth: Auth Carol object to handle authentication
        connector_id: Connector Id
        port: Port to be used (when running locally it could change)
        verbose: If True will print the header, method and URL of each API call.
        organization: Organization domain.
        environment: Which Carol's environment to use.
            There are three possible values today.

                1. 'carol.ai' for the production environment
                2. 'karol.ai' for the explore environment
                3. 'qarol.ai' for the QA environment
        host: This will overwrite the host used. Today the host is:

                1. if organization is None, host={domain}.{environment}
                2. else host={organization}.{environment}

            See Carol._set_host.
        user: User
        password:User passowrd
        api_key: Carol's Api Key
        org_level: If True, will log-in at organization level.
        dotenv_path: Path to dotenv file, if loading is required.

    Raises:
        MissingInfoCarolException if there is any mandatory parameter missing
    """

    def __init__(
        self,
        domain: T.Optional[str] = None,
        app_name: T.Optional[str] = None,
        auth: T.Optional[T.Union[ApiKeyAuth, PwdAuth]] = None,
        connector_id: T.Optional[str] = None,
        port: int = 443,
        verbose: bool = False,
        organization: T.Optional[str] = None,
        environment: T.Optional[str] = None,
        host: T.Optional[str] = None,
        user: T.Optional[str] = None,
        password: T.Optional[str] = None,
        api_key: T.Optional[str] = None,
        org_level: bool = False,
        dotenv_path: T.Optional[T.Union[str, Path]] = None,
    ):
        if dotenv_path is not None:
            dotenv_path = Path(dotenv_path)
            if not dotenv_path.exists():
                msg = f"Invalid path to dotenv: {dotenv_path}"
                raise exceptions.MissingInfoCarolException(msg)
            load_dotenv(str(dotenv_path))

        auth = _prepare_auth(api_key, auth, connector_id, password, user)

        domain = domain or os.getenv("CAROLTENANT") or None
        if (domain is None) and (not org_level):
            raise exceptions.MissingInfoCarolException("`domain` must be set.")

        if org_level is True:
            domain = None

        app_name = app_name or os.getenv("CAROLAPPNAME", " ")
        environment = environment or os.getenv("CAROL_DOMAIN") or "carol.ai"
        organization = organization or os.getenv("CAROLORGANIZATION") or None

        if (domain is None and not org_level) or app_name is None or auth is None:
            raise exceptions.MissingInfoCarolException(
                "domain, app_name and auth must be specified as parameters, either "
                "in the environment variables CAROLTENANT, CAROLAPPNAME,"
                "  CAROLAPPOAUTH or CAROLUSER+CAROLPWD and  CAROLCONNECTORID"
            )

        if environment is None and os.getenv("ENV_DOMAIN") is not None:
            raise exceptions.DeprecatedEnvVarException("ENV_DOMAIN", "CAROL_DOMAIN")

        if organization is None and os.getenv("API_SUBDOMAIN") is not None:
            raise exceptions.DeprecatedEnvVarException(
                "API_SUBDOMAIN", "CAROLORGANIZATION"
            )

        self._current_user: T.Optional[ResponseType] = None
        self._host_string = host
        self._is_org_level = org_level
        self._tenant: T.Optional[ResponseType] = None
        self._user_agent = f"pyCarol/{__version__}"
        self.app_name: T.Optional[str] = app_name
        self.auth = auth
        self.connector_id = auth.connector_id
        self.domain = domain
        self.environment = environment
        self.host = _set_host(self.environment, domain, host, organization)
        self.org = None
        self.organization = organization
        self.port = port
        self.response: T.Optional[ResponseType] = None
        self.session: T.Optional[requests.Session] = None
        self.verbose = verbose

        self.auth.login(self)

    @property
    def current_user(self) -> ResponseType:
        """Return the current user.

        Returns:
            Carol's API response payload.
        """
        endpoint = (
            "v2/orgUsers/current" if self._is_org_level is True else "v2/users/current"
        )
        if self._current_user is None:
            self._current_user = self.call_api(endpoint)
        return self._current_user

    @property
    def tenant(self) -> T.Optional[ResponseType]:
        """Return the current tenant.

        Returns:
            Carol's API response payload.
        """
        if self._tenant is None and not self._is_org_level:
            self._tenant = Tenant(self).get_tenant_by_domain(self.domain)
        return self._tenant

    def call_api(
        self,
        path: str,
        method: T.Optional[str] = None,
        data=None,
        auth: bool = True,
        params=None,
        content_type: T.Optional[str] = "application/json",
        retries: int = 8,
        session: T.Optional[requests.Session] = None,
        backoff_factor: float = 0.5,
        status_forcelist: T.Tuple[int, ...] = (502, 503, 504, 524),
        downloadable: bool = False,
        method_whitelist: T.FrozenSet[str] = frozenset(
            ["HEAD", "TRACE", "GET", "PUT", "OPTIONS", "DELETE", "POST"]
        ),
        errors: str = "raise",
        extra_headers: T.Optional[T.Dict] = None,
        files: T.Optional[T.Dict] = None,
        prefix_path: str = "/api/",
        **kwds,
    ) -> ResponseType:
        """Handle all the API calls.

        Args:
            path: API URI path. e.x.  v2/staging/schema.
            method: Set of uppercased HTTP method verbs that we should call on.
            data: Same type as :class: `Session.request.data`
                Object to send in the body of the request.
            auth: If API call should be authenticated
            params: in the query string for the :class:`requests.Request`.
            content_type: Content type for the api call
            retries: Number of retries for the API calls
            session: It allows you to persist certain parameters across requests.
            backoff_factor: Backoff factor to apply between  attempts. It will sleep
                for: {backoff factor} * (2 ^ ({retries} - 1)) seconds
            status_forcelist: A set of integer HTTP status codes that we should force a
                retry on. A retry is initiated if the request method is in
                method_whitelist and the response status code is in status_forcelist.
            downloadable: If the request will return a file to download.
            method_whitelist: Set of uppercased HTTP method verbs that we should retry
                on.
            errors: {‘ignore’, ‘raise’}, default ‘raise’
                If ‘raise’, then invalid request will raise an exception If ‘ignore’,
                then invalid request will return the request response
            extra_headers: extra headers to be sent.
            files: Used when uploading files to carol. This will be sent to
                :class: `requests.request`
            prefix_path: Prefix path to be used to create the final url
                'https://{self.host}:{self.port}{prefix_path}{path}.
            kwds: `dict` default `None`
                Extra parameters to be sent to :class: `requests.request`

        Returns:
            Dict with API response.
        """
        if session is not None:
            self.session = session

        extra_headers = extra_headers or {}
        url = f"https://{self.host}:{self.port}{prefix_path}{path}"

        if method is None:
            method = "GET" if data is None else "POST"

        met_list = [
            "HEAD",
            "TRACE",
            "GET",
            "PUT",
            "POST",
            "OPTIONS",
            "PATCH",
            "DELETE",
            "CONNECT",
        ]
        assert method in met_list, f"API method must be {met_list}"

        headers = {"accept": "application/json"}
        if auth:
            self.auth.authenticate_request(headers)

        data_json = None
        if method == "GET":
            pass

        elif method in ("POST", "DELETE", "PUT"):
            if content_type is not None:
                headers["content-type"] = content_type

            if content_type == "application/json":
                data_json = data
                data = None

        headers.update(extra_headers)
        headers.update({"User-Agent": self._user_agent})

        __count = 0
        while True:
            self.session = _retry_session(
                retries=retries,
                session=self.session,
                backoff_factor=backoff_factor,
                status_forcelist=status_forcelist,
                method_whitelist=method_whitelist,
            )

            response = self.session.request(
                method=method,
                url=url,
                data=data,
                json=data_json,
                headers=headers,
                params=params,
                files=files,
                **kwds,
            )

            if self.verbose:
                data_ = data_json if data_json is not None else data
                print(f"Calling {method} {url}. Payload: {data_}. Params: {params}")
                print(f"        Headers: {headers}")

            if response.ok or errors == "ignore":
                if downloadable:  # Used when downloading carol app file.
                    return response

                response.encoding = "utf-8"
                self.response = response
                if response.text == "":
                    return {}
                return json.loads(response.text)

            if (response.reason == "Unauthorized") and isinstance(self.auth, PwdAuth):
                if response.json().get("possibleResponsibleField") in [
                    "password",
                    "userLogin",
                ]:
                    raise exceptions.InvalidToken(response.text)
                self.auth.get_access_token()  # It will refresh token if Unauthorized
                __count += 1
                if __count < 5:  # To avoid infinity loops
                    continue

                raise Exception(
                    "Too many retries to refresh token.\n",
                    response.text,
                    response.status_code,
                )
            if response.status_code == 404:
                raise exceptions.CarolApiResponseException(
                    response.text, response.status_code
                )

            raise Exception(response.text, response.status_code)

    def issue_api_key(self, connector_id: T.Optional[str] = None) -> ResponseType:
        """Create an API key for a given connector.

        Args:
            connector_id: Connector ID to be used when creating the APIkey

        Returns:
            Dictionary with the API key.
        """
        if connector_id is None:
            connector_id = self.connector_id
        resp = self.call_api(
            "v2/apiKey/issue",
            data={"connectorId": connector_id},
            content_type="application/x-www-form-urlencoded",
        )
        return resp

    def api_key_details(self, api_key: str, connector_id: str) -> ResponseType:
        """Display information about the API key.

        Args:
            api_key: Carol's api key
            connector_id: Connector Id which API key was created.

        Returns:
            Dictionary with API key information.
        """
        resp = self.call_api(
            "v2/apiKey/details", params={"apiKey": api_key, "connectorId": connector_id}
        )

        return resp

    def api_key_revoke(self, connector_id: str) -> ResponseType:
        """Revoke API key for the given connector_id.

        Args:
            connector_id: Connector Id which API key was created.

        Returns:
            Dictionary with API request response.
        """
        resp = self.call_api(
            "v2/apiKey/revoke",
            method="DELETE",
            content_type="application/x-www-form-urlencoded",
            params={"connectorId": connector_id},
        )

        return resp

    def switch_org_level(self):
        """Switch organization level."""
        if self._is_org_level:
            warnings.warn("already in org level.", UserWarning, stacklevel=3)
            return
        org = self._current_org()
        self.auth.switch_org_context(org["mdmId"])
        self._is_org_level = True

    def switch_environment(
        self,
        env_name: T.Optional[str] = None,
        env_id: T.Optional[str] = None,
        app_name: T.Optional[str] = None,
        org_name: T.Optional[str] = None,
        org_id: T.Optional[str] = None,
    ) -> "Carol":
        """Switch org/environments.

        If the user has access to this environment, it will be "logged in" in this new
        org/environment.

        Args:
            env_name: Environment (tenant) name to switch the context to.
            env_id: Environment (tenant) id to switch the context to.
            app_name: App name in the target environment to switch the context to.
                Only needed with using CDS.
            org_name: The organization name to switch context to. If the same keep it
                `None`.
            org_id: The organization id to switch context to. If the same keep it
                `None`.

        Returns:
            Carol

        .. code:: python
            from pycarol import Carol, Staging
            carol = Carol('B', 'teste', auth=PwdAuth('email@totvs.com.br', 'pass'), )
            carol.switch_environment('A')
            Staging(carol_tenant_A).fetch_parquet(...) # fetch parquet from tenant A
            # To switch back
            carol.switch_environment('B')
            #back to tenant B
        """
        if self.org is None:
            self.org = Organization(self).get_organization_info(self.organization)

        current = self.get_current()

        if (org_id is not None and org_id != current["org_id"]) or (
            org_name is not None and org_name != current["org_name"]
        ):
            # Switch to org context.
            if not current["org_level"]:
                self.switch_org_level()
            if org_id is None:
                org_id = Organization(self).get_organization_info(org_name)["mdmId"]
            # Switch org.
            self.auth.switch_org_context(org_id)

        if env_name:
            env_id = Tenant(self).get_tenant_by_domain(env_name)["mdmId"]
        if env_id is not None:
            self.auth.switch_context(env_id=env_id)
            self._tenant = self._current_env()
            self._is_org_level = False
        else:
            self._tenant = None
            self._is_org_level = True

        self.domain = env_name
        # TODO: Today we cannot use CDS without a valid app name.
        self.app_name = app_name
        self.organization = self._current_org()["mdmName"]

        self.host = _set_host(
            domain=self.domain,
            organization=self.organization,
            environment=self.environment,
            host=self._host_string,
        )

        return self

    def switch_context(
        self,
        env_name: T.Optional[str] = None,
        env_id: T.Optional[str] = None,
        app_name: T.Optional[str] = None,
        org_name: T.Optional[str] = None,
        org_id: T.Optional[str] = None,
    ) -> SwitchContext:
        """Context manager to temporary have access to a second environment.

        Args:
            env_name: Environment (tenant) name to switch the context to.
            env_id: Environment (tenant) id to switch the context to.
            app_name: App name in the target environment to switch the context to.
                Only needed with using CDS.

        Returns:
            SwitchContext

        Examples:
            .. code:: python

                from pycarol import Carol, Staging
                carol = Carol('B', 'teste', auth=PwdAuth('email@totvs.com.br', 'pwd'), )
                with carol.switch_context('A') as carol_tenant_A:
                    # fetch parquet from tenant A
                    Staging(carol_tenant_A).fetch_parquet(...)
                #back to tenant B
        """
        if self.org is None:
            self.org = Organization(self).get_organization_info(self.organization)

        if env_name:
            env_id = Tenant(self).get_tenant_by_domain(env_name)["mdmId"]

        return SwitchContext(
            parent_context=copy.deepcopy(self),
            env_name=env_name,
            env_id=env_id,
            app_name=app_name,
            org_name=org_name,
            org_id=org_id,
        )

    def _current_env(self):
        return self.call_api("v1/tenants/current", errors="ignore")

    def _current_org(self):
        return self.call_api("v1/organizations/current")

    def get_current(self, level: str = "all") -> T.Dict[str, T.Any]:
        """Get current org/env information.

        Args:
            level: Possible Values:
                "all": To get organization and environment information.
                "org": To get organization information.
                "env": To get environment information.

        Returns:
            Dictionary with keys org_Id, org_name, env_id, env_name

        Raises:
            ValueError when level not in 'org', 'env' or 'all'
        """
        env = {}
        org = {}

        if level.lower() not in ["org", "env", "all"]:
            raise ValueError(
                f"level should be 'all', 'org', 'env', {level} was passed."
            )

        if level.lower() in ("env", "all"):
            env = self._current_env()

        if level.lower() in ("org", "all"):
            org = self._current_org()

        self._is_org_level = env.get("mdmName") is None

        return {
            "env_name": env.get("mdmName"),
            "env_id": env.get("mdmId"),
            "org_name": org.get("mdmName"),
            "org_id": org.get("mdmId"),
            "org_level": self._is_org_level,
        }

    def get_tenants_for_user(self) -> ResponseType:
        """Get all tenants for the current user.

        Returns:
            Dict
        """
        return self.call_api("v1/users/assignedTenantsForCurrentUser", method="GET")


def _prepare_auth(
    app_oauth: T.Optional[str] = None,
    auth: T.Optional[T.Union[ApiKeyAuth, PwdAuth]] = None,
    connector_id: T.Optional[str] = None,
    password: T.Optional[str] = None,
    username: T.Optional[str] = None,
) -> T.Union[ApiKeyAuth, PwdAuth]:
    app_oauth = app_oauth or os.getenv("CAROLAPPOAUTH") or None
    connector_id = (
        connector_id or os.getenv("CAROLCONNECTORID") or __CONNECTOR_PYCAROL__
    )
    password = password or os.getenv("CAROLPWD") or None
    username = username or os.getenv("CAROLUSER") or None

    if auth is not None:
        if auth.connector_id is None:
            auth.set_connector_id(connector_id)
        return auth

    if username is not None and password is not None:
        auth = PwdAuth(user=username, password=password)
        auth.set_connector_id(connector_id)
        return auth

    if app_oauth is not None and connector_id is not None:
        auth = ApiKeyAuth(app_oauth)
        auth.set_connector_id(connector_id)
        return auth

    raise exceptions.MissingInfoCarolException(
        "either `auth` or `username/password` or `api_key` or "
        "pycarol env variables must be set."
    )


def _set_host(
    environment: str,
    domain: T.Optional[str],
    host: T.Optional[str] = None,
    organization: T.Optional[str] = None,
) -> str:
    """Set the host to be used.

    Args:
        domain: Tenant name.
            e.x., tenant.carol.ai
        environment: There are three possible values today:
            1. 'carol.ai' for the production environment
            2. 'karol.ai' for the explore environment
            3. 'qarol.ai' for the QA environment
        host: Host to be used.
        organization: Organization domain.

    Returns:
        host
    """
    if host is not None:
        return host
    if organization is not None:
        return f"{organization}.{environment}"
    return f"{domain}.{environment}"


def _retry_session(
    retries: int = 5,
    session: T.Optional[requests.Session] = None,
    backoff_factor: float = 0.5,
    status_forcelist: T.Tuple[int, ...] = (500, 502, 503, 504, 524),
    method_whitelist: T.FrozenSet[str] = frozenset(
        ["HEAD", "TRACE", "GET", "PUT", "OPTIONS", "DELETE"]
    ),
) -> requests.Session:
    """Handle retries between calls.

    Args:
        retries: Number of retries for the API calls
        session: Session object dealt `None`
            It allows you to persist certain parameters across requests.
        backoff_factor: Backoff factor to apply between  attempts. It will sleep
            for: {backoff factor} * (2 ^ ({retries} - 1)) seconds
        status_forcelist: A set of integer HTTP status codes that we should force a
            retry on. A retry is initiated if the request method is in
            method_whitelist and the response status code is in status_forcelist.
        method_whitelist: `iterable` , default frozenset(['HEAD', 'TRACE', 'GET',
            'PUT', 'OPTIONS', 'DELETE'])). Set of uppercased HTTP method verbs that
            we should retry on.

    Returns:
        session
    """
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        method_whitelist=method_whitelist,
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
