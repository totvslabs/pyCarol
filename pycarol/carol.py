from urllib3.util.retry import Retry
import requests
from requests.adapters import HTTPAdapter
import json
import os, copy
import os.path
from .auth.ApiKeyAuth import ApiKeyAuth
from .auth.PwdAuth import PwdAuth
from .tenant import Tenant
from . import __CONNECTOR_PYCAROL__
from . import __version__
from . organization import Organization


class Carol:
    """
    This class handle all Carol`s API calls It will handle all API calls,
    for a given authentication method. :param domain: `str`.

    Args:

        domain: `str`. default `None`.
            Tenant name. e.x., domain.carol.ai
        app_name: `str`. default `None`.
            Carol app name.
        auth: `PwdAuth` or `ApiKeyAuth`.
            object Auth Carol object to handle authentication
        connector_id: `str` , default `__CONNECTOR_PYCAROL__`.
            Connector Id
        port: `int` , default 443.
            Port to be used (when running locally it could change)
        verbose: `bool` , default `False`.
            If True will print the header, method and URL of each API call.
        organization: `str` , default `None`.
            Organization domain. 
        environment: `str`, default `carol.ai`,
            Which Carol's environment to use. There are three possible values today.

                1. 'carol.ai' for the production environment
                2. 'karol.ai' for the explore environment
                3. 'qarol.ai' for the QA environment

        host: `str` default `None`
            This will overwrite the host used. Today the host is:

                1. if organization is None, host={domain}.{environment}
                2. else host={organization}.{environment}

            See Carol._set_host.

    OBS:
        In case all parameters are `None`, pycarol will try yo find their values in the environment variables.
        The values are:

             1. `CAROLTENANT` for domain
             2. `CAROLAPPNAME` for app_name
             3. `CAROLAPPOAUTH` for auth
             4. `CAROLORGANIZATION` for organization
             5. `CAROLCONNECTORID` for connector_id
             6. `CAROL_DOMAIN` for environment
             7. `CAROLUSER` for carol user email
             8. `CAROLPWD` for user password.


    """

    def __init__(self, domain=None, app_name=None, auth=None, connector_id=None, port=443, verbose=False,
                 organization=None, environment=None, host=None):

        settings = dict()
        if auth is None and domain is None:

            domain = os.getenv('CAROLTENANT')
            app_name = os.getenv('CAROLAPPNAME')

            assert domain and app_name, \
                f"One of the following env variables are missing:\n CAROLTENANT: {domain}\n CAROLAPPNAME: {app_name}"

            carol_user = os.getenv('CAROLUSER')
            carol_pw = os.getenv('CAROLPWD')

            if carol_user and carol_pw:
                auth = PwdAuth(user=carol_user, password=carol_pw)

            else:
                auth_token = os.getenv('CAROLAPPOAUTH')
                connector_id = os.getenv('CAROLCONNECTORID')

                assert domain and app_name and auth_token and connector_id,\
                    "One of the following env variables are missing:\n " \
                    f"CAROLTENANT: {domain}\nCAROLAPPNAME: {app_name}" \
                    f"\nCAROLAPPOAUTH: {auth}\nCAROLCONNECTORID: {connector_id}\n"

                auth = ApiKeyAuth(auth_token)

        if connector_id is None:
            if auth.connector_id is None:
                connector_id = __CONNECTOR_PYCAROL__
            else:
                connector_id = auth.connector_id

        if domain is None or app_name is None or auth is None:
            raise ValueError("domain, app_name and auth must be specified as parameters, either " +
                             "in the environment variables CAROLTENANT, CAROLAPPNAME, CAROLAPPOAUTH" +
                             " or CAROLUSER+CAROLPWD and  CAROLCONNECTORID")

        # TODO Fixed to be compatible with the old `ENV_DOMAIN`. We could add a deprecated warning.
        self.environment = environment if environment is not None else os.getenv('CAROL_DOMAIN',
                                                                                 os.getenv('ENV_DOMAIN', 'carol.ai'))
        self.organization = organization if organization is not None else os.getenv('CAROLORGANIZATION',
                                                                                    os.getenv('API_SUBDOMAIN'))
        self.domain = domain
        self.app_name = app_name
        self.port = port
        self.verbose = verbose
        self._host_string = host
        self.host = self._set_host(domain=self.domain, organization=self.organization,
                                   environment=self.environment, host=host)
        self._tenant = None
        self.connector_id = connector_id
        self.auth = auth
        self.auth.set_connector_id(self.connector_id)
        self.auth.login(self)
        self.response = None

        self.org = None



    @property
    def tenant(self):
        if self._tenant is None:
            self._tenant = Tenant(self).get_tenant_by_domain(self.domain)
        return self._tenant


    @staticmethod
    def _set_host(domain, organization, environment, host):
        """
        Set the host to be used.

        Args:

            domain: `str`
                Former tenant name.
                e.x., domain.carol.ai
            organization: `str`
                Organization domain.
            environment: `str`
                Which Carol's environment to use. There are three possible values today.

                1. 'carol.ai' for the production environment
                2. 'karol.ai' for the explore environment
                3. 'qarol.ai' for the QA environment

            host: `str`
                Host to be used. It overwrite the default one.

        Returns: `str`
            host
        """
        if host is not None:
            return host
        elif organization is not None:
            return f"{organization}.{environment}"
        else:
            return f"{domain}.{environment}"
        pass

    @staticmethod
    def _retry_session(retries=5, session=None, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504, 524),
                       method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE'])):

        """
        Static method used to handle retries between calls.

        Args:

            retries: `int` , default `5`
                Number of retries for the API calls
            session: Session object dealt `None`
                It allows you to persist certain parameters across requests.
            backoff_factor: `float` , default `0.5`
                Backoff factor to apply between  attempts. It will sleep for:
                        {backoff factor} * (2 ^ ({retries} - 1)) seconds
            status_forcelist: `iterable` , default (500, 502, 503, 504, 524).
                A set of integer HTTP status codes that we should force a retry on.
                A retry is initiated if the request method is in method_whitelist and the response status code is in
                status_forcelist.
            method_whitelist: `iterable` , default frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE']))
                Set of uppercased HTTP method verbs that we should retry on.

        Returns:
            :class:`requests.Section`
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
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def call_api(self, path, method=None, data=None, auth=True, params=None, content_type='application/json', retries=8,
                 session=None, backoff_factor=0.5, status_forcelist=(502, 503, 504, 524), downloadable=False,
                 method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE']), errors='raise',
                 extra_headers=None,
                 **kwds):
        """
        This method handles all the API calls.

        Args:

            path: `str`.
                API URI path. e.x.  v2/staging/schema
            method: 'str', default `None`.
                Set of uppercased HTTP method verbs that we should call on.
            data: 'dict`, default `None`.
                Dictionary, list of tuples, bytes, or file-like object to send in
                the body of the request.
            auth: :class: `pycarol.ApiKeyAuth` or `pycarol.PwdAuth`
                Auth type to be used within the API's calls.
            params: (optional) Dictionary, list of tuples or bytes to send
                     in the query string for the :class:`requests.Request`.
            content_type: `str`, default 'application/json'
                Content type for the api call
            retries: `int` , default `5`
                Number of retries for the API calls
            session: :class `requests.Session` object dealt `None`
                It allows you to persist certain parameters across requests.
            backoff_factor: `float` , default `0.5`
                Backoff factor to apply between  attempts. It will sleep for:
                        {backoff factor} * (2 ^ ({retries} - 1)) seconds
            status_forcelist: `iterable` , default (500, 502, 503, 504, 524).
                A set of integer HTTP status codes that we should force a retry on.
                A retry is initiated if the request method is in method_whitelist and the response status code is in
                status_forcelist.
            downloadable: `bool` default `False`.
                If the request will return a file to download.
            method_whitelist: `iterable` , default frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE']))
                Set of uppercased HTTP method verbs that we should retry on.
            errors: {‘ignore’, ‘raise’}, default ‘raise’
                If ‘raise’, then invalid request will raise an exception If ‘ignore’,
                then invalid request will return the request response
            extra_headers: `dict` default `None`
                extra headers to be sent.
            kwds: `dict` default `None`
                Extra parameters to be sent to :class: `requests.request`

        Rerturn:
            Dict with API response.

        """

        extra_headers = extra_headers or {}
        url = f'https://{self.host}:{self.port}/api/{path}'

        if method is None:
            if data is None:
                method = 'GET'
            else:
                method = 'POST'

        met_list = ['HEAD', 'TRACE', 'GET', 'PUT','POST', 'OPTIONS', 'PATCH',
                    'DELETE', 'CONNECT' ]
        assert method in met_list, f"API method must be {met_list}"

        headers = {'accept': 'application/json'}
        if auth:
            self.auth.authenticate_request(headers)

        data_json = None
        if method == 'GET':
            pass

        elif (method == 'POST') or (method == 'DELETE') or (method == 'PUT'):
            headers['content-type'] = content_type

            if content_type == 'application/json':
                data_json = data
                data = None

        headers.update(extra_headers)
        headers.update({'User-Agent': f'pyCarol/{__version__}'})

        __count = 0
        while True:
            if session is None:
                session = self._retry_session(retries=retries, session=session, backoff_factor=backoff_factor,
                                              status_forcelist=status_forcelist, method_whitelist=method_whitelist)

            response = session.request(method=method, url=url, data=data, json=data_json,
                                       headers=headers, params=params, **kwds)

            if self.verbose:
                if data_json is not None:
                    print("Calling {} {}. Payload: {}. Params: {}".format(method, url, data_json, params))
                else:
                    print("Calling {} {}. Payload: {}. Params: {}".format(method, url, data, params))
                print("        Headers: {}".format(headers))

            if response.ok or errors == 'ignore':
                if downloadable:  #Used when downloading carol app file.
                    return response

                response.encoding = 'utf-8'
                self.response = response
                if response.text == '':
                    return {}
                return json.loads(response.text)

            elif (response.reason == 'Unauthorized') and isinstance(self.auth,PwdAuth):
                if response.json().get('possibleResponsibleField') in ['password', 'userLogin']:
                    raise Exception(response.text)
                self.auth.get_access_token()  #It will refresh token if Unauthorized
                __count+=1
                if __count<5: #To avoid infinity loops
                    continue
                else:
                    raise Exception('Too many retries to refresh token.\n', response.text, response.status_code)

            raise Exception(response.text, response.status_code)

    def issue_api_key(self):
        """
        Create an API key for a given connector.

        Returns: `dict`
            Dictionary with the API key.

        """
        resp = self.call_api('v2/apiKey/issue', data={
            'connectorId': self.connector_id
        }, content_type='application/x-www-form-urlencoded')
        return resp

    def api_key_details(self, api_key, connector_id):

        """
        Display information about the API key.

        Args:

            api_key: `str`
                Carol's api key
            connector_id: `str`
                Connector Id which API key was created.

        Returns: `dict`
            Dictionary with API key information.

        """

        resp = self.call_api('v2/apiKey/details',
                             params = {"apiKey": api_key, "connectorId": connector_id})

        return resp

    def api_key_revoke(self, connector_id):
        """
        Revoke API key for ta given connector_id

        Args:

            connector_id: `str`
                Connector Id which API key was created.

        Returns: `dict`

            Dictionary with API request response.

        """

        resp = self.call_api('v2/apiKey/revoke', method='DELETE',
                             content_type='application/x-www-form-urlencoded',
                             params = {"connectorId": connector_id})

        return resp

    def copy_token(self):
        """
        Copy token to clipboard

        Returns:
            None
        """

        import pyperclip
        if isinstance(self.auth, PwdAuth):
            token = self.auth._token.access_token
            pyperclip.copy(token)
            print("Copied auth token to clipboard: " + token)
        elif isinstance(self.auth, ApiKeyAuth):
            token = self.auth.api_key
            pyperclip.copy(token)
            print("Copied API Key to clipboard: " + token)
        else:
            raise Exception("Auth object not set. Can't fetch token.")

    def switch_org_level(self):

        org = self._current_org()
        self.auth.switch_org_context(org['mdmId'])

    def switch_environment(self, env_name=None, env_id=None, app_name=None, org_name=None, org_id=None):
        """
        Switch org/environments. If the user has access to this environment, it will be "logged in" in this new
        org/environment.

        Args:

            env_name: `str` default `None`
                Environment (tenant) name to switch the context to.
            env_id: `str` default `None`
                Environment (tenant) id to switch the context to.
            app_name: `str` default `None`
                App name in the target environment to switch the context to.
                Only needed with using CDS.
            org_name: `str` default `None`
                The organization name to switch context to. If the same keep it `None`
            org_id: `str` default `None`
                The organization id to switch context to. If the same keep it `None`

        Returns:
            self

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

        if org_id is not None or org_name is not None:
            # Switch to org context.
            self.switch_org_level()
            if org_id is None:
                org_id = Organization(self).get_organization_info(org_name)['mdmId']
            # Switch org.
            self.auth.switch_org_context(org_id)

        if env_name:
            env_id = Tenant(self).get_tenant_by_domain(env_name)['mdmId']
        elif env_id is None:
            raise ValueError('Either `env_name` or `env_id` must be set.')

        self.auth.switch_context(env_id=env_id)

        self.domain = env_name
        self.app_name = app_name # TODO: Today we cannot use CDS without a valid app name.
        self._tenant = self._current_env()['mdmName']
        self.organization = self._current_org()['mdmName']

        self.host = self._set_host(domain=self.domain, organization=self.organization,
                                   environment=self.environment, host=self._host_string)

        return self

    def switch_context(self, env_name=None, env_id=None, app_name=None, org_name=None, org_id=None):
        """
        Context manager to temporary have access to a second environment

        Args:

            env_name: `str` default `None`
                Environment (tenant) name to switch the context to.
            env_id: `str` default `None`
                Environment (tenant) id to switch the context to.
            app_name: `str` default `None`
                App name in the target environment to switch the context to.
                Only needed with using CDS.

        Returns:
            None

        Usage:

        .. code:: python

            from pycarol import Carol, Staging
            carol = Carol('B', 'teste', auth=PwdAuth('email@totvs.com.br', 'pass'), )
            with carol.switch_context('A') as carol_tenant_A:
                Staging(carol_tenant_A).fetch_parquet(...) # fetch parquet from tenant A
            #back to tenant B


        """

        if self.org is None:
            self.org = Organization(self).get_organization_info(self.organization)

        if env_name:
            env_id = Tenant(self).get_tenant_by_domain(env_name)['mdmId']
        elif env_id is None:
            raise ValueError('Either `env_name` or `env_id` must be set.')

        class SwitchContext(object):

            def __init__(self, parent_context, env_name=None, env_id=None, org_id=None, org_name=None, app_name=None):
                self.parent_context = parent_context
                self.env_name = env_name
                self.env_id = env_id
                self.app_name = app_name
                self.org_id = org_id
                self.org_name = org_name

            def __enter__(self):
                self.parent_context.switch_environment(env_name=self.env_name, env_id=self.env_id,
                                                       app_name=self.app_name, org_name=self.org_name,
                                                       org_id=self.org_id)
                return self.parent_context

            def __exit__(self, exc_type, exc_val, exc_tb):
                del self.parent_context

        return SwitchContext(parent_context=copy.deepcopy(self), env_name=env_name, env_id=env_id, app_name=app_name,
                             org_name=org_name, org_id=org_id,)


    def _current_env(self):
        return self.call_api('v1/tenants/current')

    def _current_org(self):
        return self.call_api('v1/organizations/current')

    def get_current(self, level='all'):
        """
        Get current org/env information.

        Args:

            level: `str`:
                Possible Values:
                    "all": To get organization and environment information.
                    "org": To get organization information.
                    "env": To get environment information.

        Returns: `dict`
            Dictionary with keys org_Id, org_name, env_id, env_name

        """

        env = {}
        org = {}

        if level.lower() not in ['org', 'env', 'all']:
            raise ValueError(f"level should be 'all', 'org', 'env', {level} was passed.")

        if level.lower() == 'env' or level.lower() == 'all':
            env = self._current_env()

        if level.lower() == 'org' or level.lower() == 'all':
            org = self._current_org()

        return {
            "env_name": env.get('mdmName'),
            "env_id": env.get('mdmId'),
            "org_name": org.get('mdmName'),
            "org_id": org.get('mdmId'),
        }
