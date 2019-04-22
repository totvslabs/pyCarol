from urllib3.util.retry import Retry
import requests
from requests.adapters import HTTPAdapter
import json
import os
import os.path
from .auth.ApiKeyAuth import ApiKeyAuth
from .auth.PwdAuth import PwdAuth
from .tenant import Tenant
from . import __CONNECTOR_PYCAROL__


class Carol:
    def __init__(self, domain=None, app_name=None, auth=None, connector_id=None, port=443, verbose=False):
        """
        This class handle all Carol`s API calls
        It will handle all API calls, for a given authentication method.
        :param domain: `str`
            Teanant name
        :param app_name: `str`
            Carol app name.
        :param auth: `PwdAuth` or `ApiKeyAuth` object
            Auth Carol object to handle authentication
        :param connector_id: `str`, default `__CONNECTOR_PYCAROL__`
            Connector Id
        :param port: `int`, default 443
            Port to be used (when running locally)
        :param verbose: `bool`, default `False
            If True will print the header, method and URL of each API call.
        """
        self.legacy_mode = False
        self.legacy_bucket = 'carol-internal'

        settings = dict()
        if auth is None and domain is None:

            if os.path.isfile('app_config.json'):
                with open('app_config.json', 'r') as f:
                    settings = json.load(f)

                if domain is None:
                    domain = settings.get('domain', os.getenv('CAROLTENANT'))
                if app_name is None:
                    app_name = settings.get('app_name', os.getenv('CAROLAPPNAME'))

                app_config = settings.get(domain)
                if app_config is not None:
                    if auth is None:
                        auth_token = app_config.get('oauth_token')
                        if auth_token is not None:
                            auth = ApiKeyAuth(auth_token)
                        else:
                            auth_user = app_config.get('auth_user')
                            auth_pwd = app_config.get('auth_pwd')
                            if auth_user is not None and auth_pwd is not None:
                                auth = PwdAuth(auth_user, auth_pwd)
                            else:
                                auth_token = os.getenv('CAROLAPPOAUTH')
                                if auth_token is not None:
                                    auth = ApiKeyAuth(auth_token)
                                else:
                                    auth_user = os.getenv('CAROLUSER')
                                    auth_pwd = os.getenv('CAROLPWD')
                                    if auth_user is not None and auth_pwd is not None:
                                        auth = PwdAuth(auth_user, auth_pwd)

                    if connector_id is None:
                        connector_id = app_config.get('connector_id', os.getenv('CAROLCONNECTORID', __CONNECTOR_PYCAROL__))

            else: # env login
                domain = os.getenv('CAROLTENANT')
                app_name = os.getenv('CAROLAPPNAME')
                auth_token = os.getenv('CAROLAPPOAUTH')
                connector_id = os.getenv('CAROLCONNECTORID')
                assert (domain is not None) and (app_name is not None) and (auth_token is not None) and (connector_id is not None),\
                        "One of the following env variables are missing:\n " \
                        f"CAROLTENANT: {domain}\nCAROLAPPNAME: {app_name}\nCAROLAPPOAUTH: {auth}\nCAROLCONNECTORID: {connector_id}\n"
                auth = ApiKeyAuth(auth_token)


        if connector_id is None:
            connector_id =  __CONNECTOR_PYCAROL__

        if domain is None or app_name is None or auth is None:
            raise ValueError("domain, app_name and auth must be specified as parameters, in the app_config.json file " +
                             "or in the environment variables CAROLTENANT, CAROLAPPNAME, CAROLAPPOAUTH OR CAROLUSER+CAROLPWD and " +
                             "CAROLCONNECTORID")

        self.domain = domain
        self.app_name = app_name
        self.port = port
        self.verbose = verbose
        self.tenant = Tenant(self).get_tenant_by_domain(domain)
        self.connector_id = connector_id
        self.auth = auth
        self.auth.set_connector_id(self.connector_id)
        self.auth.login(self)
        self.response = None

        self.long_task_id = settings.get('long_task_id', os.getenv('LONGTASKID'))
        self.debug = settings.get('debug', os.getenv('DEBUG'))
        self.env = settings.get('env', os.getenv('ENV'))
        self.dry_run = settings.get('dry_run', os.getenv('DRYRUN'))
        self.git = settings.get('git', os.getenv('GIT'))
        self.file_name = settings.get('file_name', os.getenv('FILENAME'))
        self.function_name = settings.get('function_name', os.getenv('FUNCTIONNAME'))
        self.carol_batch_name = settings.get('carol_batch_name', os.getenv('CAROLBATCHNAME'))
        self.carol_process_type = settings.get('carol_process_type', os.getenv('CAROLPROCESSTYPE'))

    def build_ws_url(self, path):
        """
        Return the URS to be used with websocket and queries.
        :param path: `str`
            API and point.
        :return: str
        """
        return 'wss://{}.carol.ai:{}/websocket/{}'.format(self.domain, self.port, path)

    @staticmethod
    def _retry_session(retries=5, session=None, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504, 524),
                       method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE'])):
        """
        :param retries: `int`, default `5`
            Number of retries for the API all
        :param session: Session object defaut `None`
            It allows you to persist certain parameters across requests.
        :param backoff_factor: `float`, default `0.5`
            Backoff factor to apply between attempts. It will
            sleep for: {backoff factor} * (2 ^ ({retries} - 1)) seconds
        :param status_forcelist: `iterable`, default (500, 502, 503, 504, 524)
            A set of integer HTTP status codes that we should force a retry on.
             A retry is initiated if the request method is in method_whitelist and the response status code is in status_forcelist.
        :param method_whitelist: `iterable`, default frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE']))
            Set of uppercased HTTP method verbs that we should retry on.
        :return: session object
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

    def call_api(self, path, method=None, data=None, auth=True, params=None, content_type='application/json',retries=5,
                 session=None, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504, 524), downloadable=False,
                 method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE']), errors='raise',
                 extra_headers=None,
                 **kwds):
        """
        :param path:
        :param method:
        :param data:
        :param auth:
        :param params:
        :param content_type:
        :param retries:
        :param session:
        :param backoff_factor:
        :param status_forcelist:
        :param downloadable:
        :param method_whitelist:
        :param extra_headers:
        :param errors : {‘ignore’, ‘raise’}, default ‘raise’
                If ‘raise’, then invalid request will raise an exception
                If ‘ignore’, then invalid request will return the request response
        :param kwds:
        :return:
        """

        extra_headers = extra_headers or {}
        url = 'https://{}.carol.ai:{}/api/{}'.format(self.domain, self.port, path)

        if method is None:
            if data is None:
                method = 'GET'
            else:
                method = 'POST'

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

            elif (response.reason == 'Unauthorized') and  isinstance(self.auth,PwdAuth):
                self.auth.get_access_token()  #It will refresh token if Unauthorized
                __count+=1
                if __count<5: #To avoid infinity loops
                    continue
                else:
                    raise Exception('Too many retries to refresh token.\n',response.text)
            raise Exception(response.text)

    def issue_api_key(self):
        resp = self.call_api('v2/apiKey/issue', data={
            'connectorId': self.connector_id
        }, content_type='application/x-www-form-urlencoded')
        return resp

    def api_key_details(self, api_key, connector_id):

        resp = self.call_api('v2/apiKey/details',
                             params = {"apiKey": api_key,
                                            "connectorId": connector_id})

        return resp

    def api_key_revoke(self, connector_id):

        resp = self.call_api('v2/apiKey/revoke', method='DELETE',
                             content_type='application/x-www-form-urlencoded',
                             params = {"connectorId": connector_id})

        return resp

    def copy_token(self):
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


