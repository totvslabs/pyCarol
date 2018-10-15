from .tenants import Tenants
from urllib3.util.retry import Retry
import requests
from requests.adapters import HTTPAdapter
import json
import os
import os.path
from pycarol.auth.ApiKeyAuth import ApiKeyAuth
from pycarol.auth.PwdAuth import PwdAuth


class Carol:
    def __init__(self, domain=None, app_name=None, auth=None, connector_id=None, port=443, verbose=False):
        settings = dict()
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
                    connector_id = app_config.get('connector_id', os.getenv('CAROLCONNECTORID', '0a0829172fc2433c9aa26460c31b78f0'))

        if domain is None or app_name is None or auth is None:
            raise ValueError("domain, app_name and auth must be specified as parameters, in the app_config.json file " +
                             "or in the environment variables CAROLTENANT, CAROLAPPOAUTH OR CAROLUSER+CAROLPWD and " +
                             "CAROLCONNECTORID")

        self.domain = domain
        self.app_name = app_name
        self.port = port
        self.verbose = verbose
        self.tenant = Tenants(self).get_tenant_by_domain(domain)
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
        return 'wss://{}.carol.ai:{}/websocket/{}'.format(self.domain, self.port, path)

    @staticmethod
    def _retry_session(retries=5, session=None, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504, 524),
                       method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE'])):
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
                 method_whitelist=frozenset(['HEAD', 'TRACE', 'GET', 'PUT', 'OPTIONS', 'DELETE']),
                 **kwds):
      
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

        section = self._retry_session(retries=retries, session=session, backoff_factor=backoff_factor,
                                      status_forcelist=status_forcelist, method_whitelist=method_whitelist)
        response = section.request(method=method, url=url, data=data, json=data_json,
                                   headers=headers, params=params, **kwds)
        
        if self.verbose:
            if data_json is not None:
                print("Calling {} {}. Payload: {}. Params: {}".format(method, url, data_json, params))
            else:
                print("Calling {} {}. Payload: {}. Params: {}".format(method, url, data, params))
            print("        Headers: {}".format(headers))

        if response.ok:
            if downloadable:
                return response

            response.encoding = 'utf-8'
            self.response = response
            return json.loads(response.text)
        else:
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
