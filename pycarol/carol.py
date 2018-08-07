import json
import requests
from pycarol.tenants import *
from pycarol.storage import *
from pycarol.connectors import *
from pycarol.carolina import *
from pycarol.staging import *
from pycarol.tasks import *


class Carol:
    def __init__(self, domain, app_name, auth, connector_id=None, port=443, verbose=False):
        self.domain = domain
        self.app_name = app_name
        self.port = port
        self.verbose = verbose
        self.tenants = Tenants(self)
        self.storage = Storage(self)
        self.connectors = Connectors(self)
        self.carolina = Carolina(self)
        self.staging = Staging(self)
        self.tasks = Tasks(self)
        self.tenant = self.tenants.get_tenant_by_domain(domain)

        default_connector_id = '0a0829172fc2433c9aa26460c31b78f0'
        self.connector_id = connector_id
        if self.connector_id is None:
            self.connector_id = default_connector_id
        self.auth = auth
        self.auth.login(self)


    def call_api(self, path, method=None, data=None, auth=True, params=None, content_type='application/json'):
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
            response = requests.get(url=url, headers=headers, params=params)
        elif (method == 'POST') or (method == 'DELETE') or (method == 'PUT'):
            headers['content-type'] = content_type

            if content_type == 'application/json':
                data_json = data
                data = None
            response = requests.request(method=method, url=url, data=data, json=data_json,
                                     headers=headers, params=params)

        if self.verbose:
            if data_json is not None:
                print("Calling {} {}. Payload: {}. Params: {}".format(method, url, data_json, params))
            else:
                print("Calling {} {}. Payload: {}. Params: {}".format(method, url, data, params))
            print("        Headers: {}".format(headers))

        response.encoding = 'utf-8'
        if response.ok:
            return json.loads(response.text)
        else:
            raise Exception(json.loads(response.text))

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
