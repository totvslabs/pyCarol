from importlib import import_module
from flask import Flask, Response
from flask import request as flask_request
import numpy as np
import os
import json

from .health_check_online import HealthCheckOnline
from .online_request import OnlineRequest
from werkzeug.local import Local, LocalProxy


class OnlineApi():
    """ Class to execute Online API locally
    Use the command line "gunicorn run_me:flask" to execute the code below as an API.

    Usage:
        from pycarol.app.online_api import OnlineApi
        from pycarol.app.online import Online

        online = Online()

        @online.route("sample_endpoint")
        def sample1_function(args):
            return str(args)

        # "run_me" is the filename
        flask = OnlineApi('run_me').get_api()
    """
    def __init__(self, file_name=None, file_path='', domain=None, app_name=None, app_version=None, online_name=None):
        self.file_path = file_path
        self.imported_module = None
        self.endpoints = dict()
        self.logs = []

        if domain:
            self.domain = domain
        else:
            self.domain = os.environ.get('CAROLDOMAIN', 'domain')

        if app_version:
            self.app_version = app_version
        else:
            self.app_version = os.environ.get('CAROLAPPVERSION', 'version')

        if app_name:
            self.app_name = app_name
        else:
            self.app_name = os.environ.get('CAROLAPPNAME', 'name')

        if online_name:
            self.online_name = online_name
        else:
            self.online_name = os.environ.get('CAROLONLINENAME', 'name')

        if not file_name:
            file_name = os.environ.get('ALGORITHMNAME')

        if file_name:
            self.module_name = os.path.splitext(file_name)[0]
        else:
            self.module_name = ''
            self._log_append('The file name should be defined by parameter ou environment variable')

        self._dynamic_import()
        self._load_endpoints()
        self._health_check_carol()

    def _log_append(self, msg):
        self.logs.append(msg)
        print(msg)

    def _dynamic_import(self):
        try:
            self.imported_module = import_module(f'{self.file_path}{self.module_name}')
        except Exception as e:
            self._log_append(f'Problem when importing file. Module: {self.module_name}. Error: {str(e)}')

    def _load_endpoints(self):
        try:
            if self.imported_module:
                for i in dir(self.imported_module):
                    if type(getattr(self.imported_module, i)).__name__ == 'Online':
                        online = getattr(self.imported_module, i)
                        self.endpoints.update(online.get_endpoints())
        except Exception as e:
            self._log_append(f'Problem when trying to load module. Module: {self.module_name}. Error: {str(e)}')

    def _health_check_carol(self):
        healthCheckOnline = HealthCheckOnline(self.logs)
        healthCheckOnline.send_status_carol()

    def get_api(self, debug=False):
        flask = Flask(__name__)

        @flask.route('/', methods=['GET', 'POST'])
        def base():
            return 'Running! Use /api/(endpoint) to access the app api'

        @flask.route(f'/api/<api_path>', methods=['GET', 'POST'])
        def app(api_path):
            try:
                api = self.endpoints[str(api_path)]
            except:
                return f'Endpoint {api_path} not found'

            local.request = OnlineRequest(values=flask_request.values, json=flask_request.json)
            r = api()
            if isinstance(r, np.ndarray):
                r = r.tolist()
            if isinstance(r, tuple):
                resp, code = r
                return Response(json.dumps(resp), status=code, mimetype='application/json')
            return json.dumps(r)

        @flask.route(f'/statusz')
        def app_statusz():
            return 'ok'

        @flask.route(f'/healthz')
        def app_healthz():
            return 'ok'

        @flask.route(f'/logs')
        def app_logs():
            return str(self.logs)

        flask.debug = debug
        return flask

    def run(self, debug=False):
        flask = self.get_api(debug)
        flask.run()
        return flask


local = Local()
request = LocalProxy(local, 'request')