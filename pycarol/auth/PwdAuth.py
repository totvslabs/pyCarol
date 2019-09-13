import types
import time
from .PwdAuth_cloner import PwdAuthCloner


class PwdAuth:
    def __init__(self, user, password):
        self.user = user
        self.password = password
        self._token = None
        self.carol = None
        self.connector_id = None

    def set_connector_id(self, connector_id):
        self.connector_id = connector_id

    def cloner(self):
        return PwdAuthCloner(self)

    def login(self, carol):
        self.carol = carol

        data = {
            'username': self.user,
            'password': self.password,
            'grant_type': 'password',
            'subdomain': carol.domain,
            'connectorId': carol.connector_id,
            'orgSubdomain': carol.organization
        }
        resp = self.carol.call_api('v2/oauth2/token', auth=False, data=data,
                                   content_type='application/x-www-form-urlencoded')

        self._token = types.SimpleNamespace()
        self._token.access_token = resp['access_token']
        self._token.refresh_token = resp['refresh_token']
        self._token.expiration = resp['timeIssuedInMillis'] + (resp['expires_in'] * 1000)
        if self.carol.verbose:
            print("Token: {}".format(self._token.access_token))

    def authenticate_request(self, headers):
        headers['Authorization'] = self.get_access_token()

    def _is_token_expired(self):
        if self._token is None:
            return True

        if self._token.expiration == 0:
            return False

        now = time.time() * 1000
        # Adds 1 min buffer
        expiry = self._token.expiration - 60000

        return now > expiry

    def get_access_token(self):
        if self._is_token_expired():
            self._refresh_token()

        return self._token.access_token

    def _refresh_token(self):
        resp = self.carol.call_api('v2/oauth2/token', auth=False, data={
            'grant_type': 'refresh_token',
            'refresh_token': self._token.refresh_token
        }, content_type='application/x-www-form-urlencoded')

        self._token = types.SimpleNamespace()
        self._token.access_token = resp['access_token']
        self._token.refresh_token = resp['refresh_token']
        self._token.expiration = resp['timeIssuedInMillis'] + (resp['expires_in'] * 1000)
