import types
import time
from .PwdAuth_cloner import PwdAuthCloner


class PwdKeyAuth:
    """

    Args:
        access_token: `str`
            Carol access token created using the user and password.
        lazy_login: `bool` default `False`
            It will not validate the `access_token` when starting the instance.
            This is used to speedup requests, when the user knows that this token is valid. Using this option, one lose
            all refresh token capability.
    """

    def __init__(self, access_token, lazy_login=False):
        self.access_token = access_token
        self._token = None
        self.carol = None
        self.connector_id = None
        self.lazy_login = lazy_login

    def set_connector_id(self, connector_id):
        self.connector_id = connector_id

    def login(self, carol):
        """

        Args:
            carol: pycarol.Carol
                Carol() instance.

        Returns:
            None

        """
        self.carol = carol

        if self.lazy_login:
            self._token = types.SimpleNamespace()
            self._token.access_token = self.access_token
            self._token.refresh_token = None
            self._token.expiration = float('inf')
            return


        resp = self.carol.call_api(f'v2/oauth2/token/{self.access_token}', method='GET', auth=False,
                                   content_type='application/x-www-form-urlencoded')

        self._token = types.SimpleNamespace()
        self._token.access_token = resp['access_token']
        self._token.refresh_token = resp['refresh_token']
        self._token.expiration = resp['timeIssuedInMillis'] + (
                    resp['expires_in'] * 1000)
        if self.carol.verbose:
            print("Token: {}".format(self._token.access_token))

    def authenticate_request(self, headers):
        headers['Authorization'] = self.get_access_token()

    def _is_token_expired(self):
        if self._token is None:
            self.login()
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
        self._token.expiration = resp['timeIssuedInMillis'] \
                                 + (resp['expires_in'] * 1000)

    def switch_context(self, env_id):
        """
        Switch context to an environment within the same organization.

        Args:
            env_id: environment id to switch context to.

        Returns:
            None

        """

        path = f'v2/oauth2/switchTenantContext/{env_id}'
        resp = self.carol.call_api(method='POST', path=path)

        self._token = types.SimpleNamespace()
        self._token.access_token = resp['access_token']
        self._token.refresh_token = resp['refresh_token']
        self._token.expiration = resp['timeIssuedInMillis'] \
                                 + (resp['expires_in'] * 1000)

    def switch_org_context(self, org_id):
        """
        Go to the organization context or switch organization.

        Args:
            org_id: organization id.

        Returns:
            None

        """

        path = f'v2/oauth2/switchOrgContext/{org_id}'
        resp = self.carol.call_api(method='POST', path=path)

        self._token = types.SimpleNamespace()
        self._token.access_token = resp['access_token']
        self._token.refresh_token = resp['refresh_token']
        self._token.expiration = resp['timeIssuedInMillis'] + (
                resp['expires_in'] * 1000)
