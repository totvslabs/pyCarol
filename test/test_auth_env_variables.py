import pytest
import os

os.environ['CAROLTENANT']
os.getenv['CAROLAPPNAME']
os.getenv['CAROLAPPOAUTH']
os.getenv['CAROLCONNECTORID']


TENANT_NAME = 'pycarol'
APP_NAME = 'my_app'
USERNAME = 'pycarol@totvs.com.br'
PASSWORD = 'foo123'


def test_password_login():
    from pycarol import PwdAuth, Carol
    login = Carol(domain=TENANT_NAME, app_name=APP_NAME, auth=PwdAuth(user=USERNAME, password=PASSWORD))
    assert login.auth._token.access_token is not None
    assert login.auth._token.refresh_token is not None
    assert login.auth._token.expiration is not None


def test_APIKEY_create_and_revoke():
    from pycarol import ApiKeyAuth, Carol, PwdAuth
    login = Carol(domain=TENANT_NAME, app_name=APP_NAME, auth=PwdAuth(user=USERNAME, password=PASSWORD))

    api_key = login.issue_api_key()

    assert api_key['X-Auth-Key']
    assert api_key['X-Auth-ConnectorId']

    X_Auth_Key = api_key['X-Auth-Key']
    X_Auth_ConnectorId = api_key['X-Auth-ConnectorId']

    print(f"This is a API key {api_key['X-Auth-Key']}")
    print(f"This is the connector Id {api_key['X-Auth-ConnectorId']}")

    revoke = login.api_key_revoke(connector_id=X_Auth_ConnectorId)

    assert revoke['success']









