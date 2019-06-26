import pytest
import os

os.environ['CAROLTENANT'] = 'pycarol'
os.environ['CAROLAPPNAME'] = 'my_app'
os.environ['CAROLUSER'] = 'pycarol@totvs.com.br'
os.environ['CAROLPWD'] = 'foo123'



def test_password_login_env_vars():
    from pycarol import Carol
    login = Carol()
    assert login.auth._token.access_token is not None
    assert login.auth._token.refresh_token is not None
    assert login.auth._token.expiration is not None


def test_APIKEY_create_and_revoke_env_vars():
    from pycarol import Carol, PwdAuth
    login = Carol()

    old_user = os.environ.pop('CAROLUSER')
    old_pw = os.environ.pop('CAROLPWD')

    api_key = login.issue_api_key()

    assert api_key['X-Auth-Key']
    assert api_key['X-Auth-ConnectorId']

    X_Auth_Key = api_key['X-Auth-Key']
    X_Auth_ConnectorId = api_key['X-Auth-ConnectorId']

    os.environ['CAROLAPPOAUTH'] = X_Auth_Key
    os.environ['CAROLCONNECTORID'] = X_Auth_ConnectorId


    login = Carol(domain=os.environ['CAROLTENANT'], app_name=os.environ['CAROLAPPNAME'], auth=PwdAuth(user=old_user, password=old_pw))


    revoke = login.api_key_revoke(connector_id=X_Auth_ConnectorId)

    assert revoke['success']









