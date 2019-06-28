from unittest.mock import patch, Mock

def test_mock_password_login():
    from pycarol import PwdAuth, Carol

    mock_tenant = patch('pycarol.tenant.Tenant.get_tenant_by_domain')
    mock_get_tenant = mock_tenant.start()
    mock_get_tenant.return_value = Mock(status_code=200)
    mock_get_tenant.return_value.json.return_value = {}

    """Mocking a whole function"""
    mock_get_patcher = patch('pycarol.carol.Carol.call_api')
    response = {
        "access_token": "213133213231321231",
        "refresh_token": "asdasdasdads23123",
        "timeIssuedInMillis": 123456,
        "expires_in": 3600
    }

    mock_get = mock_get_patcher.start()
    mock_get.return_value = response

    login = Carol(domain="tenant", app_name="app", auth=PwdAuth(user="a@pycarol.com.br", password="12345"))

    assert login.auth._token.access_token is not None
    assert login.auth._token.refresh_token is not None
    assert login.auth._token.expiration is not None

    mock_get_patcher.stop()
    mock_tenant.stop()
