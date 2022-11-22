from pathlib import Path
from unittest import mock

import os
import pycarol


def _check_constants(carol: pycarol.Carol) -> None:
    assert carol._current_user is None
    assert carol._tenant is None
    assert carol._user_agent == f"pyCarol/{pycarol.__version__}"
    assert carol.org is None
    assert carol.response is None
    assert carol.session is None


@mock.patch("pycarol.carol._set_host")
@mock.patch("pycarol.carol._prepare_auth")
def test_init_carol_envvar(mock_prepareauth, mock_sethost) -> None:

    tmp_filepath = "/tmp/.env_test"
    with open(tmp_filepath, "w", encoding="utf-8") as file:
        file.write("CAROLAPPNAME=APPNAME\n")
        file.write("CAROL_DOMAIN=ENV\n")
        file.write("CAROLORGANIZATION=ORG\n")
        file.write("CAROLTENANT=TENANT\n")

    carol = pycarol.Carol(dotenv_path=tmp_filepath)

    Path(tmp_filepath).unlink()

    _check_constants(carol)
    assert carol._host_string is None
    assert carol._is_org_level is False
    assert carol.app_name == "APPNAME"
    assert carol.auth == mock_prepareauth.return_value
    assert carol.connector_id == mock_prepareauth.return_value.connector_id
    assert carol.domain == "TENANT"
    assert carol.environment == "ENV"
    assert carol.host == mock_sethost.return_value
    assert carol.organization == "ORG"
    assert carol.port == 443
    assert carol.verbose is False

    del os.environ["CAROLAPPNAME"]
    del os.environ["CAROL_DOMAIN"]
    del os.environ["CAROLORGANIZATION"]
    del os.environ["CAROLTENANT"]


@mock.patch("pycarol.carol._prepare_auth")
@mock.patch("pycarol.carol._set_host")
def test_init_carol_args(mock_set_host, mock_getauth) -> None:
    mock_auth = mock.MagicMock()
    carol = pycarol.Carol(
        "domain",
        "app_name",
        mock_auth,
        "connector_id",
        port=444,
        verbose=True,
        organization="organization",
        environment="environment",
        host="host",
        password="password",
        api_key="api_key",
        org_level=False,
    )

    _check_constants(carol)
    assert carol._host_string == "host"
    assert carol._is_org_level is False
    assert carol.app_name == "app_name"
    assert carol.auth == mock_getauth.return_value
    assert carol.connector_id == mock_getauth.return_value.connector_id
    assert carol.domain == "domain"
    assert carol.environment == "environment"
    assert carol.host == mock_set_host.return_value
    assert carol.organization == "organization"
    assert carol.port == 444
    assert carol.verbose is True


@mock.patch("pycarol.carol._prepare_auth")
@mock.patch("pycarol.carol._set_host")
def test_init_carol_exceptions(mock_set_host, mock_getauth) -> None:
    try:
        pycarol.Carol()
        assert "Should have thrown exception" == ""
    except Exception as exception:
        assert isinstance(exception, pycarol.exceptions.MissingInfoCarolException)


def test_set_host() -> None:
    ret = pycarol.carol._set_host("environment", "domain", "host", "org")
    assert ret == "host"
    ret = pycarol.carol._set_host("environment", "domain", None, "org")
    assert ret == "org.environment"
    ret = pycarol.carol._set_host("environment", "domain", None, None)
    assert ret == "domain.environment"


@mock.patch("pycarol.carol.ApiKeyAuth")
@mock.patch("pycarol.carol.PwdAuth")
def test_set_auth(mock_pwdauth, mock_apikeyauth) -> None:
    try:
        pycarol.carol._prepare_auth()
        assert "Should have thrown exception" == ""
    except Exception as exception:
        assert isinstance(exception, pycarol.exceptions.MissingInfoCarolException)

    auth = mock.MagicMock()
    ret = pycarol.carol._prepare_auth(auth=auth)
    assert ret == auth

    ret = pycarol.carol._prepare_auth(app_oauth="oauth")
    assert ret == mock_apikeyauth.return_value

    ret = pycarol.carol._prepare_auth(username="user", password="pass")
    assert ret == mock_pwdauth.return_value


def test_get_current() -> None:
    carol = mock.MagicMock()
    carol._current_env = lambda: {"mdmName": "name1", "mdmId": "id1"}
    carol._current_org = lambda: {"mdmName": "name2", "mdmId": "id2"}
    try:
        pycarol.carol.Carol.get_current(carol, "coco")
        assert "Should have thrown exception" == ""
    except Exception as exception:
        assert isinstance(exception, ValueError)

    ret = pycarol.carol.Carol.get_current(carol, "env")
    assert ret["env_name"] == "name1"
    assert ret["env_id"] == "id1"
    assert ret["org_name"] is None
    assert ret["org_id"] is None

    ret = pycarol.carol.Carol.get_current(carol, "all")
    assert ret["env_name"] == "name1"
    assert ret["env_id"] == "id1"
    assert ret["org_name"] == "name2"
    assert ret["org_id"] == "id2"


@mock.patch("pycarol.carol._set_host")
@mock.patch("pycarol.carol.Organization")
@mock.patch("pycarol.carol.Tenant")
@mock.patch("pycarol.carol.Carol.get_current")
def test_switch_env(mock_getcurrent, mock_tenant, mock_org, mock_sethost) -> None:
    carol = mock.MagicMock()
    carol._current_org = lambda: {"mdmName": "name"}
    carol.org = None
    carol_ = pycarol.carol.Carol.switch_environment(carol)
    assert carol_.org == mock_org.return_value.get_organization_info.return_value
    assert carol_.app_name is None
    assert carol_.domain is None
    assert carol_.host == mock_sethost.return_value
    assert carol_.organization == "name"
    assert carol_._tenant is None
    assert carol_._is_org_level is True

    carol2 = mock.MagicMock()
    carol2._current_org = lambda: {"mdmName": "name"}
    carol2.org = None
    carol_ = pycarol.carol.Carol.switch_environment(
        carol2, org_id="org", env_name="name"
    )
    assert carol_.org == mock_org.return_value.get_organization_info.return_value
    assert carol_.app_name is None
    assert carol_.domain == "name"
    assert carol_.host == mock_sethost.return_value
    assert carol_.organization == "name"
    assert carol_._tenant == carol2._current_env.return_value
    assert carol_._is_org_level is False
