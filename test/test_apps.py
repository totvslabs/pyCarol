from unittest import mock

import pycarol


def test__init() -> None:
    """Apps() must initialize carol attribute."""
    carol = mock.MagicMock()
    ret = pycarol.Apps(carol)

    assert ret.carol == carol


def test_all() -> None:
    """apps.Apps.all should return whatever /v1/tenantApps responds."""
    mock_apps = mock.MagicMock()
    call_api_ret = {"hits": [{"mdmName": "test"}], "totalHits": 54}
    mock_apps.carol.call_api.return_value = call_api_ret
    ret = pycarol.Apps.all(mock_apps)
    assert ret == {"test": {"mdmName": "test"}}
    assert mock_apps.carol.call_api.mock_calls[0][1] == ("v1/tenantApps",)


def test_get_by_name() -> None:
    """apps.Apps.get_by_name should return whatever /v1/tenantApps/name responds."""
    mock_apps = mock.MagicMock()
    call_api_ret = {"test": "test"}
    mock_apps.carol.call_api.return_value = call_api_ret
    app_name = "test"
    ret = pycarol.Apps.get_by_name(mock_apps, app_name)
    assert ret == call_api_ret
    url = f"v1/tenantApps/name/{app_name}"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)


def test_get_by_id() -> None:
    """apps.Apps.get_by_id should return whatever /v1/tenantApps responds."""
    mock_apps = mock.MagicMock()
    call_api_ret = {"test": "test"}
    mock_apps.carol.call_api.return_value = call_api_ret
    app_id = "test"
    ret = pycarol.Apps.get_by_id(mock_apps, app_id)
    assert ret == call_api_ret
    url = f"v1/tenantApps/{app_id}"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)


def test_get_settings() -> None:
    """apps.Apps.get_settings should return whatever call_api responds."""
    mock_apps = mock.MagicMock()
    call_api_ret = {
        "mdmTenantAppSettingValues": [{"mdmName": "set1", "mdmParameterValue": "val1"}]
    }
    mock_apps.carol.call_api.return_value = call_api_ret
    app_id = "test"
    ret = pycarol.Apps.get_settings(mock_apps, app_id=app_id)
    assert ret == {"set1": "val1"}
    url = f"v1/tenantApps/{mock_apps.current_app_id}/settings"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)


def test_get_manifest() -> None:
    """apps.Apps.get_manifest should return whatever call_api responds."""
    mock_apps = mock.MagicMock()
    call_api_ret = {"test": "test"}
    mock_apps.carol.call_api.return_value = call_api_ret
    app_name = "test"
    ret = pycarol.Apps.get_manifest(mock_apps, app_name=app_name)
    assert ret == call_api_ret
    url = f"v1/tenantApps/manifest/{app_name}"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)


def test_edit_manifest() -> None:
    """apps.Apps.edit_manifest should return whatever call_api responds."""
    mock_apps = mock.MagicMock()
    call_api_ret = {"test": "test"}
    mock_apps.carol.call_api.return_value = call_api_ret
    app_name = "test"
    ret = pycarol.Apps.edit_manifest(mock_apps, {}, app_name=app_name)
    assert ret == call_api_ret
    url = f"v1/tenantApps/manifest/{app_name}"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)


def test_get_git_process() -> None:
    """apps.Apps.get_git_process should return whatever call_api responds."""
    mock_apps = mock.MagicMock()
    call_api_ret = [{"test": "test"}]
    mock_apps.carol.call_api.return_value = call_api_ret
    app_name = "test"
    ret = pycarol.Apps.get_git_process(mock_apps, app_name=app_name)
    assert ret == call_api_ret
    url = f"v1/compute/{app_name}/getProcessesByGitRepo"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)


def test_build_docker_git() -> None:
    """apps.Apps.build_docker_git should return whatever call_api responds."""
    mock_apps = mock.MagicMock()
    manifest = [
        {"dockerName": "name", "dockerTag": "tag", "instanceType": "type"},
        {"dockerName": "name", "dockerTag": "tag", "instanceType": "type"},
    ]
    mock_apps.get_git_process = lambda app_name: manifest
    call_api_ret = {"test": "test"}
    mock_apps.carol.call_api.return_value = call_api_ret
    app_name = "test"
    ret = pycarol.Apps.build_docker_git(mock_apps, "toke", app_name=app_name)
    assert ret == [call_api_ret, call_api_ret]
    url = f"v1/compute/{app_name}/buildGitDocker"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)
    assert mock_apps.carol.call_api.mock_calls[1][1] == (url,)


def test_update_setting_values() -> None:
    """apps.Apps.update_setting_values should return whatever call_api responds."""
    mock_apps = mock.MagicMock()
    app_id = "id"
    mock_apps.get_by_name = lambda x: {"mdmId": app_id}
    settings_id = "settings_id"
    mock_apps._get_app_settings_config = lambda app_id: {"mdmId": settings_id}
    call_api_ret = {"test": "test"}
    mock_apps.carol.call_api.return_value = call_api_ret
    app_name = "test"
    ret = pycarol.Apps.update_setting_values(mock_apps, {}, app_name=app_name)
    assert ret == call_api_ret
    url = f"v1/tenantApps/{app_id}/settings/{settings_id}?publish=true"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)


def test_start_app_process() -> None:
    """apps.Apps.start_app_process should return whatever call_api responds."""
    mock_apps = mock.MagicMock()
    app_id = "id"
    mock_apps.get_by_name = lambda x: {"mdmId": app_id}
    process_id = "process_id"
    mock_apps.get_processes_info = lambda app_name: {"mdmId": process_id}
    call_api_ret = {"test": "test"}
    mock_apps.carol.call_api.return_value = call_api_ret
    app_name = "test"
    process_name = "process_name"
    ret = pycarol.Apps.start_app_process(mock_apps, process_name, app_name=app_name)
    assert ret == call_api_ret
    url = f"v1/tenantApps/{app_id}/aiprocesses/{process_id}/execute/{process_name}"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)


def test_get_processes_info() -> None:
    """apps.Apps.get_processes_info should return whatever call_api responds."""
    mock_apps = mock.MagicMock()
    app_id = "id"
    mock_apps.get_by_name = lambda x: {"mdmId": app_id}
    call_api_ret = {"test": "test"}
    mock_apps.carol.call_api.return_value = call_api_ret
    app_name = "test"
    ret = pycarol.Apps.get_processes_info(mock_apps, app_name=app_name)
    assert ret == call_api_ret
    url = f"v1/tenantApps/{app_id}/aiprocesses"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)


def test_get_subscribable_carol_apps() -> None:
    """apps.Apps.get_subscribable_carol_apps should return whatever call_api responds."""
    mock_apps = mock.MagicMock()
    call_api_ret = {"hits": "test"}
    mock_apps.carol.call_api.return_value = call_api_ret
    ret = pycarol.Apps.get_subscribable_carol_apps(mock_apps)
    assert ret == "test"
    url = "v1/tenantApps/subscribableCarolApps"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)


def test_install_carol_app() -> None:
    """apps.Apps.install_carol_app should return whatever call_api responds."""
    mock_apps = mock.MagicMock()
    app_id = "id"
    call_api_ret = {"mdmId": app_id}
    mock_apps.carol.call_api.return_value = call_api_ret
    app_name = "test"
    app_version = "version"

    mock_apps.get_subscribable_carol_apps = lambda: [
        {"mdmName": app_name, "mdmId": app_id, "mdmAppVersion": app_version}
    ]
    ret = pycarol.Apps.install_carol_app(
        mock_apps, app_name=app_name, app_version=app_version
    )
    assert ret == call_api_ret
    url1 = f"v1/tenantApps/subscribe/carolApps/{app_id}"
    url2 = "v1/tenantApps/id/install"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url1,)
    assert mock_apps.carol.call_api.mock_calls[1][1] == (url2,)


def test_get_app_details() -> None:
    """apps.Apps.get_app_details should return whatever call_api responds."""
    mock_apps = mock.MagicMock()
    call_api_ret = {"test": "test"}
    mock_apps.carol.call_api.return_value = call_api_ret
    app_name = "test"
    app_id = "id"
    mock_apps.get_subscribable_carol_apps = lambda: [
        {"mdmName": app_name, "mdmId": app_id}
    ]
    ret = pycarol.Apps.get_app_details(mock_apps, app_name=app_name)
    assert ret == call_api_ret
    url = f"v1/carolApps/{app_id}/details"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)


def test_upload_file() -> None:
    """apps.Apps.upload_file should return whatever call_api responds."""
    mock_apps = mock.MagicMock()
    app_id = "id"
    mock_apps.get_by_name = lambda x: {"mdmCarolAppId": app_id}
    call_api_ret = {"test": "test"}
    mock_apps.carol.call_api.return_value = call_api_ret
    app_name = "test"
    filepath = "/tmp/file.zip"
    with open(filepath, "w", encoding="utf-8") as file_:
        file_.write("test")

    ret = pycarol.Apps.upload_file(mock_apps, filepath, app_name=app_name)
    assert ret == call_api_ret
    url = f"v1/carolApps/{app_id}/files/upload"
    assert mock_apps.carol.call_api.mock_calls[0][1] == (url,)
