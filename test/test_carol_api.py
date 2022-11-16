import functools
from pathlib import Path
from unittest import mock

import pycarol


@mock.patch("pycarol.carol._set_host")
@mock.patch("pycarol.carol._prepare_auth")
def test_init_carol_envvar(mock_prepareauth, mock_sethost) -> None:
    tmp_filepath = "/tmp/.env_test"
    with open(tmp_filepath, "w", encoding="utf-8") as file:
        file.write("CAROLAPPNAME=APPNAME\n")
        file.write("CAROL_DOMAIN=ENV\n")
        file.write("CAROLORGANIZATION=ORG\n")
        file.write("CAROLTENANT=TENANT\n")

    carol = pycarol.CarolAPI(dotenv_path=tmp_filepath)

    Path(tmp_filepath).unlink()

    assert hasattr(carol, "datamodel")
    assert isinstance(carol.datamodel, pycarol.data_models.DataModel)
    assert hasattr(carol, "staging")
    assert isinstance(carol.staging, pycarol.staging.Staging)
    assert hasattr(carol, "apps")
    assert isinstance(carol.apps, pycarol.apps.Apps)
    assert hasattr(carol, "cds_golden")
    assert isinstance(carol.cds_golden, pycarol.cds.CDSGolden)
    assert hasattr(carol, "cds_staging")
    assert isinstance(carol.cds_staging, pycarol.cds.CDSStaging)
    assert hasattr(carol, "query")
    assert isinstance(carol.query, functools.partial)
    assert hasattr(carol, "task")
    assert isinstance(carol.task, pycarol.tasks.Tasks)
    assert hasattr(carol, "subscription")
    assert isinstance(carol.subscription, pycarol.subscription.Subscription)
    assert hasattr(carol, "connector")
    assert isinstance(carol.connector, pycarol.connectors.Connectors)
