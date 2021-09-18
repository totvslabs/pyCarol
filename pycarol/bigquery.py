import re
import typing as T

import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

from .carol import Carol
from .connectors import Connectors


def get_service_account() -> T.Dict[str, str]:
    """Get BigQuery credentials from Carol."""
    ...


def generate_client(service_account: T.Dict[str, str]) -> bigquery.Client:
    """Generate client from credentials."""
    credentials = Credentials.from_service_account_info(service_account)
    return bigquery.Client(project="labs-app-mdm-production", credentials=credentials)


def query(client: bigquery.Client, query_: str) -> pd.DataFrame:
    """Run query."""
    return client.query(query_).to_dataframe()


def prepare_query(
    carol: Carol,
    query_: str,
) -> str:
    """Render template replacing variables (if any) with values.

    {{connector_name.staging_table}} is replaced by:
        `TENANTID.stg_CONNECTORID_STAGINGNAME`
    {{datamodel_name}} is replaced by `TENANTID.dm_MODELNAME`
    Variables must follow the '{{variable}}' pattern.

    Args:
        carol: Carol object.
        query_: BigQuery SQL query.

    Return:
        Query string with template rendered.
    """
    template_vars = _get_template_vars(query_)
    if len(template_vars) == 0:
        return query_

    connectors = Connectors(carol)
    tenant_id = carol.tenant["mdmId"]
    connector_names = {name_ for name_, _ in template_vars if name_ is not None}
    connector_map = {
        name_: connectors.get_by_name(name_)["mdmId"] for name_ in connector_names
    }

    replace_map = {}
    for connector_name, table_name in template_vars:
        if connector_name is not None:
            key = f"{connector_name}.{table_name}"
            connector_id = connector_map[connector_name]
            value = f"`{tenant_id}.stg_{connector_id}_{table_name}`"
        else:
            key = table_name
            value = f"`{tenant_id}.dm_{table_name}`"
        replace_map[key] = value

    def _replace_func(match) -> str:
        if match.group(2) in replace_map:
            return replace_map[match.group(2)]
        raise ValueError()

    return re.sub(r"({{\s*([0-9A-z\.]+)\s*}})", _replace_func, query_)


def _get_template_vars(query_: str) -> T.Set[T.Tuple[str, str]]:
    """Get all variables in the template.

    Variables follow the '{{connector_name.table_name}}' pattern. Connector name is
    optional.

    Args:
        query_: BigQuery SQL query.

    Return:
        Set with connector name (None when there is none) and staging/model name.
    """
    regex = re.compile(
        "{{\s*((?P<connector_name>[0-9A-z]+)(\.))?(?P<table_name>[0-9A-z]+.)\s*}}"
    )
    return {
        (match.group("connector_name"), match.group("table_name"))
        for match in regex.finditer(query_)
    }
