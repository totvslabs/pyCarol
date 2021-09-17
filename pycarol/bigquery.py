import re
import typing as T

import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials


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


def get_template_vars(query_: str) -> T.Set[T.Tuple[str, str]]:
    regex = re.compile(
        "{{\s*((?P<connector_name>[0-9A-z]+)(\.))?(?P<table_name>[0-9A-z]+.)\s*}}"
    )
    return {
        (match.group("connector_name"), match.group("table_name"))
        for match in regex.finditer(query_)
    }


def prepare_query(
    query_: str,
    tenant_id: str,
    template_vars: T.Set[T.Tuple[str, str]],
    connector_map: T.Dict[str, str],
) -> str:
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
