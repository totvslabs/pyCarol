"""Back-end for BigQuery-related code."""
import re
import typing as T

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


def query(
    carol: Carol,
    query_: str,
    service_account: T.Optional[T.Dict[str, str]] = None,
):
    """Run query for datamodel.

    Args:
        query_: BigQuery SQL query.
        service_account: in case you have a service account for accessing BigQuery.

    Returns:
        Query result.
    """
    if service_account is None:  # must call carol to get service account
        raise NotImplementedError("You must pass a service_account. Not implemented.")

    query_ = prepare_query(carol, query_)
    client = generate_client(service_account)
    return client.query(query_)


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
    connector_names = {name for name, _ in template_vars if name is not None}
    connector_map = {
        name: connectors.get_by_name(name)["mdmId"] for name in connector_names
    }

    staging_vars = filter(lambda conn_name: conn_name[0] is not None, template_vars)
    model_vars = filter(lambda conn_name: conn_name[0] is None, template_vars)

    replace_map = {}
    for connector_name, table_name in staging_vars:
        key = f"{connector_name}.{table_name}"
        connector_id = connector_map[connector_name]
        replace_map[key] = f"`{tenant_id}.stg_{connector_id}_{table_name}`"
    for _, table_name in model_vars:
        replace_map[table_name] = f"`{tenant_id}.dm_{table_name}`"

    def _replace_func(match) -> str:
        if match.group(2) in replace_map:
            return replace_map[match.group(2)]
        raise ValueError()

    return re.sub(r"({{\s*([0-9A-z\.]+)\s*}})", _replace_func, query_)


REGEX = re.compile(
    r"{{\s*((?P<connector_name>[0-9A-z]+)(\.))?(?P<table_name>[0-9A-z]+.)\s*}}"
)


def _get_template_vars(query_: str) -> T.Set[T.Tuple[str, str]]:
    """Get all variables in the template.

    Variables follow the '{{connector_name.table_name}}' pattern. Connector name is
    optional.

    Args:
        query_: BigQuery SQL query.

    Return:
        Set with connector name (None when there is none) and staging/model name.
    """
    return {
        (match.group("connector_name"), match.group("table_name"))
        for match in REGEX.finditer(query_)
    }
