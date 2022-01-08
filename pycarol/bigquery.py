"""Back-end for BigQuery-related code."""
import re
import copy
import typing as T
from datetime import datetime, timedelta

from google.cloud import bigquery
from google.cloud.bigquery.job.query import QueryJob
from google.oauth2.service_account import Credentials
try:
    import pandas as pd
except ImportError:
    pass

from .carol import Carol
from .connectors import Connectors


class BQ:

    token = None

    def __init__(self, carol: Carol, service_account: dict = None):
        self.carol = carol
        self.service_account = service_account
        self._provided_sa = service_account is not None
        self.env = carol.get_current()
        self.client = None
        self.dataset_id = f"carol-{self.env['env_id'][0:20]}.{self.env['env_id']}"

    def _generate_client(self) -> bigquery.Client:
        """Generate client from credentials."""

        if self._provided_sa:
            service_account = self.service_account
        else:
            service_account = BQ.token['sa']

        credentials = Credentials.from_service_account_info(service_account)
        project = service_account["project_id"]
        client = bigquery.Client(project=project, credentials=credentials)
        return client

    def is_expired(self):

        if self._provided_sa:
            return False
        elif BQ.token is None and self.service_account is None:
            return True
        elif (BQ.token is not None) and ((BQ.token['expiration_time'] < datetime.utcnow()) or (BQ.token['env']['env_id'] != self.env['env_id'])):
            return True
        elif BQ.token is None:
            return True
        else:
            return False

    def get_credential(self, expiration_time: int = 120, force: bool = False) -> dict:
        """ Get service account for BigQuery.

        Args:
            expiration_time (int): Time in hours for credentials to expire. Max value 120.
            force (bool): Force to get new credentials skiping any cache.

        Returns:
            dict: Service account

        .. code:: python


            from pycarol import Carol
            from pycarol.bigquery import BQ

            bq = BQ(Carol())
            service_account = bq.get_credential(expiration_time=120)


        """

        if force or self.is_expired():
            expiration_estimate = datetime.utcnow() + timedelta(hours=expiration_time)

            url = 'v1/create_temporary_key'
            prefix_path = '/sql/v1/api/'
            env = self.carol.get_current()
            payload = {
                "expirationTime": expiration_time,
                "mdmOrgId": env['org_id'],
                "mdmTenantId":  env['env_id']
            }
            self.service_account = self.carol.call_api(
                method='POST', path=url, prefix_path=prefix_path, data=payload)

            BQ.token = {'sa': self.service_account,
                        'expiration_time': expiration_estimate, 'env': copy.deepcopy(self.env)}

        return self.service_account

    def query(
        self,
        query: str,
        dataset_id: T.Optional[str] = None,
        return_dataframe: bool = True,
    ):
        """Run query for datamodel. This will generate a SA if necessary.

        Args:
            query: BigQuery SQL query.
            dataset_id: BigQuery dataset ID.
                if None it will use the default dataset_id.
            return_dataframe: Return dataframe.
                Return dataframe if True.

        Returns:
            Query result.

        Usage:

        .. code:: python


            from pycarol import Carol
            from pycarol.bigquery import BQ

            bq = BQ(Carol())
            query = 'select * from invoice limit 10'	
            df = bq.query(query, return_dataframe=True)

        """

        self.service_account = self.get_credential()
        self.client = self._generate_client()

        dataset_id = dataset_id or self.dataset_id
        job_config = bigquery.QueryJobConfig(default_dataset=dataset_id)
        results = self.client.query(query, job_config=job_config)

        results = [dict(row) for row in results]
        if return_dataframe:
            return pd.DataFrame(results)
        else:
            return results


def query(
    carol: Carol,
    query_: str,
    service_account: T.Optional[T.Dict[str, str]] = None,
    dataset_id: T.Optional[str] = None,
) -> QueryJob:
    """Run query for datamodel.

    Args:
        query_: BigQuery SQL query.
        service_account: in case you have a service account for accessing BigQuery.
        dataset_id: BigQuery dataset ID.

    Returns:
        Query result.
    """
    if service_account is None:  # must call carol to get service account
        raise NotImplementedError(
            "You must pass a service_account. Not implemented.")

    query_ = _prepare_query(carol, query_)
    client = _generate_client(service_account)
    tenant_id = carol.tenant["mdmId"]
    dataset_id = dataset_id or f"labs-app-mdm-production.{tenant_id}"
    job_config = bigquery.QueryJobConfig(default_dataset=dataset_id)
    results = client.query(query_, job_config=job_config)
    return results


def _prepare_query(
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
    connector_names = {name for name, _ in template_vars if name is not None}
    connector_map = {
        name: connectors.get_by_name(name)["mdmId"] for name in connector_names
    }

    staging_vars = filter(
        lambda conn_name: conn_name[0] is not None, template_vars)
    model_vars = filter(lambda conn_name: conn_name[0] is None, template_vars)

    replace_map = {}
    for connector_name, table_name in staging_vars:
        key = f"{connector_name}.{table_name}"
        connector_id = connector_map[connector_name]
        replace_map[key] = f"`stg_{connector_id}_{table_name}`"
    for _, table_name in model_vars:
        replace_map[table_name] = f"`dm_{table_name}`"

    def _replace_func(match) -> str:
        if match.group(2) in replace_map:
            return replace_map[match.group(2)]
        raise ValueError()

    return re.sub(r"({{\s*([0-9A-z\.]+)\s*}})", _replace_func, query_)


def _generate_client(service_account: T.Dict[str, str]) -> bigquery.Client:
    """Generate client from credentials."""
    credentials = Credentials.from_service_account_info(service_account)
    project = service_account["project_id"]
    return bigquery.Client(project=project, credentials=credentials)


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
