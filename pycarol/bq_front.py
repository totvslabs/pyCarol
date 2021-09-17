import typing as T

import pandas as pd

from . import bigquery


def query(
    self,
    query_: str,
    through_carol: bool = False,
    service_account: T.Optional[T.Dict[str, str]] = None,
) -> pd.DataFrame:
    """Run query for datamodel.

    Args:
        query_: BigQuery SQL query.
        through_carol: if query will pass through Carol endpoint or access Google
            directly.
        service_account: in case you have a service account for accessing BigQuery.

    Returns:
        Query result.
    """
    if through_carol:  # must return
        raise Exception("through_carol is not implemented.")

    if service_account is None:  # must call carol to get service account
        raise Exception("You must pass a service_account. Not implemented.")

    template_vars = bigquery.get_template_vars(query_)
    if len(template_vars) > 0:
        tenant_id = self.carol.tenant["mdmId"]
        connector_names = {name_ for name_, _ in template_vars if name_ is not None}
        connector_map = {
            name_: self._connector_by_name(name_) for name_ in connector_names
        }
        query_ = bigquery.prepare_query(
            query_, tenant_id, template_vars, connector_map
        )

    client = bigquery.generate_client(service_account)
    return bigquery.query(client, query_)
