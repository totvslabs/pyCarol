from contextlib import contextmanager
import itertools
import json
import typing as T
import warnings

import pandas as pd

from pycarol import Carol, PwdAuth, PwdKeyAuth
from . import bigquery


@contextmanager
def SQLSocket(carol, *args, **kw):
    """Create Socket connection to Carol.

    Args:
        carol (Carol): Carol instance.
    """
    try:
        from websocket import create_connection
    except ModuleNotFoundError:
        warnings.warn(
            """
            websocket-client is not installed.
            Please install it with:
                pip install websocket-client
        """,
            Warning,
        )
        raise ModuleNotFoundError

    if not isinstance(carol.auth, (PwdAuth, PwdKeyAuth)):
        raise TypeError("Websocket only works with `PwdAuth`, `PwdKeyAuth`")

    def _build_ws_url():
        token = carol.auth._token.access_token
        return f"wss://{carol.organization}.carol.ai/websocket/live/{token}"

    ws = create_connection(_build_ws_url())
    user_id = carol.call_api("v1/users/current")["mdmId"]
    subs = {"action": "SUBSCRIBE", "type": "SQL_RESPONSE", "subtype": user_id}
    ws.send(json.dumps(subs))
    print(json.loads(ws.recv()))
    try:
        yield ws
    finally:
        ws.close()


class SQL:
    def __init__(self, carol):
        self.carol = carol
        warnings.warn("Experimental feature. The API might change without notice.")

    def query(
        self,
        query,
        params=None,
        method="sync",
        dataframe=True,
        service_account=None,
        **kw,
    ):
        """Execute SQL Query.

        Args:
            query: `str`
                SQL Query
            params: `list` default `None`
                Params to send to Carol
            method: `str` default `sync`
                if sync runs the query synchronously and returns the result
                if socket runs the query using web socket and returns the result
            dataframe: `bool` default `True`
                if True returns a pandas dataframe
            service_account: in case you have a service account for accessing BigQuery.
            **kw: `dict`
        Returns:

        """
        methods = ("sync", "socket", "bigquery")

        if method == "socket":
            params = params or []
            payload = {"preparedStatement": query, "params": params}
            results = _socket_query(self.carol, payload, **kw)
        elif method == "sync":
            params = params or []
            payload = {"preparedStatement": query, "params": params}
            results = _sync_query(self.carol, payload)
        elif method == "bigquery":
            return _bigquery_query(
                self.carol, query, service_account=service_account
            )
        else:
            raise ValueError(f"'method' must be either: {methods}")

        if dataframe:
            return pd.DataFrame(results)

        return results


def _socket_query(carol: Carol, payload, **kwargs):
    last_result = False
    results = []
    with SQLSocket(carol, **kwargs) as ws:
        resp = carol.call_api(path="v2/sql/query", method="POST", data=payload)
        while not last_result:
            resp = ws.recv()
            resp = json.loads(resp)
            result = json.loads(resp["message"])
            last_result = result["lastResult"]
            if result["success"]:
                results.extend(result["results"])
            else:
                raise ValueError(f"{result['error']}")
    return results


def _sync_query(carol: Carol, payload):
    results = carol.call_api(path="v2/sql/querySync", method="POST", data=payload)
    if not results[0]["success"]:
        raise ValueError(f"{results[0]['error']}")
    return list(itertools.chain(*[i["results"] for i in results]))


def _bigquery_query(
    carol: Carol,
    query: str,
    service_account: T.Optional[T.Dict[str, str]] = None,
) -> pd.DataFrame:
    """Run query for datamodel.

    Args:
        query: BigQuery SQL query.
        service_account: in case you have a service account for accessing BigQuery.

    Returns:
        Query result.
    """
    if service_account is None:  # must call carol to get service account
        raise NotImplementedError("You must pass a service_account. Not implemented.")

    query = bigquery.prepare_query(carol, query)
    client = bigquery.generate_client(service_account)
    return bigquery.query(client, query)
