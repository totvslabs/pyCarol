"""Front-end for all SQL-related operations."""
from contextlib import contextmanager
import itertools
import json
import typing as T
import warnings

try:  # dataframe
    import pandas as pd
except ModuleNotFoundError:
    ...
try:  # extra
    from websocket import create_connection  # pylint: disable=import-error

    WEBSOCKET_IMPORTED = True
except ModuleNotFoundError:
    WEBSOCKET_IMPORTED = False

from pycarol import Carol, PwdAuth, PwdKeyAuth
from . import bigquery


@contextmanager
def SQLSocket(carol: Carol, *args, **kw):
    """Create Socket connection to Carol.

    Args:
        carol (Carol): Carol instance.
    """
    if not WEBSOCKET_IMPORTED:
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
    def __init__(self, carol: Carol):
        self.carol = carol
        warnings.warn("Experimental feature. The API might change without notice.")

    def query(
        self,
        query: str,
        params: T.Optional[T.List] = None,
        method: str = "sync",
        dataframe: bool = True,
        service_account: T.Optional[T.Dict[str, str]] = None,
        dataset_id: T.Optional[str] = None,
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
                if bigquery runs the query using bigquery and returns the result
            dataframe: `bool` default `True`
                if True returns a pandas dataframe
            service_account: in case you have a service account for accessing BigQuery.
            dataset_id: overwrites the default dataset_id when using bigquery.
            **kw: `dict`
        Returns:

        """
        methods = ("sync", "socket", "bigquery")

        params = params or []
        payload = {"preparedStatement": query, "params": params}
        if method == "socket":
            results = _socket_query(self.carol, payload, **kw)
        elif method == "sync":
            results = _sync_query(self.carol, payload)
        elif method == "bigquery":
            results = bigquery.query(self.carol, query, service_account, dataset_id=dataset_id)
        else:
            raise ValueError(f"'method' must be either: {methods}")

        if method == "bigquery":
            results = [dict(row) for row in results]
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
