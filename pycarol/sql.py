import warnings
from pycarol import PwdAuth, PwdKeyAuth
from contextlib import contextmanager
import json
import itertools


@contextmanager
def SQLSocket(carol, *args, **kw):
    """
    Create Socket connection to Carol.

    Args:
        carol (Carol): Carol instance.
    
    """
    try:
        from websocket import create_connection
    except ModuleNotFoundError:
        warnings.warn("""
            websocket-client is not installed.
            Please install it with:
                pip install websocket-client
        """, Warning)
        raise ModuleNotFoundError
    
    if not isinstance(carol.auth, (PwdAuth, PwdKeyAuth)):
        raise TypeError("Websocket only works with `PwdAuth`, `PwdKeyAuth`")
    def build_ws_url():
        return f'wss://{carol.organization}.carol.ai/websocket/live/{carol.auth._token.access_token}'
    
    ws = create_connection(build_ws_url())
    user_id = carol.call_api('v1/users/current')['mdmId']
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
        warnings.warn("""
        Experimental feature. The API might change without notice.
        """)

            
    def query(self, query, params=None, method='sync', dataframe=True,  **kw):
        
        """
        Execute SQL Query

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
            **kw: `dict`
        Returns:
        
        """
        if method not in ('sync', 'socket'):
            raise ValueError("kind must be either 'sync' or 'socket'")
        
        params = params or []
        payload = {"preparedStatement": query, "params": params} 
        
        if method == 'socket':
            last_result = False
            results = []
            with SQLSocket(self.carol, **kw) as ws:
                resp = self.carol.call_api(path='v2/sql/query', method='POST', data=payload)
                while not last_result:
                    resp = ws.recv()
                    resp = json.loads(resp)
                    result = json.loads(resp['message'])
                    last_result = result['lastResult']
                    if result['success']:
                        results.extend(result['results'])
                    else:
                        raise ValueError(f"{result['error']}")
            
        elif method == 'sync':
            results = self.carol.call_api(path='v2/sql/querySync', method='POST', data=payload)
            if not results[0]['success']:
                raise ValueError(f"{results[0]['error']}")
            results = list(itertools.chain(*[i['results'] for i in results]))
        if dataframe:
            import pandas as pd
            return pd.DataFrame(results)
        else:
            return results