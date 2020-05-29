import json
import gzip, io
from collections import defaultdict

_FILE_MARKER = '<files>'


def drop_duplicated_parquet_dask(d):
    """
    Merge updates and delete records from the parquet files in CDS.

    Args:
        d: dask DataFrame

    Returns:
        dask DataFrame

    """

    d = d.set_index('mdmCounterForEntity', ) \
        .drop_duplicates(subset='mdmId', keep='last') \
        .reset_index()
    if 'mdmDeleted' in d.columns:
        d['mdmDeleted'] = d['mdmDeleted'].fillna(False)
        d = d[~d['mdmDeleted']]
    d = d.reset_index(drop=True)
    return d


def drop_duplicated_parquet(d):
    """
    Merge updates and delete records from the parquet files in CDS.

    Args:
        d: pd.DataFrame

    Returns:
        pd.DataFrame

    """

    d = d.sort_values('mdmCounterForEntity').reset_index(drop=True).drop_duplicates(subset='mdmId', keep='last')
    if 'mdmDeleted' in d.columns:
        d['mdmDeleted'] = d['mdmDeleted'].fillna(False)
        d = d[~d['mdmDeleted']]
    d = d.reset_index(drop=True)
    return d


class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """

    def default(self, obj):
        import numpy as np
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32,
                              np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def _attach_path(branch, trunk):
    '''
    Insert a branch of directories on its trunk.
    '''
    parts = branch.split('/', 1)
    if len(parts) == 1:  # branch is a file
        trunk[_FILE_MARKER].append(parts[0])
    else:
        node, others = parts
        if node not in trunk:
            trunk[node] = defaultdict(dict, ((_FILE_MARKER, []),))
        _attach_path(others, trunk[node])


def prettify_path(d, indent=0):
    '''
    Print the file tree structure with proper indentation.

    :param: d: `dict`
        list of path to prettify
    :param : indent: `int`, defaut `0`
        Ident to use.
    '''
    for key, value in d.items():
        if key == _FILE_MARKER:
            if value:
                print('  ' * indent + str(value))
        else:
            print('  ' * indent + str(key))
            if isinstance(value, dict):
                prettify_path(value, indent + 1)


def ranges(min_v, max_v, nb):
    if min_v == max_v:
        max_v += 1
    step = int((max_v - min_v) / nb) + 1
    step = list(range(min_v, max_v, step))
    if step[-1] != max_v:
        step.append(max_v)
    step = [[step[i], step[i + 1] - 1] for i in range(len(step) - 1)]
    step.append([max_v, None])
    step = [[None, min_v - 1]] + step
    return step


def stream_data(data, step_size, compress_gzip):
    """

    :param data:  `pandas.DataFrame` or `list of dict`,
        Data to be sliced.
    :param step_size: 'int'
        Number of records per slice.
    :param compress_gzip: 'bool'
        If to compress the data to send
    :return: Generator, cont
        Return a slice of `data` and the count of records until that moment.
    """
    import pandas as pd
    if isinstance(data, pd.DataFrame):
        is_df = True
    else:
        is_df = False
        assert isinstance(data, list)

    data_size = len(data)
    cont = 0
    for i in range(0, data_size, step_size):
        if is_df:
            data_to_send = data.iloc[i:i + step_size]
            cont += len(data_to_send)
            # print('Sending {}/{}'.format(cont, data_size), end='\r')
            data_to_send = data_to_send.to_json(orient='records', date_format='iso', lines=False)
            if compress_gzip:
                out = io.BytesIO()
                with gzip.GzipFile(fileobj=out, mode="w", compresslevel=9) as f:
                    f.write(data_to_send.encode('utf-8'))
                yield out.getvalue(), cont
            else:
                yield json.loads(data_to_send), cont
        else:
            data_to_send = data[i:i + step_size]
            cont += len(data_to_send)
            # print('Sending {}/{}'.format(cont, data_size), end='\r')
            if compress_gzip:
                out = io.BytesIO()
                with gzip.GzipFile(fileobj=out, mode="w", compresslevel=9) as f:
                    f.write(json.dumps(data_to_send, cls=NumpyEncoder).encode('utf-8'))
                yield out.getvalue(), cont
            else:
                yield data_to_send, cont
    return None, None


class Hashabledict(dict):
    def __hash__(self):
        return hash(frozenset(self))
