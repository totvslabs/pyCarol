import json
import gzip, io, zipfile, os
from collections import defaultdict
from pathlib import Path
_FILE_MARKER = '<files>'


def drop_duplicated_parquet_dask(d, untie_field='mdmCounterForEntity'):
    """
    Merge updates and delete records from the parquet files in CDS.

    Args:
        d: dask DataFrame
        untie_field: str
            Field to be used to untie records with the same `mdmId`. 

    Returns:
        dask DataFrame

    """

    if untie_field not in d.columns:
        #Use the standard one. 
        untie_field = 'mdmCounterForEntity'


    d = d.set_index(untie_field, ) \
        .drop_duplicates(subset='mdmId', keep='last') \
        .reset_index()
    if 'mdmDeleted' in d.columns:
        d['mdmDeleted'] = d['mdmDeleted'].fillna(False)
        d = d[~d['mdmDeleted']]
    d = d.reset_index(drop=True)
    return d


def drop_duplicated_parquet(d, untie_field='mdmCounterForEntity'):
    """
    Merge updates and delete records from the parquet files in CDS.

    Args:
        d: pd.DataFrame
            Dataframe
        untie_field: str
            Field to be used to untie records with the same `mdmId`. 

    Returns:
        pd.DataFrame

    """

    if untie_field not in d.columns:
        untie_field = 'mdmCounterForEntity'

    d = d.sort_values(untie_field).reset_index(drop=True).drop_duplicates(subset='mdmId', keep='last')
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
    if isinstance(data, list):
        is_df = False
    else:
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            is_df = True

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


def zip_folder(path_to_zip):
    path_to_zip = os.path.abspath(path_to_zip)
    path_to_zip = Path(path_to_zip)
    base_name = path_to_zip.name

    zip_file = Path(path_to_zip).with_suffix('.zip')
    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED, ) as z:
        for f in list(path_to_zip.rglob('*.*')):
            z.write(f, arcname=f.relative_to(path_to_zip.parents[0]))

    return zip_file

def find_keys(node, kv):
    """Find values of a key in a nested dictionary.

    Args:

        node (dict): Dictionary to search.
        kv (str): key to search.

    Yields:
        str: values.

    """
    if isinstance(node, list):
        for i in node:
            yield from find_keys(i, kv)
    elif isinstance(node, dict):
        if kv in node:
            yield node[kv]
        for j in node.values():
            yield from find_keys(j, kv)
                
def unroll_list(l):
    """Unroll a list of lists.

    Args:
        l (list): list of list to unroll.

    Yields:
        list: unrolled list.
    """

    if isinstance(l, list):
        for i in l:
            yield from unroll_list(i)
    else:
        yield l