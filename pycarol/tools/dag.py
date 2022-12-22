from pycarol import Carol, Staging, Connectors, DataModel
from collections import defaultdict
import itertools


def find_keys(node, kv):
    """Find recursively all the values from a given key. 

    Args:
        node (dict of dict): Nested dictionary.
        kv (str): dictionary key to find. 

    Yields:
        dict: dictionary
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
    """Unroll a list of lists to a flat list

    Args:
        l (list of lists): List of list to unroll

    Yields:
        list: unrolled list.
    """

    if isinstance(l, list):
        for i in l:
            yield from unroll_list(i)
    else:
        yield l


def get_staging_prefix(carol, staging_prefix, connector_name, connector_id):
    """Create the prefix for a given set of staging_prefix, connector_name, connector_id

    Args:
        carol: `pycarol.Carol`
            Carol() object.
        connector_name: `str`
            Connector Name
        prefix: 'str` default `DM_`
            prefix to add to the data model name. e.g., if dm_name='mydatamoldel', the result will be "DM_mydatamoldel`.
            This is only applied for DataModel.
        staging_prefix: 'str` default `None`
            Prefix for the staging name. e.g., if staging_prefix='connector_name` the output will be:
                    { "connector_name_stag3" : {"connector_name_stag1", "connector_name_stag2"}}
            Possible values are: 'connector_name', 'connector_id', None

    """
    conn = Connectors(carol)
    if staging_prefix is None:
        staging_prefix = ''
    elif staging_prefix == 'connector_name':
        if connector_name is None:
            connector_name = conn.get_by_id(connector_id)['mdmName']
        staging_prefix = connector_name + '_'
    elif staging_prefix == 'connector_id':
        if connector_id is None:
            connector_id = conn.get_by_name(connector_name)['mdmId']
        staging_prefix = connector_id + '_'
    else:
        raise ValueError(
            f'`staging_prefix` should be either `connector_name`, `connector_id` or `None` {staging_prefix} was used'
        )
    return staging_prefix, connector_name, connector_id
