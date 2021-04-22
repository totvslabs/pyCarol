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


def get_dm_relationship_constraints(carol, connector_name=None, connector_id=None, dm_prefix='DM_', staging_prefix=None):
    """
    Create relationship between data models based on their relationship constraints

    Args:
        carol: `pycarol.Carol`
            Carol() object.
        connector_name: `str`
            Connector Name
        dm_prefix: 'str` default `DM_`
            data model prefix to add to the data model name. e.g., if dm_name='mydatamoldel', the result will be "DM_mydatamoldel`

    Returns: `defaultdict(set)`
        dictionary { "dm1" : {"dm2", "dm3"}} where "dm1" depends on "dm2"" and "dm3"

    """
    staging_prefix, connector_name, connector_id = get_staging_prefix(
        carol, staging_prefix=staging_prefix,
        connector_name=connector_name, connector_id=connector_id
    )

    conn = Connectors(carol)
    conn_to_create = conn.get_all(
        include_mappings=True, include_connectors=True, include_consumption=True)
    conn_to_create = [
        i for i in conn_to_create if i['mdmName'] == connector_name]
    dm_mapping = defaultdict(set)
    for staging_name, mapping in conn_to_create[0]['mdmEntityMappings'].items():
        dm_mapping[dm_prefix + mapping['mdmMasterEntityName']].update([staging_name])

    # find Relationship Constraints
    dm = DataModel(carol)
    dms = dm.get_all().template_dict.keys()

    relationship_constraints = defaultdict(set)
    for i in dms:
        snap = dm.get_by_name(i)['mdmRelationshipConstraints']
        relationship_constraints[dm_prefix + i].update([dm_prefix + j['mdmTargetEntityName'] for j in snap])
        # if there are more than one staging mapping to a DM I need to restrict them too, sinse process staging mean
        # create golden records.
        for parent_staging in dm_mapping[dm_prefix + i]:
            relationship_constraints[staging_prefix+parent_staging].update(
                [dm_prefix + j['mdmTargetEntityName'] for j in snap])

    return relationship_constraints


def get_mapping_constraints(carol, connector_name=None, connector_id=None, dm_prefix='DM_', staging_prefix=None):
    """
    Create relationship between data models and stagings in mappings.

    Args:
        carol: `pycarol.Carol`
            Carol() object.
        connector_name: `str`
            Connector Name
        connector_id: `str`
            Connector ID
        dm_prefix: 'str` default `DM_`
            data model prefix to add to the data model name. e.g., if dm_name='mydatamoldel', the result will be "DM_mydatamoldel`.
            This is only applied for DataModel.
        staging_prefix: 'str` default `None`
            Prefix for the staging name. e.g., if staging_prefix='connector_name` the output will be:
                    { "connector_name_stag3" : {"connector_name_stag1", "connector_name_stag2"}}
            Possible values are: 'connector_name', 'connector_id', None

    Returns: `defaultdict(set)`
        dictionary { "dm1" : {"stag1", "stag2"}} where "dm1" depends on "stag1"" and "stag2"

    """

    lookup_types = ['LOOKUP_DATA_MODEL', 'LOOKUP_STAGING_TABLE', 'LOOKUP_MULTIPLE_DATA_MODELS',
                    'LOOKUP_MULTIPLE_STAGING_TABLES']

    staging_prefix, connector_name, connector_id = get_staging_prefix(
        carol, staging_prefix=staging_prefix,
        connector_name=connector_name, connector_id=connector_id
    )

    conn = Connectors(carol)
    conn_to_create = conn.get_all(
        include_mappings=True, include_connectors=True, include_consumption=True)
    conn_to_create = [
        i for i in conn_to_create if i['mdmName'] == connector_name]

    stag = Staging(carol)

    dm_to_mapping = defaultdict(set)
    for staging_source in conn_to_create[0]['mdmEntityMappings'].keys():
        # staging_source = 'currency'
        curr_map = conn_to_create[0]['mdmEntityMappings'][staging_source]
        assert staging_source == curr_map['mdmStagingType']
        staging_source = curr_map['mdmStagingType']
        dm_target = curr_map['mdmMasterEntityName']
        mapping_id = curr_map['mdmId']
        entity_space = curr_map['mdmEntitySpace']
        connector_id = curr_map['mdmConnectorId']
        mappings_to_get = stag.get_mapping_snapshot(connector_id=connector_id, mapping_id=mapping_id,
                                                    entity_space=entity_space)
        dm_to_mapping[dm_prefix +
                      dm_target].update([staging_prefix + staging_source])

        # Get mapping lookups
        for i in mappings_to_get[None]['fieldMappings']:
            field_cleanse_rules = i['fieldCleanseRules']
            for cleanse in field_cleanse_rules:

                # it can be inside actions and falseActions if inside an If.
                unest_func = list(itertools.chain(*(list(find_keys(cleanse, 'actions'))
                                                    + list(find_keys(cleanse, 'falseActions'))
                                                    + [[cleanse, ]])))

                for cleanse_func in unest_func:
                    if cleanse_func['fieldFunction']['mdmName'] in lookup_types:
                        if "DATA_MODEL" in cleanse_func['fieldFunction']['mdmName']:
                            _prefix = dm_prefix
                            # index 0 sinde it is the only parameter for DM lookups.
                            idx = 0
                        elif "STAGING_TABLE" in cleanse_func['fieldFunction']['mdmName']:
                            # index 1 sinde the first param is the connector name.
                            idx = 1
                            _prefix = staging_prefix

                        dm_to_mapping[staging_prefix + staging_source].update(
                            [_prefix + cleanse_func['parameterValues'][idx]])

    return dm_to_mapping


def get_etl_constraints(carol, connector_name=None, connector_id=None, staging_prefix=None):
    """
    Create relationship between stagings in ETLs.

    Args:
        carol: `pycarol.Carol`
            Carol() object.
        connector_name: `str`
            connector name to get the relationships.
        connector_id: `str`
            connector ID to get the relationships.
        staging_prefix: 'str` default `None`
            Prefix for the staging name. e.g., if staging_prefix='connector_name` the output will be:
                 { "connector_name_stag3" : {"connector_name_stag1", "connector_name_stag2"}}
            Possible values are: 'connector_name', 'connector_id', None

    Returns: `defaultdict(set)`
        dictionary { "stag3" : {"stag1", "stag2"}} where "stag3" depends on "stag1"" and "stag2"

    """

    if connector_id is None and connector_name is None:
        raise ValueError(
            'Either connector_id or connector_name must be set.')

    conn = Connectors(carol)

    staging_prefix, connector_name, connector_id = get_staging_prefix(
        carol, staging_prefix=staging_prefix,
        connector_name=connector_name, connector_id=connector_id
    )

    st = conn.get_all_stagings(
        connector_name=connector_name, connector_id=connector_id)

    etls = conn.get_etl_information(
        connector_name=connector_name, connector_id=connector_id)
    rel_etls = defaultdict(set)
    for etl in etls:
        if etl.get("mdmETLType", "") == 'JOIN':
            source = staging_prefix + etl['mdmSourceEntityName']
            other_side = staging_prefix + etl['mdmOtherSourceEntityName']
            etl_target = list(find_keys(etl, 'mdmParameterValues'))
            etl_target = list(unroll_list(etl_target))

            # get the target table.
            assert len(etl_target) == 1
            etl_target = etl_target[0]

            # get source.
            etl_source = set(itertools.chain(
                *[i.keys() for i in find_keys(etl, 'mdmStagingTypeToFieldForMatching')]))
            etl_source = [staging_prefix + i for i in etl_source]
            rel_etls[staging_prefix + etl_target].update(etl_source)
            # Need to add the sourve->other side to the list. We need to guarantee that the other side is there to do the join.
            rel_etls[source].update({other_side})

        elif etl.get("mdmETLType", "") in ['SPLIT', 'DUPLICATE']:
            # get all actions
            source = staging_prefix + etl['mdmSourceEntityName']
            split_actions = list(find_keys(etl, 'mdmParameterValues'))
            split_actions = set(unroll_list(split_actions))
            for target in split_actions:
                if target in st:
                    rel_etls[staging_prefix+target].update({source})

        else:
            raise ValueError(
                f'mdmETLType are "SPLIT", "DUPLICATE" or "JOIN", {etl.get("mdmETLType", "")} were received')

    return rel_etls


def generate_dependency_graph(carol, connector_list=None, dm_prefix='DM_', staging_prefix='connector_name', only_mapping=False):
    """Generates dependency graph for a list of connectors.

    Args:
        carol (pycarol.Carol): INstance of Carol
        connector_list (list, optional): List of connectors to create the dependency graph. Defaults to None.
        dm_prefix: 'str` default `DM_`
            data model prefix to add to the data model name. e.g., if dm_name='mydatamoldel', the result will be "DM_mydatamoldel`.
            This is only applied for DataModel.
        staging_prefix: 'str` default `None`
            Prefix for the staging name. e.g., if staging_prefix='connector_name` the output will be:
                 { "connector_name_stag3" : {"connector_name_stag1", "connector_name_stag2"}}
            Possible values are: 'connector_name', 'connector_id', None
        only_mapping (bool, optional): Only generates mappings and constraints dependency graph. Defaults to False.

    Returns:
        default: defaultdict(set). Dependency graph.
    """

    if connector_list is None:
        connector_list = Connectors(carol).get_all()
        connector_list = [i['mdmName'] for i in connector_list]

    
    all_rel = defaultdict(set)
    for connector_name in connector_list:

        dm_constraints = get_dm_relationship_constraints(carol, dm_prefix=dm_prefix, connector_name=connector_name, )
        mapping_constraint = get_mapping_constraints(carol, connector_name=connector_name, dm_prefix=dm_prefix, staging_prefix=staging_prefix)
        if not only_mapping:
            etl_constraints = get_etl_constraints(carol, connector_name=connector_name,  staging_prefix=staging_prefix )
        else:
            etl_constraints = defaultdict(set)


        for d in [dm_constraints, etl_constraints, mapping_constraint]:
            for key, value in d.items():
                all_rel[key].update(all_rel[key] | value)

    return all_rel