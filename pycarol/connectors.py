import json
from collections import defaultdict
from .utils.deprecation_msgs import _deprecation_msgs


class Connectors:
    """
    This class handle all APIs related to Carol connectors

    Args:

        carol: class: pycarol.Carol

    """

    def __init__(self, carol):
        self.carol = carol

    def create(self, name, label=None, group_name="Others", overwrite=False):
        """
        Create a connector

        Args:

            name: 'str'
                Connector name
            label: 'str'
                Connector label in UI
            group_name: `str` default "Others"
                Connector group name in UI
            overwrite: `bool` default `False`
                Overwrite if already exists.
        """

        if label is None:
            label = name

        resp = self.carol.call_api('v1/connectors', data={
            'mdmName': name,
            'mdmGroupName': group_name,
            'mdmLabel': {"en-US": label}
        }, errors='ignore')
        if resp.get('mdmId') is not None:
            return resp.get('mdmId')
        if ('already exists' in resp.get('errorMessage', [])):
            if overwrite:
                self.delete_by_name(name)
                return self.create(name, label, group_name, False)
            else:
                return self.get_by_name(name)['mdmId']

        else:
            raise Exception(resp)

    def get_by_name(self, name, errors='raise'):
        """
        Get connector information using the connector name

        Args:

            name: 'str'
                Connector Name
            errors: {‘ignore’, ‘raise’}, default ‘raise’
                If ‘raise’, then invalid request will raise an exception If ‘ignore’,
                then invalid request will return the request response


        Returns: `dict`
            connector information.

        """

        resp = self.carol.call_api(f'v1/connectors/name/{name}', errors=errors)
        return resp

    def get_by_id(self, id, errors='raise'):
        """
        Get connector information using the connector name

        Args:

            id: 'str'
                Connector ID
            errors: {‘ignore’, ‘raise’}, default ‘raise’
                If ‘raise’, then invalid request will raise an exception If ‘ignore’,
                then invalid request will return the request response


        Returns: `dict`
            connector information.

        """

        resp = self.carol.call_api(f'v1/connectors/{id}', errors=errors)
        return resp

    def delete_by_name(self, name, force_deletion=True):
        """
        Delete connector by name

        Args:

            name: `str`
                Connector name
            force_deletion: `bool` default `True`
                Force the deletion

        Returns: None

        """

        mdm_id = self.get_by_name(name)['mdmId']
        self.delete_by_id(mdm_id, force_deletion)

    def delete_by_id(self, connector_id=None, mdm_id=None, force_deletion=True):
        """
        Delete Connector by ID

        Args:

            connector_id: `str``
                Connector ID
            mdm_id: `str``
                Connector ID
            force_deletion: `bool` default `True`
                Force the deletion

        Returns: None

        """

        if connector_id is None and mdm_id is not None:
            _deprecation_msgs(
                "mdm_id is deprecated and will be removed, use connector_id")

        connector_id = connector_id if connector_id else mdm_id

        if connector_id is None:
            raise ValueError('Connector Id must be set')

        self.carol.call_api(
            f'v1/connectors/{connector_id}?forceDeletion={force_deletion}', method='DELETE')

    def get_all(self, offset=0, page_size=-1, sort_order='ASC', sort_by=None, include_connectors=False,
                include_mappings=False, include_consumption=False, print_status=True, save_results=False,
                filename='connectors.json'):
        """
        Get all connectors.

        Args:

            offset: `int`, default `0`
                Offset for the response.
            page_size: `int`, default `1000`
                Number of records in each call.
            sort_by: `str` default `None`
                Field to sort by
            sort_order: `str`, default `ASC`
                Sort Order. Possible values "ASC" and "DESC"
            include_connectors: `bool` default `False`
                Include connector information
            include_mappings: `bool` default `False`
                Include mapping information
            include_consumption: `bool` default `False`
                Include consumption information
            print_status: `bool` default `True`
                Print status of the request.
            save_results: `bool` default `False`
                If save json with the results.
            filename: `str` default `None`
                Filename to save

        Returns: `dict`
            Connector information

        """

        params = {"offset": offset, "pageSize": str(page_size), "sortOrder": sort_order,
                  "includeMappings": include_mappings, "includeConsumption": include_consumption,
                  "includeConnectors": include_connectors}

        if sort_by is not None:
            params['sortBy'] = sort_by

        connectors = []
        set_param = True
        count = offset
        total_hits = float("inf")
        if save_results:
            file = open(filename, 'w', encoding='utf8')
        while count < total_hits:
            conn = self.carol.call_api('v1/connectors', params=params)
            count += conn['count']
            if set_param:
                total_hits = conn["totalHits"]
                set_param = False
            conn = conn['hits']

            if not conn:
                break

            connectors.extend(conn)
            params['offset'] = count
            if print_status:
                print('{}/{}'.format(count, total_hits), end='\r')
            if save_results:
                file.write(json.dumps(conn, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_results:
            file.close()
        return connectors

    def stats(self, connector_id=None, connector_name=None, all_connectors=False):
        """
        Get connector stats

        Args:

            connector_id: `str`, default `None`
                Connector Id
            connector_name: `str`, default `None`
                Connectot name
            all_connectors: `bool` default `False`
                Get from all connectors.

        Returns: `dict`
            Dict with the status of the connectors.

        """

        if all_connectors:
            response = self.carol.call_api('v1/connectors/stats/all')
        else:
            if connector_name:
                connector_id = self.get_by_name(connector_name)['mdmId']
            else:
                assert connector_id

            response = self.carol.call_api(
                'v1/connectors/{}/stats'.format(connector_id))

        self._conn_stats = response['aggs']
        return {key: list(value['stagingEntityStats'].keys()) for key, value in self._conn_stats.items()}

    def staging_to_connectors_map(self):
        """
        Create a dictionary where the mapping of connectors and stagings.

        Returns: `dict`
            Dict

        """

        d = defaultdict(list)
        connectors = self.get_all(print_status=False)
        for connector in connectors:
            current_connector = connector['mdmId']
            conn_stats = self.stats(current_connector)
            for i in conn_stats[current_connector]:
                d[i].append(current_connector)

        return d

    def find_by_staging(self, staging_name=None):
        """
        Find connector given a staging table

        Args:

            staging_name: `str` default `None`
                Staging table name

        Returns: `dict`
            Connector information

        """

        d = self.staging_to_connectors_map()

        if staging_name:
            conn = d.get(staging_name, None)
            if conn is None:
                raise ValueError(
                    'There is no staging named {}'.format(staging_name))

            elif len(conn) > 1:
                print('More than one connector with the staging {}'.format(
                    staging_name))
                return conn

    def get_dm_mappings(self, connector_id=None, connector_name=None, staging_name=None,
                        dm_id=None, dm_name=None, reverse_mapping=False, offset=0, page_size=1000, sort_by=None,
                        sort_order='ASC', all_connectors=False):
        """
        Get data models mappings information.

        Args:

            connector_id: `str`, default `None`
                Connector Id
            connector_name: `str`, default `None`
                Connectot name
            staging_name: `str`, default `None`
                Staging name
            dm_id: `str`, default `None`
                Data model Id
            dm_name: `str`, default `None`
                Data model name
            reverse_mapping: `bool`, default `False`
                If to return the reverse mapping.
            offset: `int`, default `0`
                Offset for the response.
            page_size: `int`, default `1000`
                Number of records in each call.
            sort_by: `str` default `None`
                Field to sort by
            sort_order: `str`, default `ASC`
                Sort Order. Possible values "ASC" and "DESC"
            all_connectors: `bool`, default `False`
                It will return all the mapping for all connectors/stagings

        Returns: `dict`
            Mapping json definition

        """

        if all_connectors:
            payload = {
                "offset": offset,
                "sortBy": sort_by,
                "pageSize": page_size,
                "sortOrder": sort_order
            }
            url = "v1/connectors/mappings/all"

        else:
            if connector_id is None and connector_name is None:
                raise ValueError(
                    'Either connector_id or connector_name must be set if `all_connectors=False`. ')
            connector_id = connector_id if connector_id else self.get_by_name(connector_name)[
                'mdmId']

            if dm_name is not None:
                url_dm = f"v1/entities/templates/name/{dm_name}"
                dm_id = self.carol.call_api(url_dm, method='GET')['mdmId']

            payload = {
                "reverseMapping": reverse_mapping,
                "entityId": dm_id,
                "stagingType": staging_name,
                "offset": offset,
                "sortBy": sort_by,
                "pageSize": page_size,
                "sortOrder": sort_order
            }

            url = f"v1/connectors/{connector_id}/entityMappings"

        set_param = True
        to_get = float('inf')
        count = 0
        self.resp = []
        while count < to_get:

            resp = self.carol.call_api(url, method='GET', params=payload)
            if set_param:
                self.total_hits = resp["totalHits"]
                to_get = resp["totalHits"]
                set_param = False

            count += resp['count']
            query = resp['hits']
            if not query:
                break

            self.resp.extend(query)
            payload['offset'] = count

        return self.resp

    def get_entity_mappings(
            self, connector_name=None, connector_id=None,
            reverse_mapping=False, staging_name=None,
            offset=0, page_size=1000, sort_order='ASC',
            sort_by=None, print_status=False, errors='raise'
    ):
        """
        Get all Entity Mappings.

        Args:
            connector_name: `str`, `str`, default `None`
                Connector Name
            connector_id: `str`, `str`, default `None`
                Connector ID
            reverse_mapping: `bool` default `False`
                When using with consumer.False if you don't know what consumer is.
            staging_name: str` default None
                Name of the staging to find the mapping. Returns 404  if there is not mapping..
            offset: `int`, default 0
                Offset for pagination. Only used when `scrollable=False`
            page_size: `int`, default 100
                Number of records downloaded in each pagination. The maximum value is 1000
            sort_order: `str`, default 'ASC'
                Sort ascending ('ASC') vs. descending ('DESC').
            sort_by: `str`,  default `None`
                Name to sort by.
            print_status: `bool`, default `False`
                Print the number of records in each interaction.
            errors: {‘ignore’, ‘raise’}, default ‘raise’
                If ‘raise’, then invalid request will raise an exception If ‘ignore’,
                then invalid request will return the request response.

        Returns: list of dict
            List of dict with mappings,
        """

        connector_id = connector_id if connector_id else self.get_by_name(connector_name)[
            'mdmId']

        template_data = []
        count = offset
        query_params = {
            "offset": offset, "pageSize": str(page_size),
            "sortOrder": sort_order,
            "sortBy": sort_by,
            'stagingType': staging_name,
            'reverseMapping': reverse_mapping,
        }

        set_param = True
        total_hits = float("inf")
        while count < total_hits:
            query = self.carol.call_api(path=f'v1/connectors/{connector_id}/entityMappings', method="GET",
                                        params=query_params, errors=errors)

            if query.get('hits') is None:
                if len(template_data) == 0:
                    # when errors==ignore it will return the error msg.
                    return query
                else:
                    return template_data

            if query['count'] == 0:
                print('There are no more results.')
                print(f'Expecting {total_hits}, response = {count}')
                break
            count += query['count']
            if set_param:
                total_hits = query["totalHits"]
                set_param = False

            query = query['hits']
            template_data.extend(query)

            query_params['offset'] = count
            if print_status:
                print(f'{count}/{total_hits}', end='\r')

        return template_data

    def _play_pause_mapping(self, kind, entity_mapping_id=None, staging_name=None,
                            connector_name=None, connector_id=None,
                            reverse_mapping=False,
                            process_cds=True, ):

        connector_id = connector_id if connector_id else self.get_by_name(connector_name)[
            'mdmId']

        if entity_mapping_id is None:
            if staging_name is None:
                raise ValueError(
                    "Either staging_name or entity_mapping_id must be set.")
            entity_mappings = self.get_entity_mappings(
                connector_id=connector_id, staging_name=staging_name)
            entity_mappings = [i['mdmId'] for i in entity_mappings]
        else:
            if isinstance(entity_mapping_id, str):
                entity_mappings = [entity_mapping_id]
            elif isinstance(entity_mapping_id, list):
                entity_mappings = entity_mapping_id
            else:
                raise ValueError(
                    'entity_mapping_id must be string of list of string.')

        responses = {}
        for _mapping in entity_mappings:
            entity_mapping_id = _mapping

            params = {
                'reverseMapping': reverse_mapping,
                'processCds': process_cds,
            }

            resp = self.carol.call_api(path=f'v1/connectors/{connector_id}/entityMappings/{entity_mapping_id}/{kind}',
                                       method="POST", params=params, )
            responses[entity_mapping_id] = resp

        return responses

    def play_mapping(
            self, entity_mapping_id=None, staging_name=None,
            connector_name=None, connector_id=None,
            reverse_mapping=False,
            process_cds=True,
    ):
        """
        Start mapping.

        Args:
            entity_mapping_id: `str` or list of strings
                Mapping ids to be resumed.
            staging_name:
                Staging name for starting the mapping
            connector_name: `str`, `str`, default `None`
                Connector Name
            connector_id: `str`, `str`, default `None`
                Connector ID
            reverse_mapping: `bool` default `False`
                When using with consumer.False if you don't know what consumer is.
            process_cds: `bool` default `True`
                Process pending records after play.

        Returns: dict
         Dictionary with the response of all mappings played.

        """

        responses = self._play_pause_mapping(
            kind='play', entity_mapping_id=entity_mapping_id, staging_name=staging_name,
            connector_name=connector_name, connector_id=connector_id,
            reverse_mapping=reverse_mapping,
            process_cds=process_cds,
        )

        return responses

    def pause_mapping(
            self, entity_mapping_id=None, staging_name=None,
            connector_name=None, connector_id=None,
            reverse_mapping=False,
    ):
        """
        Pause mapping.

        Args:
            entity_mapping_id: `str` or list of strings
                Mapping ids to be stopped.
            staging_name:
                Staging name to stop the mappings
            connector_name: `str`, `str`, default `None`
                Connector Name
            connector_id: `str`, `str`, default `None`
                Connector ID
            reverse_mapping: `bool` default `False`
                When using with consumer.False if you don't know what consumer is.

        Returns: dict
         Dictionary with the response of all mappings played.

        """

        responses = self._play_pause_mapping(
            kind='pause', entity_mapping_id=entity_mapping_id, staging_name=staging_name,
            connector_name=connector_name, connector_id=connector_id,
            reverse_mapping=reverse_mapping,
        )

        return responses

    def _play_pause_etl(self, kind, staging_name=None,
                        connector_name=None, connector_id=None, ):

        connector_id = connector_id if connector_id else self.get_by_name(connector_name)[
            'mdmId']
        resp = self.carol.call_api(
            path=f'v1/etl/staging/{connector_id}/{staging_name}/{kind}', method='POST')

        return resp

    def play_etl(
            self, staging_name=None,
            connector_name=None, connector_id=None,
    ):
        """
        Start ETL processes.

        Args:
            staging_name:
                Staging name for starting the ETLs
            connector_name: `str`, `str`, default `None`
                Connector Name
            connector_id: `str`, `str`, default `None`
                Connector ID

        Returns: dict
         Dictionary with the response of all mappings played.

        """

        responses = self._play_pause_etl(
            kind='play', staging_name=staging_name,
            connector_name=connector_name, connector_id=connector_id,
        )

        return responses

    def pause_etl(
            self, staging_name=None,
            connector_name=None, connector_id=None,
    ):
        """
        Pause ETL.

        Args:
            staging_name:
                Staging name to stop the ETLs
            connector_name: `str`, `str`, default `None`
                Connector Name
            connector_id: `str`, `str`, default `None`
                Connector ID

        Returns: dict
         Dictionary with the response Carol's response

        """

        responses = self._play_pause_etl(
            kind='pause', staging_name=staging_name,
            connector_name=connector_name, connector_id=connector_id,

        )

        return responses

    def get_etl_information(self, connector_id=None, connector_name=None):
        """
        Get ETL Configurations for a connector

        Args:

            connector_name: `str`, `str`, default `None`
                Connector Name
            connector_id: `str`, `str`, default `None`
                Connector ID

        Returns: list of dict
            Etl information

        """

        connector_id = connector_id if connector_id else self.get_by_name(connector_name)[
            'mdmId']

        return self.carol.call_api(path=f"v1/etl/connector/{connector_id}")

    def get_all_stagings(self, connector_name=None, connector_id=None):
        """
        Get all stagings from a connector.

        Args:

            connector_name: `str`, `str`, default `None`
                Connector Name
            connector_id: `str`, `str`, default `None`
                Connector ID

        Returns: list
            List of staging tables.

        """
        if connector_name is not None:
            connector_id = self.get_by_name(connector_name)['mdmId']
        elif connector_id is None:
            raise ValueError(
                'Either `connector_id` or `connnector_name` should be set.')

        c = self.carol.call_api(path=f"v1/staging/connectors/{connector_id}/tables")
        return sorted(c)
