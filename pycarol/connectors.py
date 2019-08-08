import json
from collections import defaultdict


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
        mdm_id = self.get_by_name(name)['mdmId']
        self.delete_by_id(mdm_id, force_deletion)

    def delete_by_id(self, mdm_id, force_deletion=True):
        self.carol.call_api('v1/connectors/{}?forceDeletion={}'.format(mdm_id, force_deletion), method='DELETE')

    def get_all(self, offset=0, page_size=-1, sort_order='ASC', sort_by=None, include_connectors=False,
                include_mappings=False, include_consumption=False, print_status=True, save_results=False,
                filename='connectors.json'):

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

        if all_connectors:
            response = self.carol.call_api('v1/connectors/stats/all')
        else:
            if connector_name:
                connector_id = self.get_by_name(connector_name)['mdmId']
            else:
                assert connector_id

            response = self.carol.call_api('v1/connectors/{}/stats'.format(connector_id))

        self._conn_stats = response['aggs']
        return {key: list(value['stagingEntityStats'].keys()) for key, value in self._conn_stats.items()}

    def staging_to_connectors_map(self):
        d = defaultdict(list)
        connectors = self.get_all(print_status=False)
        for connector in connectors:
            current_connector = connector['mdmId']
            conn_stats = self.stats(current_connector)
            for i in conn_stats[current_connector]:
                d[i].append(current_connector)

        return d

    def find_by_staging(self, staging_name=None):
        d = self.staging_to_connectors_map()

        if staging_name:
            conn = d.get(staging_name, None)
            if conn is None:
                raise ValueError('There is no staging named {}'.format(staging_name))

            elif len(conn) > 1:
                print('More than one connector with the staging {}'.format(staging_name))
                return conn

    def get_dm_mappings(self, connector_id=None, connector_name=None, staging_name=None,
                        dm_id=None, dm_name=None, reverse_mapping=False, offset=0, page_size=1000, sort_by=None,
                        sort_order='ASC', all_connectors=False):
        """
        Get data models mappings information.

        :param connector_id: `str`, default `None`
            Connector Id
        :param connector_name: `str`, default `None`
            Connectot name
        :param staging_name: `str`, default `None`
            Staging name
        :param dm_id:  `str`, default `None`
            Data model Id
        :param dm_name: `str`, default `None`
            Data model name
        :param reverse_mapping: `bool`, default `False`
            If to return the reverse mapping.
        :param offset: `int`, default `0`
            Offset for the response.
        :param page_size: `int`, default `1000`
            number of records in each call.
        :param sort_by: `str`, default `None`
            Sort response by
        :param sort_order: `str`, default `ASC`
            Sort Order. Possible values "ASC" and "DESC"
        :param all_connectors: `bool`, default `False`
            It will return all the mapping for all connectors/stagings
        :return:
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
            if connector_name:
                connector_id = self.get_by_name(connector_name)['mdmId']
            else:
                assert connector_id

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
