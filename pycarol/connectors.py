import json
from collections import defaultdict


class Connectors:
    def __init__(self, carol):
        self.carol = carol

    def create(self, name, label=None, group_name="Others", overwrite=False):
        """
        Create a connector
        :param name: name
        :type name: str
        :param label: label
        :type label: str
        :param group_name: Group name
        :type group_name: str
        :param overwrite: Overwrite if it already exists. It will delete the connector and create a new one.
        :type overwrite: bool
        :return: None
        :rtype: None
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


    def get_by_name(self, name):
        """
        Get connector information using the connector name
        :param name:
        :type name: str
        :return: None
        :rtype: None
        """
        resp = self.carol.call_api('v1/connectors/name/{}'.format(name))
        return resp

    def delete_by_name(self, name, force_deletion=True):
        mdm_id = self.get_by_name(name)['mdmId']
        self.delete_by_id(mdm_id, force_deletion)

    def delete_by_id(self, mdm_id, force_deletion=True):
        self.carol.call_api('v1/connectors/{}?forceDeletion={}'.format(mdm_id, force_deletion), method='DELETE')

    def get_all(self, offset=0, page_size=-1, sort_order='ASC', sort_by=None, include_connectors = False,
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

            connectors.extend(conn)
            params['offset'] = count
            if print_status:
                print('{}/{}'.format(count, total_hits), end ='\r')
            if save_results:
                file.write(json.dumps(conn, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_results:
            file.close()
        return connectors

    def stats(self, connector_id=None, connector_name=None):

        if connector_name:
            connector_id = self.get_by_name(connector_name)['mdmId']
        else:
            assert connector_id


        response = self.carol.call_api('v1/connectors/{}/stats'.format(connector_id))

        conn_stats = response['aggs']
        return {key: list(value['stagingEntityStats'].keys()) for key, value in conn_stats.items()}

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

            elif len(conn)>1:
                print('More than one connector with the staging {}'.format(staging_name))
                return conn
