import time
import copy
import warnings
import pandas as pd



from ..utils.importers import _import_dask, _import_pandas
from ..verticals import Verticals
from ..storage import Storage
from ..query import Query
from ..filter import TYPE_FILTER, Filter
import itertools


from ..connectors import Connectors





class DataModelView:

    def __init__(self, carol):
        self.carol = carol


    def get_by_name(self, view_name):
        return self._get(view_name, by='name')


    def _get(self, id,  by='name'):

        if by == 'name':
            url = f"v1/relationshipView/name/{id}"
        elif by == 'id':
            url = f"/api/v1/relationshipView/{id}"
        else:
            raise print('Type incorrect, it should be "id" or "name"')

        return self.carol.call_api(url, method='GET')

    def get_all(self, offset=0, page_size=-1, sort_order='ASC',
                sort_by=None, print_status=False,
                save_file=None):
        # TODO:
        pass


    def export(self, view_name=None, view_id=None, sync_view=True, full_export=False,
               delete_previous=False, export_format='PARQUET'):
        """

        Export datamodel to s3

        This method will trigger or pause the export of the data in the datamodel to
        s3

        :param view_name: `str`, default `None`
            View Name
        :param view_id: `str`, default `None`
            View id
        :param sync_view: `bool`, default `True`
            Sync the view
        :param full_export: `bool`, default `True`
            Do a resync of the view
        :param delete_previous: `bool`, default `False`
            Delete previous exported files.
        :param export_format: `str`, default `PARQUET`
            Format of the data to be exported. Possible values are:
                "PARQUET", "CSV", "JSON", "EXCEL"
        :return: None
        """

        if sync_view:
            status = 'RUNNING'
        else:
            status = 'PAUSED'

        if view_name:
            view_id = self.get_by_name(view_name)['mdmId']
        else:
            assert view_id

        query_params = {"status": status, "fullExport": full_export,
                        'relationshipViewId':view_id, 'format':export_format,
                        "deletePrevious": delete_previous}

        url = f'v1/goldenRecordView/exporter'
        return self.carol.call_api(url, method='POST', params=query_params)


    def fetch_parquet(self, view_name, merge_records=True, backend='pandas', return_dask_graph=False,
                      columns=None, return_metadata=False, callback=None, max_hits=None):

        """

        :param view_name: `str`
            View name to be imported
        :param merge_records: `bool`, default `True`
            This will keep only the most recent record exported. Sometimes there are updates and/or deletions and
            one should keep only the last records.
        :param backend: ['dask','pandas'], default `dask`
            if to use either dask or pandas to fetch the data
        :param return_dask_graph: `bool`, default `false`
            If to return the dask graph or the dataframe.
        :param columns: `list`, default `None`
            List of columns to fetch.
        :param return_metadata: `bool`, default `False`
            To return or not the fields ['mdmId', 'mdmCounterForEntity']
        :param callback: `callable`, default `None`
            Function to be called each downloaded file.
        :param max_hits: `int`, default `None`
            Number of records to get.
        :return:
        """

        if isinstance(columns, str):
            columns = [columns]

        assert backend == 'dask' or backend == 'pandas'

        if return_dask_graph:
            assert backend == 'dask'

        # validate export
        #TODO: THIS IS NOT WORKING, BUG CAROL
        #dms = self._get_view_export_stats()
        #if not dms.get(dm_name):
        #    raise Exception(
        #        f'"{dm_name}" is not set to export data, \n'
        #        f'use `dm = DataModel(login).export(dm_name="{dm_name}", sync_dm=True) to activate')

        if columns:
            columns.extend(['mdmId', 'mdmCounterForEntity', 'mdmLastUpdated'])

        storage = Storage(self.carol)
        if backend == 'dask':
            d = _import_dask(storage=storage, view_name=view_name, import_type='view',
                             merge_records=merge_records, return_dask_graph=return_dask_graph,
                             columns=columns)

        elif backend == 'pandas':

            d = _import_pandas(storage=storage, view_name=view_name, golden=True, columns=columns, callback=callback,
                               max_hits=max_hits,import_type='view')
            if d is None:
                warnings.warn("No data to fetch!", UserWarning)
                _field_types = self._get_name_type_DMs(self.get_by_name(dm_name)['mdmFields'])
                cols_keys = list(_field_types)
                if return_metadata:
                    cols_keys.extend(['mdmId', 'mdmCounterForEntity', 'mdmLastUpdated'])

                elif columns:
                    columns = [i for i in columns if i not in ['mdmId', 'mdmCounterForEntity', 'mdmLastUpdated']]

                d = pd.DataFrame(columns=cols_keys)
                for key, value in _field_types.items():
                    d.loc[:, key] = d.loc[:, key].astype(_DATA_MODEL_TYPES_MAPPING.get(value.lower(), str), copy=False)
                if columns:
                    columns = list(set(columns))
                    d = d[list(set(columns))]
                return d

        else:
            raise ValueError(f'backend should be either "dask" or "pandas" you entered {backend}')

        if merge_records:
            if not return_dask_graph:
                d.sort_values('mdmCounterForEntity', inplace=True)
                d.reset_index(inplace=True, drop=True)
                d.drop_duplicates(subset='mdmId', keep='last', inplace=True)
                d.reset_index(inplace=True, drop=True)
            else:
                d = d.set_index('mdmCounterForEntity', sorted=True) \
                    .drop_duplicates(subset='mdmId', keep='last') \
                    .reset_index(drop=True)

        if not return_metadata:
            to_drop = set(['mdmId', 'mdmCounterForEntity', 'mdmLastUpdated']).intersection(set(d.columns))
            d = d.drop(labels=to_drop, axis=1)

        return d


    def export_all(self, sync_dm=True, full_export=False, delete_previous=False):
        """

        Export all datamodel to s3

        This method will trigger or pause the export of the data in the datamodel to
        s3

        :param dm_name: `str`, default `None`
            Datamodel Name
        :param dm_id: `str`, default `None`
            Datamodel id
        :param sync_dm: `bool`, default `True`
            Sync the data model
        :param full_export: `bool`, default `True`
            Do a resync of the data model
        :param delete_previous: `bool`, default `False`
            Delete previous exported files.
        :return: None
        """
        self.get_all()

        for _name, i in self.template_dict.items():
            dm_id = i['mdmId']
            if i['mdmPublishedExists']:
                self.export(dm_id=dm_id, sync_dm=sync_dm, full_export=full_export,
                            delete_previous=delete_previous)
            else:
                print(f'Data Model `{_name}` is only in draft, and cannot be exported. Publish the Data Model to export it.')



    def _get_view_export_stats(self):
        """
        Get export status for data models

        :return: `dict`
            dict with the information of which data model is exporting its data.
        """

        json_q = Filter.Builder(key_prefix="") \
            .must(TYPE_FILTER(value="mdmGoldenRecordViewExport")).build().to_json()

        query = Query(self.carol, index_type='CONFIG', page_size=1000, only_hits=False)
        query.query(json_q, ).go()

        dm_results = query.results
        dm_results = [elem.get('hits', elem) for elem in dm_results
                      if elem.get('hits', None)]
        dm_results = list(itertools.chain(*dm_results))

        dm = DataModel(self.carol).get_all().template_data
        dm = {i['mdmId']: i['mdmName'] for i in dm}

        if dm_results is not None:
            return {dm.get(i['mdmEntityTemplateId'], i['mdmEntityTemplateId']+'_NOT_FOUND' ): i for i in dm_results}

        return dm_results


