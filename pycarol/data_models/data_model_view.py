import warnings
import json
from ..utils.importers import _import_dask, _import_pandas
from ..storage import Storage
from ..utils.miscellaneous import drop_duplicated_parquet, drop_duplicated_parquet_dask
from ..utils.deprecation_msgs import _deprecation_msgs

class DataModelView:

    def __init__(self, carol):
        self.carol = carol

    def get_by_name(self, view_name):
        return self._get(view_name, by='name')

    def get_by_id(self, view_id):
        return self._get(view_id, by='id')


    def _get(self, id,  by='name'):

        if by == 'name':
            url = f"v1/relationshipView/name/{id}"
        elif by == 'id':
            url = f"/api/v1/relationshipView/{id}"
        else:
            raise print('Type incorrect, it should be "id" or "name"')

        return self.carol.call_api(url, method='GET')

    def _build_query_params(self):
        if self.sort_by is None:
            self.query_params = {"offset": self.offset, "pageSize": str(self.page_size), "sortOrder": self.sort_order}
        else:
            self.query_params = {"offset": self.offset, "pageSize": str(self.page_size), "sortOrder": self.sort_order,
                                 "sortBy": self.sort_by}

    def reprocess(self, view_name=None, view_id=None, cds=False):
        """
        Reprocess the view records.

        Args:
            view_name: `str`, default `None`
                View name
            view_id: `str`, default `None`
                View ID
            cds: `bool`, default `False`
                Save view on CDS.

        Returns: `dict`
            Task response.

        """

        if view_name is not None:
            view_id = self.get_by_name(view_name=view_name)['mdmId']
        else:
            assert view_id is not None, "'view_id' or 'view_name' must be set"

        url_filter = "v1/goldenRecordView/reprocess"

        query_params = {"relationshipViewId" : view_id,
                        "cds" : cds}
        return self.carol.call_api(url_filter, params=query_params,
                                   method='POST')

    def get_all(self, offset=0, page_size=-1, sort_order='ASC',
                sort_by=None, print_status=False,
                save_file=None):


        self.offset = offset
        self.page_size = page_size
        self.sort_order = sort_order
        self.sort_by = sort_by
        self._build_query_params()

        self.template_dict = {}
        self.template_data = []
        count = self.offset

        set_param = True
        self.total_hits = float("inf")
        if save_file:
            assert isinstance(save_file, str)
            file = open(save_file, 'w', encoding='utf8')
        while count < self.total_hits:
            url_filter = "v1/relationshipView"
            query = self.carol.call_api(url_filter, params=self.query_params, method='GET')

            if query['count'] == 0:
                print('There are no more results.')
                print('Expecting {}, reponse = {}'.format(self.total_hits, count))
                break
            count += query['count']
            if set_param:
                self.total_hits = query["totalHits"]
                set_param = False

            query = query['hits']
            self.template_data.extend(query)

            self.template_dict.update({i['mdmName']: {'mdmId': i['mdmId'],
                                                      'mdmRunningState': i['mdmRunningState'],
                                                      'mdmEntityType': i['mdmEntityType']}
                                       for i in query})

            self.query_params['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.total_hits), end='\r')
            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()
        return self


    def export(self, view_name=None, view_id=None, sync_view=True, full_export=False,
               delete_previous=False, export_format='PARQUET'):
        """

        @DEPRECATED. This function was removed in pycarol 3.34

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

        _deprecation_msgs("This function was removed from pyCarol")
        return None



    def fetch_parquet(self, view_name, merge_records=True, backend='pandas', return_dask_graph=False,
                      columns=None, return_metadata=False, callback=None, max_hits=None,
                      cds=False, max_workers=None, return_callback_result=False):

        """

        Args:
            view_name: `str`
                View name to be imported
            merge_records: `bool`, default `True`
                This will keep only the most recent record exported. Sometimes there are updates and/or deletions and
                one should keep only the last records.
            backend: ['dask','pandas'], default `dask`
                if to use either dask or pandas to fetch the data
            return_dask_graph: `bool`, default `false`
                If to return the dask graph or the dataframe.
            columns: `list`, default `None`
                List of columns to fetch.
            return_metadata: `bool`, default `False`
                To return or not the fields ['mdmId', 'mdmCounterForEntity']
            callback: `callable`, default `None`
                Function to be called each downloaded file.
            max_hits: `int`, default `None`
                Number of records to get.
            cds: `bool`, default `False`
                    Get view data from CDS.
            max_workers: `int` default `None`
                Number of workers to use when downloading parquet files with pandas back-end.
            return_callback_result `bool` default `False`
                If a callback is used, it will return the result of the response of the callback. This will skip all the
                operation to merge records and return selected columns.
        :return:
        """

        if callback:
            assert callable(callback), \
                f'"{callback}" is a {type(callback)} and is not callable.'

        if isinstance(columns, str):
            columns = [columns]

        assert backend == 'dask' or backend == 'pandas'

        if return_dask_graph:
            assert backend == 'dask'

        if not cds:
            _deprecation_msgs("`cds` option will be removed from pycarol 3.33. Consider use `cds=True`"
                              " to avoid problems. ")

        if columns:
            columns.extend(['mdmId', 'mdmCounterForEntity', 'mdmLastUpdated'])

        import_type = 'view_cds'
        storage = Storage(self.carol)
        token_carolina = storage.backend.carolina.token
        storage_space = storage.backend.carolina.get_bucket_name(import_type)

        if backend == 'dask':
            d = _import_dask(storage=storage, view_name=view_name, import_type=import_type,
                             merge_records=merge_records, return_dask_graph=return_dask_graph,
                             columns=columns, )

        elif backend == 'pandas':

            d = _import_pandas(storage=storage, view_name=view_name, golden=True, columns=columns, callback=callback,
                               max_hits=max_hits, import_type=import_type, max_workers=max_workers,
                               token_carolina=token_carolina,
                               )
            if d is None:
                warnings.warn("No data to fetch!", UserWarning)
                return d

        else:
            raise ValueError(f'backend should be either "dask" or "pandas" you entered {backend}')

        if (return_callback_result) and (callback is not None):
            return d

        if merge_records:
            if (not return_dask_graph) or (backend == 'pandas'):
                d = drop_duplicated_parquet(d)
            else:
                d = drop_duplicated_parquet_dask(d)

        if not return_metadata:
            to_drop = set(['mdmId', 'mdmCounterForEntity', 'mdmLastUpdated']).intersection(set(d.columns))
            d = d.drop(labels=to_drop, axis=1)

        return d


    def export_all(self, sync_view=True, full_export=False, delete_previous=False):
        """

        @DEPRECATED. This function was removed in pycarol 3.34
        Export all data model view

        This method will trigger or pause the export of the data in the data model view to CDS

        :param sync_view: `bool`, default `True`
            Sync the data model view
        :param full_export: `bool`, default `True`
            Do a resync of the data model view
        :param delete_previous: `bool`, default `False`
            Delete previous exported files.
        :return: None
        """

        _deprecation_msgs("This function was removed from pyCarol")
        return None


    def _get_view_export_stats(self):
        """
        @DEPRECATED. This function was removed in pycarol 3.34

        Get export status for views

        :return: `dict`
            dict with the information of which data model view is exporting its data.
        """

        _deprecation_msgs("This function was removed from pyCarol")
        return None


