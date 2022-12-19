"""

This submodule has all the classes to query data from RT layer in Carol.

"""

import json
import itertools
from datetime import datetime
from .connectors import Connectors
from .named_query import NamedQuery
from .filter import Filter, MAXIMUM, MINIMUM, TYPE_FILTER, TERM_FILTER
from .filter import RANGE_FILTER as RF
from .utils.miscellaneous import ranges
import copy


def delete_golden(carol, dm_name, now=None):
    """
    Delete Golden records.

    It Will delete all golden records of a given data model based on lastUpdate.

    Args:

        carol: `pycarol.carol.Carol`
            Carol instance
        dm_name: `str`
            Data model name
        now: `str`
            Delete records where last update is less the `now`. Any date time ISO format is accepted.

    Usage:

    .. code:: python

        from pycarol.query import delete_golden
        from pycarol.auth.PwdAuth import PwdAuth
        from pycarol.carol import Carol
        login = Carol()
        delete_golden(login, dm_name=my_dm)

        #To delete based on a date.
        delete_golden(login, dm_name=my_dm, now='2018-11-16')

    Attention:

        This API will delete all data in the DataModel, and if there is a DataModel View related to this DataModel
        one needs to reprocess it.

    """

    if now is None:
        now = datetime.utcnow().isoformat(timespec='seconds')

    json_query = Filter.Builder() \
        .should(TYPE_FILTER(value=dm_name + "Golden")) \
        .should(TYPE_FILTER(value=dm_name + "Master")) \
        .must(RF("mdmLastUpdated", [None, now])) \
        .build().to_json()

    try:
        Query(carol).delete(json_query)
    except:
        # if it it too many records, one would have a timeout but the records will be deleted anyway
        pass

    json_query = Filter.Builder() \
        .type(dm_name + "Rejected") \
        .must(RF("mdmLastUpdated", [None, now])) \
        .build().to_json()

    try:
        Query(carol, index_type='STAGING').delete(json_query)
    except:
        # if it it too many records, one would have a timeout but the records will be deleted anyway
        pass


class Query:
    """
    Class to query data from Carol.

    This class can be used to query data from data models and stagings tables, query using named queries and delete
    records.

    Args:

        carol: carol: Carol object
            Carol object.
        max_hits:  `int`, default float('inf')
            Number of records that will be downloaded.
        offset: `int`, default 0
            Offset for pagination. Only used when `scrollable=False`
        page_size: `int`, default 100
            Number of records downloaded in each pagination. The maximum value is 1000
        sort_order: `str`, default 'ASC'
            Sort ascending ('ASC') vs. descending ('DESC').
        sort_by: `str`,  default `None`
            Name to sort by.
        scrollable: `bool`, default True
            Use scroll for pagination. This should be the main way of doing, unless you are querying few data.
        index_type: `str`, default 'MASTER'
            Query data from 'MASTER', 'STAGING'
        only_hits: `bool`, default 'True'
            Return only results in the response path $hits.mdmGoldenFieldAndValues
        fields: `list`, default `None`
            Fields to return in response. e.g., ["mdmGoldenFieldAndValues.mdmtaxid", "mdmGoldenFieldAndValues.date"]
        get_aggs: `bool`, default `False`
            To be used if the query/named query has aggravations
        save_results: `bool`, default `False`
            If save the result of the query in the file specified in `filename`
        filename: `str`, default `query_result.json`
            File path to save the response.
        print_status: `bool`, default `True`
            Print the number of records in each interaction.
        safe_check: `bool`, default `False`
            To be used if there are repeated records (same mdmId)
        get_errors: `bool`, default `False`
            To get the errors in the goldenRecords, if any.
        flush_result: `bool`, default `False`
            To be used with save_results, it will not copy the result to memory, only to the file.
        use_stream: `bool`, default `False`
            Use the stram of data.
        get_times: `bool`, default `False`
            It will create a list of times that each pagination took.
        kwargs: `dict`
            Extra parameters to be passed to Carol.call_api

    """
    def __init__(self, carol, max_hits=float('inf'), offset=0, page_size=100, sort_order='ASC', sort_by=None,
                 scrollable=True, index_type='MASTER', only_hits=True, fields=None, get_aggs=False,
                 save_results=False, filename='query_result.json', print_status=True, safe_check=False,
                 get_errors=False, flush_result=False, use_stream=False, get_times=False, **kwargs):

        self.carol = carol
        self.max_hits = max_hits
        self.offset = offset
        self.page_size = page_size
        self.sort_order = sort_order
        self.sort_by = sort_by
        self.scrollable = scrollable
        self.index_type = index_type
        self.only_hits = only_hits
        self.fields = fields
        self.get_aggs = get_aggs
        self.use_stream = use_stream

        self.save_results = save_results
        self.filename = filename
        self.print_status = print_status
        self.safe_check = safe_check
        self.get_errors = get_errors
        self.flush_result = flush_result
        self.get_times = get_times
        self.named_query = None
        self.callback = None

        # Crated to send to the Rest API
        self.query_params = None
        self.drop_list = None
        self.json_query = None
        self.total_hits = None
        self.query_times = []
        self.kwargs = kwargs

        self.results = []

        if self.max_hits == float('inf'):
            self.get_all = True
        else:
            self.get_all = False

    def _build_query_params(self):
        self.query_params = {"offset": self.offset, "pageSize": self.page_size, "sortOrder": self.sort_order,
                             "indexType": self.index_type}

        if self.sort_by is not None:
            self.query_params["sortBy"] = self.sort_by
        if self.scrollable:
            self.query_params["scrollable"] = self.scrollable
        if self.fields:
            self.query_params["fields"] = self.fields

    def _build_return_fields(self):
        if isinstance(self.fields, str):
            self.fields = [self.fields]

        if self.fields is not None:
            self.fields = ','.join(self.fields)


    def page(self, offset=0):
        """
        Get only one page of the result using offset.

        Args:
            offset: `int`, default 0
                Offset to get. To properly paginate manually, offset should be `offset + page_size`.

        Returns:
            Query json response
        """

        self.offset = offset
        self.scrollable = False

        self.results = []
        if self.json_query is None:
            raise ValueError("You must call query() or named() before calling page()")

        self._build_return_fields()
        self._build_query_params()

        if self.named_query is None:
            url_filter = "v2/queries/filter"
        else:
            url_filter = "v2/queries/named/{}".format(self.named_query)

        result = self.carol.call_api(url_filter, data=self.json_query, params=self.query_params, timeout=240,
                                     method_whitelist=frozenset(['POST']), **self.kwargs)

        if self.only_hits:

            result = result['hits']
            return [elem.get('mdmGoldenFieldAndValues', elem)
                    for elem in result if  elem.get('mdmGoldenFieldAndValues', None)]

        else:
            return result

    def go(self, callback=None):
        """

        Args:

            callback: `callable` object
                This function will receive the current batch of records from the filter made.
        Returns: `None`

        """

        self.results = []
        if self.json_query is None:
            raise ValueError("You must call all() or query() or named() before calling go()")

        self._build_return_fields()
        self._build_query_params()

        self._scrollable_query_handler(callback)

        return self


    def _scrollable_query_handler(self, callback=None):
        if not self.offset == 0:
            raise ValueError('It is not possible to use offset when using scroll for pagination')

        set_param = True
        count = self.offset
        if self.save_results:
            file = open(self.filename, 'w', encoding='utf8')

        if self.named_query is None:
            url_filter = "v2/queries/filter"
        else:
            url_filter = "v2/queries/named/{}".format(self.named_query)

        to_get = float("inf")
        downloaded = 0
        while count < to_get:

            result = self.carol.call_api(url_filter, data=self.json_query, params=self.query_params, timeout=240,
                                         method_whitelist=frozenset(['POST']), **self.kwargs)

            if set_param:
                self.total_hits = result["totalHits"]
                if self.get_all:
                    to_get = result["totalHits"]
                elif self.max_hits <= result["totalHits"]:
                    to_get = self.max_hits
                else:
                    to_get = result["totalHits"]

                set_param = False
                if self.safe_check:
                    self.mdmId_list = []
                if self.get_errors:
                    self.query_errors = {}

            count += result['count']
            downloaded += result['count']
            scroll_id = result.get('scrollId', None)
            if (scroll_id is not None) or (self.get_aggs):
                url_filter = "v2/queries/filter/{}".format(scroll_id)
            elif (result['count'] == 0):
                if count < self.total_hits:
                    print(f'Total records downloaded: {count}/{self.total_hits}')
                    print(f'Something is wrong, no scrollId to continue \n')
                    break
            else:
                raise Exception('No Scroll Id to use. Something is wrong')

            if self.get_times:
                self.query_times.append(result.pop('took'))

            if self.only_hits:

                result = result['hits']
                if self.safe_check:
                    self.mdmId_list.extend([mdm_id['mdmId'] for mdm_id in result])
                    if len(self.mdmId_list) > len(set(self.mdmId_list)):
                        raise Exception('There are repeated records')

                if self.get_errors:
                    self.query_errors.update(
                        {elem.get('mdmId', elem): elem.get('mdmErrors', elem) for elem in result if elem['mdmErrors']})

                result = [elem.get('mdmGoldenFieldAndValues', elem) for elem in result if
                          elem.get('mdmGoldenFieldAndValues',
                                   None)]  # get mdmGoldenFieldAndValues if not empty and if it exists

                if not self.flush_result:
                    self.results.extend(result)
            else:
                result.pop('count')
                result.pop('totalHits')
                # result.pop('scrollId')
                if not self.flush_result:
                    self.results.append(result)

                if self.get_aggs:
                    if self.save_results:
                        file.write(json.dumps(result, ensure_ascii=False))
                        file.write('\n')
                        file.flush()
                    break

            if callback:
                if callable(callback):
                    callback(result)
                else:
                    raise Exception(
                        f'"{callback}" is a {type(callback)} and is not callable. This variable must be a function.')

            if self.print_status:
                print('{}/{}'.format(downloaded, to_get), end='\r')
            if self.save_results:
                file.write(json.dumps(result, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if self.save_results:
            file.close()

    def check_total_hits(self, json_query, index_type="MASTER"):
        """
        Check the total hits for a given query
        :param json_query: Json object with the query to use
        :param index_type: Index type to query.
        :return: number of records for this query
        """

        querystring = {"indexType": index_type}
        self.json_query = json_query
        url_filter = "v2/queries/count"
        result = self.carol.call_api(url_filter, data=self.json_query, method='POST', params=querystring,
                                     method_whitelist=frozenset(['HEAD', 'TRACE', 'GET',
                                                                 'PUT', 'OPTIONS', 'DELETE', 'POST']))
        self.total_hits = result
        return result

    def named(self, named_query, params=None, json_query=None):
        if json_query is not None and params is not None:
            raise Exception("You can only specify either params or json_query, but not both!")

        self.results = []
        self.named_query = named_query

        if json_query is not None:
            self.json_query = json_query
        else:
            if params is None:
                params = {}
            self.json_query = params

        return self

    def named_query_params(self, named_query):
        named = NamedQuery(self.carol)
        named.by_name(named_query=named_query)
        return named.param_dict

    def query(self, json_query):
        self.json_query = json_query
        return self

    def all(self, dm_name):
        if not dm_name.endswith('Golden'):
            dm_name = dm_name + 'Golden'
        self.json_query = Filter.Builder().type(dm_name).build().to_json()
        self.index_type = 'MASTER'
        return self

    def delete(self, json_query):
        # TODO: we should check the number of records to be deleted. If too many,
        # it can be a problem.
        self.json_query = json_query
        self.querystring = {"indexType": self.index_type}
        url_filter = "v2/queries/filter"
        result = self.carol.call_api(url_filter, data=self.json_query,
                                     params=self.querystring, method='DELETE',
                                     method_whitelist=frozenset(['HEAD', 'TRACE', 'GET',
                                                                 'PUT', 'OPTIONS']))

        print('Deleted: ', result)


class ParQuery:
    def __init__(self, carol, backend='dask', return_df=True, verbose=50, n_jobs=4):
        """

        :param carol:
        :param backend:
        :param return_df:
        :param verbose:
        :param n_jobs:
        """
        import pandas as pd
        self._stag_mdm_key_range = None
        self._multiplier = None
        self.carol = carol
        self.return_df = return_df
        if return_df:
            import pandas as pd
        self.backend = backend
        self.verbose = verbose
        self.n_jobs = n_jobs


        assert self.backend == 'dask' or self.backend == 'joblib'

    def _get_min_max(self, datamodel_name, mdm_key, index_type, custom_filter=None, multiplier=None ):

        if custom_filter is not None:
            j = custom_filter
        else:
            j = Filter.Builder() \
                .type(datamodel_name) \
                .aggregation_list([MINIMUM(name='MINIMUM', params=mdm_key), MAXIMUM(name='MAXIMUM', params=mdm_key)]) \
                .build().to_json()

        query = Query(self.carol, index_type=index_type, only_hits=False, get_aggs=True, save_results=False,
                      print_status=True, page_size=1, ).query(j).go()

        if query.results[0].get('aggs') is None:
            return None, None, None
        sample = query.results[0].get('hits')[0]
        min_v = query.results[0]['aggs']['MINIMUM']['value']
        max_v = query.results[0]['aggs']['MAXIMUM']['value']
        print(f"Total Hits to download: {query.total_hits}")

        if multiplier is not None:
            min_v = int(min_v*multiplier) - 10
            max_v = int(max_v*multiplier) + 10
        return min_v, max_v, sample

    def _get_staging_from_golden_rejected(self,datamodel_name, connector_id, staging_name, fields):

        index_type = 'STAGING'
        self.datamodel_name = f"{datamodel_name}Rejected"
        self.fields = 'mdmStagingRecord'
        self.filter_stag = f"{connector_id}_{staging_name}"
        only_hits = False
        mdm_key = 'mdmStagingRecord.mdmCounterForEntity'
        j = Filter.Builder() \
            .type(self.datamodel_name) \
            .must(TERM_FILTER(key='mdmStagingEntityName.raw',
                              value=self.filter_stag)) \
            .aggregation_list([MINIMUM(name='MINIMUM', params=mdm_key),
                               MAXIMUM(name='MAXIMUM', params=mdm_key)]) \
            .build().to_json()

        min_v, max_v, sample = self._get_min_max(datamodel_name=self.datamodel_name, mdm_key=mdm_key,
                                                 index_type=index_type, custom_filter=j)
        if (min_v is None) and (max_v is None):
            return []
        chunks = ranges(min_v, max_v, self.slices)

        # rejected

        print(f"Number of chunks for rejected: {len(chunks)}")
        self.fields_to_get = [self.fields + '.' + i for i in sample.get(self.fields).keys() for j in fields if
                              j + '_' in i]

        self.custom_filter = Filter.Builder() \
            .type(self.datamodel_name) \
            .must(TERM_FILTER(key='mdmStagingEntityName.raw',
                              value=self.filter_stag))

        if self.backend == 'dask':
            list_to_compute = _dask_backend(carol=self.carol, chunks=chunks, datamodel_name=self.datamodel_name,
                                            page_size=self.page_size, index_type=index_type, fields=self.fields,
                                            only_hits=only_hits, mdm_key=mdm_key, return_df=self.return_df,
                                            fields_to_get=self.fields_to_get, custom_filter=self.custom_filter)
        elif self.backend == 'joblib':
            list_to_compute = _joblib_backend(carol=self.carol, chunks=chunks, datamodel_name=self.datamodel_name,
                                              page_size=self.page_size, index_type=index_type, fields=self.fields,
                                              only_hits=only_hits, mdm_key=mdm_key, return_df=self.return_df,
                                              fields_to_get=self.fields_to_get, custom_filter=self.custom_filter,
                                              n_jobs=self.n_jobs, verbose=self.verbose)
        else:
            raise KeyError

        return list_to_compute

    def _get_golden(self, datamodel_name=None, fields=None):

        index_type = 'MASTER'
        self.datamodel_name = f"{datamodel_name}Golden"
        self.fields = 'mdmGoldenFieldAndValues'
        self.fields_to_get = [self.fields + '.' + i if self.fields not in i else i for i in fields]
        only_hits = True
        mdm_key = 'mdmCounterForEntity'

        min_v, max_v, sample = self._get_min_max(datamodel_name=self.datamodel_name, mdm_key=mdm_key,
                                                 index_type=index_type)
        if (min_v is None) and (max_v is None):
            return []
        chunks = ranges(min_v, max_v, self.slices)
        print(f"Number of chunks: {len(chunks)}")

        if self.backend == 'dask':
            list_to_compute = _dask_backend(carol=self.carol, chunks=chunks, datamodel_name=self.datamodel_name,
                                            page_size=self.page_size, index_type=index_type, fields=self.fields,
                                            only_hits=only_hits, mdm_key=mdm_key, return_df=self.return_df,
                                            fields_to_get=self.fields_to_get, custom_filter=self.custom_filter)
        elif self.backend == 'joblib':
            list_to_compute = _joblib_backend(carol=self.carol, chunks=chunks, datamodel_name=self.datamodel_name,
                                              page_size=self.page_size, index_type=index_type, fields=self.fields,
                                              only_hits=only_hits, mdm_key=mdm_key, return_df=self.return_df,
                                              fields_to_get=self.fields_to_get, custom_filter=self.custom_filter,
                                              n_jobs=self.n_jobs, verbose=self.verbose)

        if self.return_df:
            return pd.concat(list_to_compute, ignore_index=True, sort=True)
        list_to_compute = list(itertools.chain(*list_to_compute))
        return list_to_compute


    def _get_staging(self, connector_id=None, connector_name=None, staging_name=None, fields=None):

        if not connector_id:
            connector_id = Connectors(self.carol).get_by_name(connector_name)['mdmId']

        index_type = 'STAGING'
        self.datamodel_name = f"{connector_id}_{staging_name}"
        self.fields_to_get = fields
        self.fields = None
        only_hits = False
        mdm_key = 'mdmCounterForEntity'
        if self._stag_mdm_key_range is not None:
            mdm_key = self._stag_mdm_key_range



        min_v, max_v, sample = self._get_min_max(datamodel_name=self.datamodel_name, mdm_key=mdm_key,
                                                 index_type=index_type, multiplier=self._multiplier )
        if (min_v is None) and (max_v is None):
            return []
        chunks = ranges(min_v, max_v, self.slices)
        print(f"Number of chunks: {len(chunks)}")

        if self.backend == 'dask':
            list_to_compute = _dask_backend(carol=self.carol, chunks=chunks, datamodel_name=self.datamodel_name,
                                            page_size=self.page_size, index_type=index_type, fields=self.fields,
                                            only_hits=only_hits, mdm_key=mdm_key, return_df=self.return_df,
                                            fields_to_get=self.fields_to_get, custom_filter=self.custom_filter)
        elif self.backend == 'joblib':
            list_to_compute = _joblib_backend(carol=self.carol, chunks=chunks, datamodel_name=self.datamodel_name,
                                              page_size=self.page_size, index_type=index_type, fields=self.fields,
                                              only_hits=only_hits, mdm_key=mdm_key, return_df=self.return_df,
                                              fields_to_get=self.fields_to_get, custom_filter=self.custom_filter,
                                              n_jobs=self.n_jobs, verbose=self.verbose)
        else:
            raise KeyError

        if self.return_df:
            return pd.concat(list_to_compute, ignore_index=True, sort=True)
        list_to_compute = list(itertools.chain(*list_to_compute))
        return list_to_compute



    def go(self, datamodel_name=None, slices=1000, page_size=1000, staging_name=None, connector_id=None,
           connector_name=None, fields=None):
        assert slices < 9999, '10k is the largest slice possible'
        self.slices = slices
        self.page_size = page_size
        if fields is None:
            fields = []
        self.custom_filter = None

        if datamodel_name is None:
            assert connector_id or connector_name
            assert staging_name

            return self._get_staging(connector_id=connector_id, connector_name=connector_name, staging_name=staging_name,
                              fields=fields)
        else:

            return self._get_golden(datamodel_name=datamodel_name, fields=fields)


def _dask_backend(carol, chunks, datamodel_name, page_size, index_type, fields,
                  only_hits, mdm_key, return_df, fields_to_get, custom_filter):
    list_to_compute = []
    for RANGE_FILTER in chunks:
        y = dask.delayed(_par_query)(
            datamodel_name=datamodel_name,
            RANGE_FILTER=RANGE_FILTER,
            page_size=page_size,
            login=carol,
            index_type=index_type,
            fields=fields,
            only_hits=only_hits,
            mdm_key=mdm_key,
            return_df=return_df,
            fields_to_get=fields_to_get,
            custom_filter=custom_filter,
        )
        list_to_compute.append(y)

    return dask.compute(*list_to_compute)


def _joblib_backend(carol, chunks, datamodel_name, page_size, index_type, fields,
                    only_hits, mdm_key, return_df, fields_to_get, custom_filter, n_jobs, verbose, ):
    from joblib import Parallel, delayed
    list_to_compute = Parallel(n_jobs=n_jobs,
                               verbose=verbose)(delayed(_par_query)(
        datamodel_name=datamodel_name,
        RANGE_FILTER=RANGE_FILTER,
        page_size=page_size,
        login=carol,
        index_type=index_type,
        fields=fields,
        only_hits=only_hits,
        mdm_key=mdm_key,
        return_df=return_df,
        fields_to_get=fields_to_get,
        custom_filter=custom_filter,
    )
                                                for RANGE_FILTER in chunks)
    return list_to_compute


def _par_query(datamodel_name, RANGE_FILTER, page_size=1000, login=None, index_type='MASTER', fields=None, mdm_key=None,
               only_hits=True, return_df=True, fields_to_get=None, custom_filter=None):
    if custom_filter is not None:
        json_query = copy.deepcopy(custom_filter)
        json_query = json_query.must(RF(key=mdm_key, value=RANGE_FILTER)).build().to_json()

    else:
        json_query = Filter.Builder() \
            .type(datamodel_name) \
            .must(RF(key=mdm_key, value=RANGE_FILTER)) \
            .build().to_json()

    query = Query(login, page_size=page_size, save_results=False, print_status=False, index_type=index_type,
                  only_hits=only_hits,
                  fields=fields_to_get).query(json_query).go()
    query = query.results

    if not only_hits:
        query = [i['hits'] for i in query]
        query = list(itertools.chain(*query))
        if fields:
            query = [elem.get(fields, elem) for elem in query if
                     elem.get(fields, None)]

    if return_df:
        return pd.DataFrame(query)

    return query
