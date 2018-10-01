import json
from websocket import create_connection
import itertools
from joblib import Parallel, delayed
import dask
import pandas as pd
from pycarol.connectors import Connectors


class Query:
    """ It implements the calls for the following endpoints:
        1. POST - /api/v2/queries/filter
        2. POST - /api/v2/queries/named/{query_name}
        2. DELETE - /api/v2/queries/filter
        2. POST - /api/v2/queries/filter/{scrollId}

    Usage::

    """
    def __init__(self, carol, max_hits=float('inf'), offset=0, page_size=50, sort_order='ASC', sort_by=None,
                 scrollable=True, index_type='MASTER', only_hits=True, fields=None, get_aggs=False,
                 save_results=False, filename='query_result.json', print_status=True, safe_check=False,
                 get_errors=False, flush_result=False, use_stream=False, get_times=False):

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

    def go(self, callback=None):
        """
        """
        if self.json_query is None:
            raise ValueError("You must call all() or filter() or named() before calling go()")

        self._build_return_fields()
        self._build_query_params()

        if self.use_stream and self.named_query is None:
            self._streamable_query_handler(callback)
        elif self.scrollable:
            self._scrollable_query_handler(callback)
        else:
            self._oldQueryHandler()

        return self

    def _streamable_query_handler(self, callback=None):
        if not self.offset == 0:
            raise ValueError('It is not possible to use offset when using streaming')

        set_param = True
        count = self.offset
        if self.save_results:
            file = open(self.filename, 'w', encoding='utf8')
        url = self.carol.build_ws_url("query/" + self.carol.auth._token.access_token)
        # print(url)
        ws = create_connection(url)
        params = self.query_params.copy()

        if 'scrollable' in params:
            del params['scrollable']
        if 'pageSize' in params:
            del params['pageSize']
        params['query'] = self.json_query.copy()
        # print(params)

        ws.send(str(params))
        to_get = float("inf")
        downloaded = 0
        try:
            while ws.connected:
                result = json.loads(ws.recv())
                self.results.append(result)
                count += 1
                downloaded += 1
                if callback:
                    if callable(callback):
                        callback(result)
                    else:
                        raise Exception(
                            f'"{callback}" is a {type(callback)} and is not callable. This variable must be a function.')
                if self.print_status and count % 50 == 0:
                    print('{}/{}'.format(downloaded, to_get), end='\r')
                if self.save_results:
                    print('Saving...')
                    file.write(json.dumps(result, ensure_ascii=False))
                    file.write('\n')
                    if count % 1000 == 0:
                        file.flush()
                # print("Received '%s'" % result)
        except Exception as e:
            if self.save_results:
                file.close()
            print(f'{downloaded} rows downloaded before error')
            raise Exception(e.args[0]['errorMessage'])
        print("WS Closed")
        if self.save_results:
            file.close()

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

            result = self.carol.call_api(url_filter, data=self.json_query, params=self.query_params)

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

    def check_total_hits(self, json_query):
        """
        Check the total hits for a given query
        :param json_query: Json object with the query to use
        :return: number of records for this query
        """
        self.json_query = json_query
        url_filter = "v2/queries/filter?offset={}&pageSize={}&indexType={}".format(str(0), str(0), self.index_type)
        result = self.carol.call_api(url_filter, data=self.json_query)
        self.total_hits = result["totalHits"]
        return self.total_hits

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
        named = nq.namedQueries(self.token_object)
        named.getParamByName(named_query=named_query)
        return named.paramDict

    def query(self, json_query):
        self.json_query = json_query
        return self

    def all(self, dm_name):
        if not dm_name.endswith('Golden'):
            dm_name = dm_name + 'Golden'
        self.json_query = {"mustList": [{"mdmFilterType": "TYPE_FILTER", "mdmValue": dm_name}]}
        self.index_type = 'MASTER'
        return self

    def delete(self, json_query):
        # TODO: we should check the number of records to be deleted. If too many,
        # it can be a problem.
        self.json_query = json_query
        self.querystring = {"indexType": self.index_type}
        url_filter = "v2/queries/filter"
        result = self.carol.call_api(url_filter, data=self.json_query,
                                     params=self.querystring, method='DELETE')

        print('Deleted: ', result)


class ParQuery:

    def __init__(self, carol, backend='dask', return_df=True, verbose=50, n_jobs=4):
        self.carol = carol
        self.return_df=return_df
        self.backend = backend
        self.verbose=verbose
        self.n_jobs=n_jobs

        assert self.backend=='dask' or self.backend == 'joblib'

    @staticmethod
    def ranges(min_v, max_v, nb):
        step = int((max_v - min_v) / nb) + 1
        step = list(range(min_v, max_v, step))
        if step[-1] != max_v:
            step.append(max_v)
        step = [[step[i], step[i + 1] - 1] for i in range(len(step) - 1)]
        step.append([max_v, None])
        return step

    def _get_min_max(self):
        j = {"mustList": [{"mdmFilterType": "TYPE_FILTER", "mdmValue": f"{self.datamodel_name}"}],
             "aggregationList": [{"type": "MINIMUM", "name": "MINIMUM", "params": [f"{self.mdmKey}"]},
                                 {"type": "MAXIMUM", "name": "MAXIMUM", "params": [f"{self.mdmKey}"]}]}

        query = Query(self.carol, index_type=self.index_type, only_hits=False, get_aggs=True, save_results=False,
                      print_status=True, page_size=0).query(j).go()
        min_v = query.results[0]['aggs']['MINIMUM']['value']
        max_v = query.results[0]['aggs']['MAXIMUM']['value']
        print(f"Total Hits to download: {query.total_hits}")
        return min_v, max_v

    def go(self, datamodel_name=None, slices=1000, staging_name=None, connector_id=None, connector_name=None,
           get_staging_from_golden=False ):
        assert slices < 9999, '10k is the largest slice possible'


        if datamodel_name is None:
            assert connector_id or connector_name
            assert staging_name

            if not connector_id:
                connector_id = Connectors(self.carol).get_by_name(connector_name)['mdmId']

            self.index_type = 'STAGING'
            self.datamodel_name = f"{connector_id}_{staging_name}"
            self.fields=None
            self.only_hits=False
            self.mdmKey = 'mdmCreated'
        elif get_staging_from_golden:
            self.index_type = 'MASTER'
            self.datamodel_name = f"{datamodel_name}Master"
            self.fields = 'mdmStagingRecord'
            self.only_hits=False
            self.mdmKey = 'mdmStagingRecord.mdmCreated'
        else:
            self.index_type = 'MASTER'
            self.datamodel_name = f"{datamodel_name}Golden"
            self.fields = 'mdmGoldenFieldAndValues'
            self.only_hits=True
            self.mdmKey = 'mdmCounterForEntity'

        min_v, max_v = self._get_min_max()
        self.chunks = self.ranges(min_v, max_v, slices)

        print(f"Number of chunks: {len(self.chunks)}")


        if self.backend=='dask':
            list_to_compute = self._dask_backend()
        elif self.backend=='joblib':
            list_to_compute = self._joblib_backend()
        else:
            raise KeyError

        if self.return_df:
            return pd.concat(list_to_compute,ignore_index=True)
        list_to_compute = list(itertools.chain(*list_to_compute))
        return list_to_compute


    def _dask_backend(self):

        list_to_compute = []
        for RANGE_FILTER in self.chunks:
            y = dask.delayed(_par_query)(
                datamodel_name=self.datamodel_name,
                RANGE_FILTER=RANGE_FILTER,
                page_size=4999,
                login=self.carol,
                index_type=self.index_type,
                fields=self.fields,
                only_hits=self.only_hits,
                mdmKey=self.mdmKey,
                return_df=self.return_df,
            )
            list_to_compute.append(y)

        return dask.compute(*list_to_compute)

    def _joblib_backend(self):

        list_to_compute = Parallel(n_jobs=self.n_jobs,
                                   verbose=self.verbose)(delayed(_par_query)(
                                                        datamodel_name=self.datamodel_name,
                                                        RANGE_FILTER=RANGE_FILTER,
                                                        page_size=4999,
                                                        login=self.carol,
                                                        index_type=self.index_type,
                                                        fields=self.fields,
                                                        only_hits=self.only_hits,
                                                        mdmKey=self.mdmKey,
                                                        return_df=self.return_df,
                                                                             )
                                                    for RANGE_FILTER in self.chunks)
        return list_to_compute


def _par_query(datamodel_name, RANGE_FILTER, page_size=1000, login=None, index_type='MASTER',fields=None, mdmKey=None,
               only_hits=True, return_df=True):
    json_query = {
        "mustList": [
            {
                "mdmFilterType": "TYPE_FILTER",
                "mdmValue": datamodel_name
            },
            {
                "mdmFilterType": "RANGE_FILTER",
                "mdmKey": f"{mdmKey}",
                "mdmValue": RANGE_FILTER
            }
        ]

    }

    query = Query(login, page_size=page_size, save_results=False, print_status=False, index_type=index_type,
                  only_hits=only_hits,
                  fields=fields).query(json_query).go()
    query = query.results

    if not only_hits:
        query = [i['hits'] for i in query]
        query = list(itertools.chain(*query))
        if fields:
            query = [elem.get(fields, elem) for elem in query if
                     elem.get(fields,None)]

    if return_df:
        return pd.DataFrame(query)

    return query

