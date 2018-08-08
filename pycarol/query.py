import json
from websocket import create_connection


class Query:
    """ It implements the calls for the following endpoints:
        1. POST - /api/v2/queries/filter
        2. POST - /api/v2/queries/named/{query_name}
        2. DELETE - /api/v2/queries/filter
        2. POST - /api/v2/queries/filter/{scrollId}

    Usage::



    """
    def __init__(self, carol, max_hits=float('inf'), offset=0, page_size=1000, sort_order='ASC', sort_by=None,
                 scrollable=True, index_type='MASTER', only_hits=True, fields=None, get_aggs=False,
                 save_results=False, filename='query_result.json', print_status=True, safe_check=False,
                 get_errors=False, flush_result=False, use_stream=False):
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

        self.named_query = None
        self.callback = None
        
        # Crated to send to the Rest API
        self.query_params = None
        self.drop_list = None
        self.json_query = None
        self.total_hits = None

        self.results = []

        if self.max_hits == float('inf'):
            self.get_all = True
        else:
            self.get_all = False

    def _build_query_params(self):
        if self.sort_by is None:
            self.query_params = {"offset": self.offset, "pageSize": self.page_size, "sortOrder": self.sort_order,
                                "indexType": self.index_type}
        else:
            self.query_params = {"offset": self.offset, "pageSize": self.page_size, "sortOrder": self.sort_order,
                                "sortBy": self.sort_by, "indexType": self.index_type}

        if self.scrollable:
            self.query_params.update({"scrollable": self.scrollable})
        if self.fields:
            self.query_params.update({"fields": self.fields})

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
        print(url)
        ws = create_connection(url)

        params = self.query_params.copy()
        if 'scrollable' in params:
            del params['scrollable']
        if 'pageSize' in params:
            del params['pageSize']
        params['query'] = self.json_query.copy()

        print(params)
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

                #print("Received '%s'" % result)
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
            try:
                result = self.carol.call_api(url_filter, data=self.json_query, params=self.query_params)
            except Exception as e:
                if self.save_results:
                    file.close()
                raise Exception(e.args[0]['errorMessage'])

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

            if self.only_hits:
                result = result['hits']
                if self.safe_check:
                    self.mdmId_list.extend([mdm_id['mdmId'] for mdm_id in result])
                    if len(self.mdmId_list) > len(set(self.mdmId_list)):
                        raise Exception('There are repeated records')

                if self.get_errors:
                    self.query_errors.update({elem.get('mdmId',elem) :  elem.get('mdmErrors',elem) for elem in result if elem['mdmErrors']})

                result = [elem.get('mdmGoldenFieldAndValues',elem) for elem in result if elem.get('mdmGoldenFieldAndValues',None)]  #get mdmGoldenFieldAndValues if not empty and if it exists

                if not self.flush_result:
                    self.results.extend(result)
            else:
                result.pop('count')
                result.pop('took')
                result.pop('totalHits')
                #result.pop('scrollId')
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
                    raise Exception(f'"{callback}" is a {type(callback)} and is not callable. This variable must be a function.')
            
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

    def named_query_params(self,named_query):
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


        #TODO: we should check the number of records to be deleted. If too many,
        #it can be a problem.
        self.json_query = json_query
        self.querystring = {"indexType": self.index_type}
        url_filter = "v2/queries/filter"
        result = self.carol.call_api(url_filter, data=self.json_query,
                                     params=self.querystring, method='DELETE')

        print('Deleted: ',result)