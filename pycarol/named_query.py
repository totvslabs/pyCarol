import json
import re


class NamedQuery:

    def __init__(self, carol, max_hits=float('inf'), offset=0, page_size=1000, sort_order='ASC', sort_by=None,
                 index_type='MASTER', only_hits=True, fields=None, get_aggs=False,
                 save_results=False, filename=None, print_status=True,
                 ):

        self.named_query_data = []
        self.named_query_dict = {}

        self.carol = carol
        self.max_hits = max_hits
        self.offset = offset
        self.page_size = page_size
        self.sort_order = sort_order
        self.sort_by = sort_by
        self.index_type = index_type
        self.only_hits = only_hits
        self.fields = fields
        self.get_aggs = get_aggs

        self.save_results = save_results

        if self.save_results:
            assert filename, "`save_results=True`, you should specify the filename"
        self.filename = filename
        self.print_status = print_status

        self.param_dict = {}

        self.results = []

        if self.max_hits == float('inf'):
            self._get_all = True
        else:
            self._get_all = False

    def _build_query_params(self):
        self.query_params = {"offset": self.offset, "pageSize": self.page_size, "sortOrder": self.sort_order,
                             "indexType": self.index_type}

        if self.sort_by is not None:
            self.query_params["sortBy"] = self.sort_by

    def _get_param(self, named_query=None):
        assert self.named_query_dict
        self.param_dict = {}
        if named_query is None:
            for key, value in self.named_query_dict.items():
                self.param_dict[key] = re.findall(r'\{\{(.*?)\}\}', json.dumps(value, ensure_ascii=False))

    def get_all(self):

        self._build_query_params()
        self.named_query_data = []
        count = self.offset

        set_param = True
        to_get = float("inf")
        downloaded = 0
        if self.save_results:
            file = open(self.filename, 'w', encoding='utf8')

        url_filter = "v2/named_queries"
        while count < to_get:

            result = self.carol.call_api(url_filter, params=self.query_params)

            if set_param:
                self.total_hits = result["totalHits"]
                if self._get_all:
                    to_get = result["totalHits"]
                elif self.max_hits <= result["totalHits"]:
                    to_get = self.max_hits
                else:
                    to_get = result["totalHits"]
                set_param = False

            count += result['count']
            downloaded += result['count']
            query = result['hits']

            self.named_query_data.extend(query)
            self.named_query_dict.update({i['mdmQueryName']: i for i in query})
            self.query_params['offset'] = count
            if self.print_status:
                print('{}/{}'.format(count, self.total_hits), end='\r')
            if self.save_results:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if self.save_results:
            file.close()
        self._get_param()

        return self.named_query_dict

    def by_name(self, named_query):

        self.named_query_data = []
        if self.save_results:
            file = open(self.filename, 'w', encoding='utf8')

        url_filter = "v2/named_queries/name/{}".format(named_query)
        result = self.carol.call_api(url_filter)

        self.named_query_dict.update({result['mdmQueryName']: result})

        if self.save_results:
            file.write(json.dumps(result, ensure_ascii=False))
            file.write('\n')
            file.flush()
            file.close()
        self._get_param()

        return result

    def create_named_query(self, named_query, overwrite=True):

        if isinstance(named_query, dict):
            named_query = [named_query]
        else:
            assert isinstance(named_query, list)

        url_filter = 'v2/named_queries'
        count = 0
        _requet_type = "POST"
        for query in named_query:
            count += 1
            query.pop('mdmId', None)
            query.pop('mdmTenantId', None)

            try:
                old_query = self.by_name(query['mdmQueryName'])
            except Exception as e:
                if 'Not found' in e.args[0]:
                    result = self.carol.call_api(path=url_filter, method=_requet_type, data=query)
            else:
                if overwrite:
                    _requet_type = "PUT"
                    mdm_id = self.named_query_dict[old_query['mdmQueryName']]['mdmId']
                    url_filter = 'v2/named_queries/{}'.format(mdm_id)
                    result = self.carol.call_api(path=url_filter, method=_requet_type, data=query)
                else:
                    print(f"{old_query['mdmQueryName']} "
                          f"already exists and will not be copied, use `overwrite=True` to overwrite")

            print('{}/{} named queries copied'.format(count, len(named_query)), end='\r')
