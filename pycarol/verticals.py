import json


class Verticals(object):
    def __init__(self, carol):

        self.carol = carol
        self.offset = 0
        self.page_size = 100
        self.sort_order = 'ASC'
        self.sort_by = None
        self.total_hits = float("inf")

    def _build_query_params(self):
        if self.sort_by is None:
            self.query_params = {"offset": self.offset, "pageSize": str(self.page_size), "sortOrder": self.sort_order}
        else:
            self.query_params = {"offset": self.offset, "pageSize": str(self.page_size), "sortOrder": self.sort_order,
                                "sortBy": self.sort_by}

    def all(self, offset=0, page_size=100, sort_order='ASC', sort_by=None, print_status=True, save_file=False,
               filename='data/verticalsIds.json'):

        self.offset = offset
        self.page_size = page_size
        self.sort_order = sort_order
        self.sort_by = sort_by
        self._build_query_params()

        self.verticals_dict = {}
        self.verticals_data = []
        count = self.offset


        set_param = True
        self.total_hits = float("inf")
        if save_file:
            file = open(filename, 'w', encoding='utf8')
        while count < self.total_hits:
            url_filter = "v1/verticals"
            query = self.carol.call_api(url_filter, params=self.query_params)
            count += query['count']
            if set_param:
                self.total_hits = query["totalHits"]
                set_param = False

            query = query['hits']
            self.verticals_data.extend(query)
            self.verticals_dict.update({i['mdmName']: i['mdmId'] for i in query})
            self.query_params['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.total_hits), end ='\r')
            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()
        return self.verticals_dict

    def get(self,names=None, ids=None, save_file=False,
                  filename='data/verticals_schema.json'):

        if names is None:
            assert ids is not None
            if isinstance(ids,str):
                iter_list = [ids]
            url_filter = "v1/verticals/"
        else:
            if isinstance(ids,names):
                iter_list = [names]
            url_filter = "v1/verticals/name/"


        if save_file:
            file = open(filename, 'w', encoding='utf8')

        self.verticals_dict = {}
        self.verticals_data = []
        for it in iter_list:

            url_filter = url_filter+ f"{it}"
            query = self.carol.call_api(url_filter, params=self.query_params)

            self.verticals_data.extend(query)
            self.verticals_dict.update({i['mdmName']: i['mdmId'] for i in query})

            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()
        return self.verticals_dict
