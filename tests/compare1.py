def namedQuery(self, named_query, json_query, offset=0, pageSize=50, sortOrder='ASC', indexType='MASTER',
               only_hits=True, sortBy='mdmLastUpdated', safe_check='False',
               print_status=True, save_result=True, filename='results_json.json'):
    self.offset = offset
    self.pageSize = pageSize
    self.sortOrder = sortOrder
    self.indexType = indexType
    self.sortBy = sortBy
    self.query_data = []
    self.only_hits = only_hits

    self._setQuerystring()

    set_param = True
    count = self.offset
    self.totalHits = float("inf")
    if save_result:
        file = open(filename, 'w', encoding='utf8')
    while count < self.totalHits:
        url_filter = "https://{}.carol.ai/api/v2/queries/named/{}".format(self.token_object.domain, named_query)
        self.lastResponse = requests.post(url=url_filter, headers=self.headers, params=self.querystring,
                                          json=json_query)
        if not self.lastResponse.ok:
            # error handler for token
            if self.lastResponse.reason == 'Unauthorized':
                self.token_object.refreshToken()
                self.headers = {'Authorization': self.token_object.access_token, 'Content-Type': 'application/json'}
                continue
            if save_result:
                file.close()
            raise Exception(self.lastResponse.text)

        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        count += query['count']
        if set_param:
            self.totalHits = query["totalHits"]
            set_param = False
            if safe_check:
                mdmId_list = []

        if self.only_hits:
            query = query['hits']
            if safe_check:
                mdmId_list.extend([mdm_id['mdmId'] for mdm_id in query])
                if len(mdmId_list) > len(set(mdmId_list)):
                    raise Exception('There are repeated records')
            query = [elem['mdmGoldenFieldAndValues'] for elem in query]
            self.query_data.extend(query)
        else:
            query.pop('count')
            query.pop('took')
            query.pop('totalHits')
            self.query_data.append(query)
            if save_result:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
                break

        self.querystring['offset'] = count
        if print_status:
            print('{}/{}'.format(count, self.totalHits), end='\r')
        if save_result:
            file.write(json.dumps(query, ensure_ascii=False))
            file.write('\n')
            file.flush()
    if save_result:
        file.close()