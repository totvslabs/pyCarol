class Entity:
    
    def __init__(self, json):
        self._json = json
        self.name = json['nlpName']
        if 'nlpValues' in json:
            self.values = json['nlpValues']
        else:
            self.values = []
        if 'nlpCanonicalValue' in json:
            self.canonical_value = json['nlpCanonicalValue']
        else:
            self.canonical_value = None
            
    def add_values(self, values):
        self._json['nlpValues'].append(values)
        values += values  #???
        
    def _update_json(self):
        self._json['nlpName'] = self.name
        self._json['nlpValues'] = self.values
        self._json['nlpCanonicalValue'] = self.canonical_value

        self._json.pop('mdmCreated',None)
        self._json.pop('mdmId',None)
        self._json.pop('mdmLastUpdated',None)
        self._json.pop('mdmTenantId',None)