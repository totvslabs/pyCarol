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
        values += values
        
    def _update_json(self):
        self._json['nlpName'] = self.name
        self._json['nlpValues'] = self.values
        self._json['nlpCanonicalValue'] = self.canonical_value
               
        if 'mdmCreated' in self._json:
            self._json.pop('mdmCreated')
        if 'mdmId' in self._json:
            self._json.pop('mdmId')
        if 'mdmLastUpdated' in self._json:
            self._json.pop('mdmLastUpdated')
        if 'mdmTenantId' in self._json:
            self._json.pop('mdmTenantId')
        
