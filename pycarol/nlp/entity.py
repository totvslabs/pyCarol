class Entity:
    
    def __init__(self, json):
        self._json = json
        self.name = json['nlpName']
        self.values = json.get('nlpValues', [])
        self.canonical_value = json.get('nlpCanonicalValue')
            
    def add_values(self, values):
        assert isinstance(values, list)
        self.values.extend(values)
        self._json['nlpValues'] = self.values
        
    def _update_json(self):
        self._json['nlpName'] = self.name
        self._json['nlpValues'] = self.values
        self._json['nlpCanonicalValue'] = self.canonical_value

        self._json.pop('mdmCreated',None)
        self._json.pop('mdmId',None)
        self._json.pop('mdmLastUpdated',None)
        self._json.pop('mdmTenantId',None)