class Entity:
    
    def __init__(self, name, values, canonical_value=None):
        self.name = name
        self.values = values
        self.canonical_value = canonical_value

        self._json_data = {}
        self._update_json_data()

    @classmethod
    def from_json(cls, json_data):
        return cls(name=json_data['nlpName'],
                   values=json_data.get('nlpValues', []),
                   canonical_value=json_data.get('nlpCanonicalValue'))

    def add_values(self, values):
        if isinstance(values, list):
            self.values.extend(values)
        else:
            self.values.append(values)
        self._json_data['nlpValues'] = self.values
        
    def _update_json_data(self):
        self._json_data['nlpName'] = self.name
        self._json_data['nlpValues'] = self.values
        self._json_data['nlpCanonicalValue'] = self.canonical_value
        self._json_data.pop('mdmCreated',None)
        self._json_data.pop('mdmId',None)
        self._json_data.pop('mdmLastUpdated',None)
        self._json_data.pop('mdmTenantId',None)

