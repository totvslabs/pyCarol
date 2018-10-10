class SkillConfig:
    
    def __init__(self, json):
        self._json = json
        self.levenshtein_distance = json['nlpLevenshteinDistance']
        self.case_sensitive = json.get('nlpCaseSensitive', False)
        self.stemming = json.get('nlpStemming', False)
        self.external_parsing = json.get('nlpExternalParsing', False)
        self.language = json.get('nlpLanguage')
        self.name = json.get('nlpName')
        
    def _update_json(self):
        self._json['nlpLevenshteinDistance'] = self.levenshtein_distance
        self._json['nlpCaseSensitive'] = self.case_sensitive
        self._json['nlpStemming'] = self.stemming
        self._json['nlpExternalParsing'] = self.external_parsing
        if self.language is not None:
            self._json['nlpLanguage'] = self.language

        self._json.pop('nlpName', None)
        self._json.pop('mdmCreated', None)
        self._json.pop('mdmId', None)
        self._json.pop('mdmLastUpdated', None)
        self._json.pop('mdmTenantId', None)