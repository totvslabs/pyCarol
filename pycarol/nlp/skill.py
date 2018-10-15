class Skill:
    
    def __init__(self, json):
        self._json = json
        self.name = json['nlpName']
        self.texts = []
        self.rich_elements = []
        self.required = json.get('nlpRequiredEntityTypes', [])
        self.optional = json.get('nlpOptionalEntityTypes', [])
        self.at_least_one = json.get('nlpAtLeastOneEntityType', [])
        self.example_question = json.get('nlpExampleQuestion')
        self.context_model = json.get('nlpContextModel')
        self.voice = json.get('nlpAnswerModel', {}).get('nlpVoiceMessage')
        self.query_models = json.get('nlpAnswerModel', {}).get('nlpNamedQueryModels', {})
        self.related_skills = json.get('nlpAnswerModel', {}).get('nlpRelatedSkills', [])
        self.fallback_answer = json.get('nlpAnswerModel', {}).get('nlpFallbackAnswer')
        
        if 'nlpAnswerModel' in json and 'nlpSkillAnswerData' in json['nlpAnswerModel']:
            for element in json['nlpAnswerModel']['nlpSkillAnswerData']:
                if 'url' not in element and 'content' in element:
                    self.texts.append(element['content'])
                else:
                    self.rich_elements.append(element)
    
    def add_text(self, text):
        _text_element = dict(
            type='html',
            width=0,
            height=0,
            content=text
        )
        self.texts.append(text)
        if 'nlpAnswerModel' in self._json and 'nlpSkillAnswerData' in self._json['nlpAnswerModel']:
            self._json['nlpAnswerModel']['nlpSkillAnswerData'].append(_text_element)
        else:
            if 'nlpAnswerModel' not in self._json:
                self._json['nlpAnswerModel'] = {}
            self._json['nlpAnswerModel']['nlpSkillAnswerData'] = [_text_element]
    
    def add_entities_required(self, required):
        assert isinstance(required,list)
        self.required.extend(required)
        self._json['nlpRequiredEntityTypes'] = self.required

    def add_entities_at_least_one(self, at_least_one):
        assert isinstance(at_least_one, list)
        self.at_least_one.extend(at_least_one)
        self._json['nlpAtLeastOneEntityType'] = self.at_least_one
        
    def add_entities_optional(self, optional):
        assert isinstance(optional, list)
        self.optional.extend(optional)
        self._json['nlpOptionalEntityTypes'] = self.optional
    
    def add_related_skills(self, related_skills):
        assert isinstance(related_skills, list)
        self.related_skills.extend(related_skills)
        if 'nlpAnswerModel' not in self._json:
            self._json['nlpAnswerModel'] = {}
        self._json['nlpAnswerModel']['nlpRelatedSkills'] = self.related_skills            
        
    def add_rich_element(self, url, content_type='image', width=200, height=200):
        _rich_element = dict(
            type=content_type,
            width=width,
            height=height,
            url=url
        )
        self.rich_elements.append(_rich_element)
        if 'nlpAnswerModel' in self._json and 'nlpSkillAnswerData' in self._json['nlpAnswerModel']:
            self._json['nlpAnswerModel']['nlpSkillAnswerData'].append(_rich_element)
        else:
            if 'nlpAnswerModel' not in self._json:
                self._json['nlpAnswerModel'] = {}
            self._json['nlpAnswerModel']['nlpSkillAnswerData'] = [_rich_element]
        
    def add_context_model(self, context_model_name, missing_message, complete_message, confirmation_message,
                          entity_fields = None, numerical_fields = None, text_fields = None):
        _context_model = dict(
            nlpName=context_model_name,
            nlpMissingMessage=missing_message,
            nlpCompleteMessage=complete_message,
            nlpConfirmationMessage=confirmation_message
        )

        if entity_fields is not None:
            _context_model['nlpEntityFieldList'] = entity_fields
        if numerical_fields is not None:
            _context_model['nlpNumericalFieldList'] = numerical_fields
        if text_fields is not None:
            _context_model['nlpTextFieldList'] = text_fields

        self.context_model = _context_model
        self._json['nlpContextModel'] = self.context_model
        
    def add_query_model(self, query_model_name, display_name, query_name, primary_key = None, secondary_key = None,
                        output_params = {}, input_params = {}, flags = None, sort_by = None, sort_direction = 'ASC',
                        disambiguate = False):
        _query_model = dict(
            nlpDisplayName = display_name,
            nlpQueryName = query_name,
            nlpSortDirection = sort_direction,
            nlpDisambiguate = disambiguate,
            nlpOutputParams = output_params,
            nlpInputParams = input_params,
            nlpFlags = flags
        )
        if flags is None:
            _query_model['nlpFlag'] = []
        if primary_key is not None:
            _query_model['nlpPrimaryKey'] = primary_key
        if secondary_key is not None:
            _query_model['nlpSecondaryKey'] = secondary_key
        if sort_by is not None:
            _query_model['nlpSortBy'] = sort_by
        self.query_models.update({query_model_name : _query_model})
        if 'nlpAnswerModel' not in self._json:
            self._json['nlpAnswerModel'] = []
        self._json['nlpAnswerModel']['nlpNamedQueryModels'] = self.query_models

        
    def _update_json(self):
        self._json['nlpName'] = self.name
        if 'nlpAnswerModel' not in self._json:
            self._json['nlpAnswerModel'] = {}
        if self.texts and self.voice is None:
            self.voice = self.texts[0]
        self._json['nlpAnswerModel']['nlpVoiceMessage'] = self.voice
        self._json['nlpAnswerModel']['nlpNamedQueryModels'] = self.query_models
        self._json['nlpRequiredEntityTypes'] = self.required
        self._json['nlpOptionalEntityTypes'] = self.optional
        self._json['nlpAtLeastOneEntityType'] = self.at_least_one
        self._json['nlpAnswerModel']['nlpRelatedSkills'] = self.related_skills
        self._json['nlpExampleQuestion'] = self.example_question
        self._json['nlpAnswerModel']['nlpFallbackAnswer'] = self.fallback_answer
        self._json['nlpContextModel'] = self.context_model
        self._json['nlpAnswerModel']['nlpNamedQueryModels'] = self.query_models
        self._json['nlpAnswerModel']['nlpSkillAnswerData'] = []
        for text in self.texts:
            self._json['nlpAnswerModel']['nlpSkillAnswerData'].append({'type': 'html', 'width': 0, 'height': 0, 'content': text})
        self._json['nlpAnswerModel']['nlpSkillAnswerData'].extend(self.rich_elements)

        self._json.pop('mdmCreated',None)
        self._json.pop('mdmId',None)
        self._json.pop('mdmLastUpdated',None)
        self._json.pop('mdmTenantId',None)
     

