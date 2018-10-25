class Skill:

    def __init__(self, name, voice, example_question, texts=None, rich_elements=None,
                 required=None, optional=None, at_least_one=None, context_model=None,
                 query_models=None, related_skills=None, fallback_answer=None):
        self.name = name
        self.voice = voice
        self.example_question = example_question

        if texts is None:
            self.texts = []
        else:
            self.texts = texts

        if rich_elements is None:
            self.rich_elements = []
        else:
            self.rich_elements = rich_elements

        if required is None:
            self.required = []
        else:
            self.required = required

        if optional is None:
            self.optional = []
        else:
            self.optional = optional

        if at_least_one is None:
            self.at_least_one = []
        else:
            self.at_least_one = at_least_one

        self.context_model = context_model

        if query_models is None:
            self.query_models = []
        else:
            self.query_models = query_models

        if related_skills is None:
            self.related_skills = []
        else:
            self.related_skills = related_skills

        self.fallback_answer = fallback_answer

        self._json_data = {}
        self._update_json_data()

    @classmethod
    def from_json(cls, json_data):
        skill = cls(name=json_data['nlpName'],
                    voice=json_data.get('nlpAnswerModel', {}).get('nlpVoiceMessage'),
                    example_question=json_data.get('nlpExampleQuestion'),
                    texts=[],
                    rich_elements=[],
                    required=json_data.get('nlpRequiredEntityTypes', []),
                    optional=json_data.get('nlpOptionalEntityTypes', []),
                    at_least_one=json_data.get('nlpAtLeastOneEntityType', []),
                    context_model=json_data.get('nlpContextModel'),
                    query_models=json_data.get('nlpAnswerModel', {}).get('nlpNamedQueryModels', {}),
                    related_skills=json_data.get('nlpAnswerModel', {}).get('nlpRelatedSkills', []),
                    fallback_answer=json_data.get('nlpAnswerModel', {}).get('nlpFallbackAnswer')
                    )

        if 'nlpAnswerModel' in json_data and 'nlpSkillAnswerData' in json_data['nlpAnswerModel']:
            for element in json_data['nlpAnswerModel']['nlpSkillAnswerData']:
                if 'url' not in element and 'content' in element:
                    skill.texts.append(element['content'])
                else:
                    skill.rich_elements.append(element)

        return skill

    def add_text(self, text):
        _text_element = dict(
            type='html',
            width=0,
            height=0,
            content=text
        )
        self.texts.append(text)
        if 'nlpAnswerModel' in self._json_data and 'nlpSkillAnswerData' in self._json_data['nlpAnswerModel']:
            self._json_data['nlpAnswerModel']['nlpSkillAnswerData'].append(_text_element)
        else:
            if 'nlpAnswerModel' not in self._json_data:
                self._json_data['nlpAnswerModel'] = {}
            self._json_data['nlpAnswerModel']['nlpSkillAnswerData'] = [_text_element]
    
    def add_required(self, required):
        assert isinstance(required, list)
        self.required.extend(required)
        self._json_data['nlpRequiredEntityTypes'] = self.required

    def add_at_least_one(self, at_least_one):
        assert isinstance(at_least_one, list)
        self.at_least_one.extend(at_least_one)
        self._json_data['nlpAtLeastOneEntityType'] = self.at_least_one
        
    def add_optional(self, optional):
        assert isinstance(optional, list)
        self.optional.extend(optional)
        self._json_data['nlpOptionalEntityTypes'] = self.optional
    
    def add_related(self, related_skills):
        assert isinstance(related_skills, list)
        self.related_skills.extend(related_skills)
        if 'nlpAnswerModel' not in self._json_data:
            self._json_data['nlpAnswerModel'] = {}
        self._json_data['nlpAnswerModel']['nlpRelatedSkills'] = self.related_skills
        
    def add_rich(self, url, content_type='image', width=200, height=200):
        _rich_element = dict(
            type=content_type,
            width=width,
            height=height,
            url=url
        )
        self.rich_elements.append(_rich_element)
        if 'nlpAnswerModel' in self._json_data and 'nlpSkillAnswerData' in self._json_data['nlpAnswerModel']:
            self._json_data['nlpAnswerModel']['nlpSkillAnswerData'].append(_rich_element)
        else:
            if 'nlpAnswerModel' not in self._json_data:
                self._json_data['nlpAnswerModel'] = {}
            self._json_data['nlpAnswerModel']['nlpSkillAnswerData'] = [_rich_element]
        
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
        self._json_data['nlpContextModel'] = self.context_model
        
    def add_query_model(self, model_name, query_name, primary_key = None, secondary_key = None,
                        output_params = None, input_params = None, flags = None, sort_by = None, sort_direction = 'ASC',
                        disambiguate = False):
        _query_model = dict(
            nlpName=model_name,
            nlpQueryName=query_name,
            nlpSortDirection=sort_direction,
            nlpDisambiguate=disambiguate,
            nlpOutputParams=output_params,
            nlpInputParams=input_params,
            nlpFlags=flags
        )
        if flags is None:
            _query_model['nlpFlag'] = []
        if primary_key is not None:
            _query_model['nlpPrimaryKey'] = primary_key
        if secondary_key is not None:
            _query_model['nlpSecondaryKey'] = secondary_key
        if sort_by is not None:
            _query_model['nlpSortBy'] = sort_by
        self.query_models.append(_query_model)
        if 'nlpAnswerModel' not in self._json_data:
            self._json_data['nlpAnswerModel'] = []
        self._json_data['nlpAnswerModel']['nlpNamedQueryModels'] = self.query_models

        
    def _update_json_data(self):
        self._json_data['nlpName'] = self.name
        if 'nlpAnswerModel' not in self._json_data:
            self._json_data['nlpAnswerModel'] = {}
        if self.texts and self.voice is None:
            self.voice = self.texts[0]
        self._json_data['nlpAnswerModel']['nlpVoiceMessage'] = self.voice
        self._json_data['nlpAnswerModel']['nlpNamedQueryModels'] = self.query_models
        self._json_data['nlpRequiredEntityTypes'] = self.required
        self._json_data['nlpOptionalEntityTypes'] = self.optional
        self._json_data['nlpAtLeastOneEntityType'] = self.at_least_one
        self._json_data['nlpAnswerModel']['nlpRelatedSkills'] = self.related_skills
        self._json_data['nlpExampleQuestion'] = self.example_question
        self._json_data['nlpAnswerModel']['nlpFallbackAnswer'] = self.fallback_answer
        self._json_data['nlpContextModel'] = self.context_model
        self._json_data['nlpAnswerModel']['nlpSkillAnswerData'] = []
        for text in self.texts:
            self._json_data['nlpAnswerModel']['nlpSkillAnswerData'].append({'type': 'html', 'width': 0, 'height': 0, 'content': text})
        self._json_data['nlpAnswerModel']['nlpSkillAnswerData'].extend(self.rich_elements)
        self._json_data.pop('mdmCreated',None)
        self._json_data.pop('mdmId',None)
        self._json_data.pop('mdmLastUpdated',None)
        self._json_data.pop('mdmTenantId',None)

    def __str__(self):
        self._update_json_data()
        return str(self._json_data)
