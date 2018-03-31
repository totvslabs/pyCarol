import itertools
import requests
import sys
import csv
import re
import string
import pandas as pd

class skillCarol:
    """ Implements calls for the endpoints:
        1. /api/v1/ai/skill
        2. /api/v1/ai/skillEntity
        3. /api/v1/ai/nlp/query
    """
    
    # Voice and text patterns hold default substitutions for skill texts, e.g. "totvs" -> "tótus" / "TOTVS" for voice/text
    def __init__(self, token_object, voice_patterns='utils/voice_patterns.csv', text_patterns='utils/text_patterns.csv'):
        self.dev = token_object.dev
        self.token_object = token_object
        self.headers = self.token_object.headers_to_use
        try:            
            csvfile = open(voice_patterns, newline='')
            pats = csv.reader(csvfile, delimiter=',')
            self.voice_patterns = [(re.compile(re.escape(pat), re.IGNORECASE), sub) for [pat,sub] in pats]
        except:
            self.voice_patterns = []
        try:
            csvfile = open(text_patterns, newline='')
            pats = csv.reader(csvfile, delimiter=',')
            self.text_patterns = [(re.compile(re.escape(pat), re.IGNORECASE), sub) for [pat,sub] in pats]
        except:
            self.text_patterns = []
        
    def getSkills(self, pagesize=100):
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill?pageSize={}&sortOrder=ASC".format(self.token_object.domain, self.dev, pagesize)
        
        self.lastResponse = requests.get(url=url_filter, headers=self.headers)
        self.lastResponse.enconding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query['hits']
    
    def getSkillByName(self, name):
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)
        
        self.lastResponse = requests.get(url=url_filter, headers=self.headers)
        self.lastResponse.enconding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
    
    def postSkill(self, skill):
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/".format(self.token_object.domain, self.dev)
        
        self.lastResponse = requests.get(url=url_filter, headers=headers.self.headers, json=skill)
        self.lastResponse.enconding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
    
    # Create skill from name, required, and text
    def postSkillLazy(name, required_entity_list, skill_text, use_subs=True):
        # Build skill json
        voice_text = skill_text
        if use_subs:
            voice_text = self.treatVoiceMessage(skill_text)
            skill_text = self.treatTextMessage(skill_text)
        
        s_json = {}
        s_json['nlpName'] = name
        s_json['nlpRequiredEntityTypes'] = required_entity_list
        answer_model = {}
        answer_model['nlpVoiceMessage'] = voice_text
        answer_data = []
        answer_data.append({})
        answer_data[0]['content'] = skill_text
        answer_data[0]['height'] = 0
        answer_data[0]['type'] = 'html'
        answer_data[0]['width'] = 0
        answer_model['nlpSkillAnswerData'] = answer_data
        s_json['nlpAnswerModel'] = answer_model
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/".format(self.token_object.domain, self.dev)
    
        self.lastResponse = requests.post(url=url_filter, headers=headers.self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
    
    def getEntities(self, pagesize=200):
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skillEntity?pageSize={}&sortOrder=ASC".format(self.token_object.domain, self.dev, pagesize)
        
        self.lastResponse = requests.get(url=url_filter, headers=self.headers)
        self.lastResponse.enconding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query['hits']
    
    def getEntityByName(self, name):
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skillEntity/name/{}".format(self.token_object.domain, self.dev, name)
        
        self.lastResponse = requests.get(url=url_filter, headers=self.headers)
        self.lastResponse.enconding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
    
    def postEntity(self, entity):
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skillEntiy/".format(self.token_object.domain, self.dev)
        
        self.lastResponse = requests.get(url=url_filter, headers=headers.self.headers, json=skill)
        self.lastResponse.enconding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
    
    #Create entity given only names and values
    def postEntityLazy(self, name, values, canonical=''):
        #Build entity json
        s_json = {}
        s_json['nlpName'] = name
        s_json['nlpValues'] = values
        s_json['nlpCanonicalValue'] = canonical
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skillEntity/".format(self.token_object.domain, self.dev)
    
        self.lastResponse = requests.post(url=url_filter, headers=headers.self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
    
    
    # Edit text answer
    def editSkillText(self, name, new_text):
        s_json = self.getSkillByName(name)
        s_json['nlpAnswerModel']['nlpSkillAnswerData'][0]['content'] = new_text
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query

    # Edit voice answer
    def editSkillVoice(self, name, new_message):
        s_json = self.getSkillByName(name)
        s_json['nlpAnswerModel']['nlpVoiceMessage'] = new_message
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query

    # Replace required entities
    def editSkillRequired(self, name, new_required):
        s_json = self.getSkillByName(name)
        s_json['nlpRequiredEntityTypes'] = new_required
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query


    # Replace atLeastOne entitites
    def editSkillAtLeastOne(name, new_atleast):
        s_json = self.getSkillByName(name)
        s_json['nlpAtLeastOneEntityType'] = new_required
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
        
    # Replace optional entities
    def editSkillOptional(self, name, new_optional):
        s_json = self.getSkillByName(name)
        s_json['nlpOptionalEntityTypes'] = new_optional
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query

    # Append to required list
    def addSkillsRequired(self, name, required):
        s_json = self.getSkillByName(name)
        s_json['nlpRequiredEntityTypes'] += required
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
        
    # Append to atleastone list
    def addSkillsAtLeastOne(self, name, atleast):
        s_json = self.getSkillByName(name)
        s_json['nlpAtLeastOneEntityType'] += atleast
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
        
    # Append to optional list
    def addSkillsOptional(self, name, optional):
        s_json = self.getSkillByName(name)
        s_json['nlpOptionalEntityTypes'] += required
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
    
    # Replace related skills list
    def editRelatedSkills(self, name, related):
        s_json = self.getSkillByName(name)
        s_json['nlpAnswerModel']['nlpRelatedSkills'] = related
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
    
    # Append to related skills list
    def addRelatedSkills(self, name, related):
        s_json = self.getSkillByName(name)
        s_json['nlpAnswerModel']['nlpRelatedSkills'] += related
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query

    # Replace example 
    def editSkillExampleQuestion(name, example):
        s_json = self.getSkillByName(name)
        s_json['nlpExampleQuestion'] += required
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
        
    # Replace entity value list
    def editEntityValues(self, name, values):
        if isinstance(values, str):
            return "Please send list of values, not string"
        s_json = self.getEntityByName(name)
        s_json['nlpValues'] = values
        
        url_filter = "https://{}.carol.ai{}/api/v3/ai/skillEntity/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
        
    # Append to entity value list
    def addEntityValues(self, name, values):
        if isinstance(values, str):
            return "Please send list of values, not string"
        s_json = self.getEntityByName(name)
        s_json['nlpValues'] += values
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skillEntity/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
    
    # Add rich element (video/image/chart) to answer
    def addRichElement(self, name, url, content_type='image', width=200, height=200):
        s_json = get_skill(name)
        rich_element = {}
        rich_element['type'] = content_type
        rich_element['width'] = width
        rich_element['height'] = height
        rich_element['url'] = url
        s_json['nlpAnswerModel']['nlpSkillAnswerData'].append(rich_element)
        
        url_filter = "https://{}.carol.ai{}/api/v1/ai/skill/name/{}".format(self.token_object.domain, self.dev, name)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.encoding = 'utf8'
        query = json.loads(self.lastResponse.text)
    
    def treatVoiceMessage(self, message, pattern_file='voice_patterns.csv'):
        for pat in self.voice_patterns:
            message = pat[0].sub(pat[1], message)
            
        return message
        
    def treatTextMessage(self, message, pattern_file='voice_patterns.csv'):
        for pat in self.text_patterns:
            message = pat[0].sub(pat[1], message)
            
        return message
    
    # Make question to the engine
    def question(self, question):
        url_filter = "https://{}.carol.ai{}/api/v1/ai/nlp/query/".format(self.token_object.domain, self.dev)
        s_json = {}
        s_json['question'] = question
        
        self.lastResponse = requests.post(url=url_filter, headers=self.headers, json=s_json)
        self.lastResponse.enconding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
    
    # Give feedback: positive/negative
    def giveFeedback(self, answer, feedback):
        if feedback not in ['positive', 'negative']:
            print('Please use positive/negative')
        
        logId = answer['nlpLogId']
        url_filter = "https://{}.carol.ai{}/api/v1/ai/nlp/feedback/{}".format(self.token_object.domain, self.dev, logId)

        self.lastResponse = requests.put(url=url_filter, headers=self.headers, data=feedback)
        self.lastResponse.enconding = 'utf8'
        query = json.loads(self.lastResponse.text)
        
        return query
    
    def getUnrecognizedQuestions(self, pagesize=1000):
        url_filter = "https://{}.carol.ai{}/api/v1/ai/nlp/feedback?pageSize={}&sortOrder=ASC".format(self.token_object.domain, self.dev, pagesize)
        
        self.lastResponse = requests.get(url=url_filter, headers = self.headers)
        self.lastResponse.enconding = 'utf8'
        query = json.loads(self.lastResponse.text)['hits']

        unrecognized = [answer['nlpQuestion'] for answer in query if answer['nlpAnswer']['recognized'] == False]
        unrecognized = [question for question in unrecognized if question != 'Não entendi. Tente falar mais alto ou digite no campo de texto.']
        unrecognized = list(set(list(unrecognized)))
    
        return unrecognized
