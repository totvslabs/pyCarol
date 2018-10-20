import json
from .data_models_fields import DataModelFields
from .verticals import Verticals
import time

class DataModel:

    def __init__(self, carol):
        self.carol = carol

        self.fields_dict = {}
        self.entity_template_ = {}

    def _build_query_params(self):
        if self.sort_by is None:
            self.query_params = {"offset": self.offset, "pageSize": str(self.page_size), "sortOrder": self.sort_order}
        else:
            self.query_params = {"offset": self.offset, "pageSize": str(self.page_size), "sortOrder": self.sort_order,
                                "sortBy": self.sort_by}

    def _get_name_type_data_models(self,fields):
        f = {}
        for field in fields:
            if field.get('mdmMappingDataType', None) not in ['NESTED', 'OBJECT']:
                f.update({field['mdmName']: field['mdmMappingDataType']})
            else:
                f[field['mdmName']] = self._get_name_type_data_models(field['mdmFields'])
        return f

    def _get(self,id, by = 'id'):

        if by == 'name':
            url = f"v1/entities/templates/name/{id}"
        elif by == 'id':
            url = f"v1/entities/templates/{id}/working"
        else:
            raise print('Type incorrect, it should be "id" or "name"')

        resp = self.carol.call_api(url, method='GET')
        self.entity_template_ = {resp['mdmName'] : resp}
        self.fields_dict.update({resp['mdmName']: self._get_name_type_data_models(resp['mdmFields'])})
        return resp


    def get_all(self, offset=0, page_size=-1, sort_order='ASC',
                sort_by=None, print_status=False,
                save_file=None):

        self.offset = offset
        self.page_size = page_size
        self.sort_order = sort_order
        self.sort_by = sort_by
        self._build_query_params()

        self.template_dict = {}
        self.template_data = []
        count = self.offset

        set_param = True
        self.total_hits = float("inf")
        if save_file:
            assert isinstance(save_file,str)
            file = open(save_file, 'w', encoding='utf8')
        while count < self.total_hits:
            url_filter = "v1/entities/templates"
            query = self.carol.call_api(url_filter, params=self.query_params, method='GET')

            if query['count'] == 0:
                print('There are no more results.')
                print('Expecting {}, reponse = {}'.format(self.total_hits, count))
                break
            count += query['count']
            if set_param:
                self.total_hits = query["totalHits"]
                set_param = False

            query = query['hits']
            self.template_data.extend(query)
            self.fields_dict.update({i['mdmName']: self._get_name_type_data_models(i['mdmFields'])
                                     for i in query})
            self.template_dict.update({i['mdmName']: {'mdmId': i['mdmId'],
                                                      'mdmEntitySpace': i['mdmEntitySpace'] }
                                       for i in query})

            self.query_params['offset'] = count
            if print_status:
                print('{}/{}'.format(count, self.total_hits), end='\r')
            if save_file:
                file.write(json.dumps(query, ensure_ascii=False))
                file.write('\n')
                file.flush()
        if save_file:
            file.close()
        return self

    def get_by_name(self,name):
        return self._get(name, by = 'name')

    def get_by_id(self,id):
        return self._get(id, by='id')

    def get_snapshot(self,dm_id,entity_space):
        url_snapshot = f'v1/entities/templates/{dm_id}/snapshot?entitySpace={entity_space}'
        resp = self.carol.call_api(url_snapshot, method='GET')
        self.snapshot_ = {resp['entityTemplateName']: resp}
        return resp

    def export(self, dm_name=None, dm_id=None, sync_dm=True, full_export=False):
        """

        Export datamodel to s3

        This method will trigger or pause the export of the data in the datamodel to
        s3

        :param dm_name: `str`, default `None`
            Datamodel Name
        :param dm_id: `str`, default `None`
            Datamodel id
        :param sync_dm: `bool`, default `True`
            Sync the data model
        :param full_export: `bool`, default `True`
            Do a resync of the data model
        :return: None
        """

        if sync_dm:
            status = 'RUNNING'
        else:
            status = 'PAUSED'

        if dm_name:
            dm_id= self.get_by_name(dm_name).entity_template_.get(dm_name)['mdmId']
        else:
            assert dm_id

        query_params = {"status": status, "fullExport":full_export}
        url = f'v1/entities/templates/{dm_id}/exporter'
        return  self.carol.call_api(url, method='POST', params=query_params)

    def delete(self, dm_id=None, dm_name=None, entity_space='WORKING'):
        #TODO: Check Possible entity_spaces

        if dm_id is None:
            assert dm_name is not None
            resp = self.get_by_name(dm_name)
            dm_id = resp['mdmId']

        url = "v1/entities/templates/{dm_id}"
        querystring = {"entitySpace": entity_space}

        return self.carol.call_api(url, method='DELETE', params=querystring)

    def _get_name_type_DMS(self,fields):
        f = {}
        for field in fields:
            if field.get('mdmMappingDataType', None) not in ['NESTED', 'OBJECT']:
                f.update({field['mdmName']: field['mdmMappingDataType']})
            else:
                f[field['mdmName']] = self._get_name_type_DMS(field['mdmFields'])
        return f



class entIntType(object):
    ent_type = 'long'


class entDoubleType(object):
    ent_type = 'double'


class entStringType(object):
    ent_type = "string"


class entNullType(object):
    ent_type = "string"


class entBooleanType(object):
    ent_type = "boolean"


class entArrayType(object):
    ent_type = "nested"


class entObjectType(object):
    ent_type = "object"


class entType(object):
    @classmethod
    def get_ent_type_for(cls, t):
        """docstring for get_schema_type_for"""
        SCHEMA_TYPES = {
            type(None): entNullType,
            str: entStringType,
            int: entIntType,
            float: entDoubleType,
            bool: entBooleanType,
            list: entArrayType,
            dict: entObjectType,
        }

        schema_type = SCHEMA_TYPES.get(t)

        if not schema_type:
            raise JsonEntTypeNotFound("There is no schema type for  %s.\n Try:\n %s" % (
            str(t), ",\n".join(["\t%s" % str(k) for k in SCHEMA_TYPES.keys()])))
        return schema_type


class JsonEntTypeNotFound(Exception):
    pass


class CreateDataModel(object):
    def __init__(self, carol):
        self.carol = carol
        self.template_dict = {}

        self.fields = DataModelFields(self.carol)
        self.fields.possible_types()
        self.all_possible_types = self.fields._possible_types
        self.all_possible_fields = self.fields.fields_dict

    def from_snapshot(self,snapshot, publish=False, overwrite=False):

        while True:
            url = 'v1/entities/templates/snapshot'
            resp = self.carol.call_api(url=url, method='POST', data=snapshot)

            if not resp.ok:
                if ('Record already exists' in self.lastResponse.json()['errorMessage']) and (overwrite):
                    del_DM = DataModel(self.carol)
                    del_DM.get_by_name(snapshot['entityTemplateName'])
                    dm_id = del_DM.entity_template_.get(snapshot['entityTemplateName']).get('mdmId',None)
                    if dm_id is None: #if None
                        continue
                    entity_space = del_DM.entity_template_.get(snapshot['entityTemplateName'])['mdmEntitySpace']
                    del_DM.delete(dm_id=dm_id, entity_space=entity_space)
                    time.sleep(0.5) #waint for deletion
                    continue

            break
        print('Data Model {} created'.format(snapshot['entityTemplateName']))
        self.template_dict.update({resp['mdmName']: resp})
        if publish:
            self.publish_template(resp['mdmId'])


    def publish_template(self, dm_id):

        url = f'v1/entities/templates/{dm_id}/publish'
        resp = self.carol.call_api(url=url, method='POST')
        return resp

    def _check_verticals(self):
        self.vertical_names = Verticals(self.carol).all()

        if self.mdmVerticalIds is not None:
            for key, value in self.vertical_names.items():
                if value == self.mdmVerticalIds:
                    self.mdmVerticalIds = value
                    self.mdmVerticalNames = key
                    return
        else:
            for key, value in self.vertical_names.items():
                if key == self.mdmVerticalNames:
                    self.mdmVerticalIds = value
                    self.mdmVerticalNames = key
                    return

        raise Exception('{}/{} are not valid values for mdmVerticalNames/mdmVerticalIds./n'
                        ' Possible values are: {}'.format(self.mdmVerticalNames, self.mdmVerticalIds,
                                                          self.vertical_names))

    def _checkEntityTemplateTypes(self):
        self.entityTemplateTypesDict = entityTemplateTypeIds(self.carol).all()

        if self.mdmEntityTemplateTypeIds is not None:
            for key, value in self.entityTemplateTypesDict.items():
                if value == self.mdmEntityTemplateTypeIds:
                    self.mdmEntityTemplateTypeIds = value
                    self.mdmEntityTemplateTypeNames = key
                    return
        else:
            for key, value in self.entityTemplateTypesDict.items():
                if key == self.mdmEntityTemplateTypeNames:
                    self.mdmEntityTemplateTypeIds = value
                    self.mdmEntityTemplateTypeNames = key
                    return

        raise Exception('{}/{} are not valid values for mdmEntityTemplateTypeNames/mdmEntityTemplateTypeIds./n'
                        ' Possible values are: {}'.format(self.mdmVerticalNames, self.mdmVerticalIds,
                                                          self.entityTemplateTypesDict))

    def _checkEntityTemplateName(self):

        est_ = entityTemplate(self.token_object)
        est_.getByName(self.mdmName)
        if not est_.entityTemplate_ == {}:
            raise Exception('mdm name {} already exist'.format(self.mdmName))

    def create(self, mdmName, overwrite = False, mdmVerticalIds=None, mdmVerticalNames=None, mdmEntityTemplateTypeIds=None,
               mdmEntityTemplateTypeNames=None, mdmLabel=None, mdmGroupName='Others',
               mdmTransactionDataModel=False):

        self.mdmName = mdmName
        self.mdmGroupName = mdmGroupName

        if not mdmLabel:
            self.mdmLabel = self.mdmName
        else:
            self.mdmLabel = mdmLabel


        self.mdmTransactionDataModel = mdmTransactionDataModel

        assert ((mdmVerticalNames is not None) or (mdmVerticalIds is not None))
        assert ((mdmEntityTemplateTypeIds is not None) or (mdmEntityTemplateTypeNames is not None))

        self.mdmVerticalNames = mdmVerticalNames
        self.mdmVerticalIds = mdmVerticalIds
        self.mdmEntityTemplateTypeIds = mdmEntityTemplateTypeIds
        self.mdmEntityTemplateTypeNames = mdmEntityTemplateTypeNames

        self._checkVerticals()
        self._checkEntityTemplateTypes()
        if not overwrite:
            self._checkEntityTemplateName()

        payload = {"mdmName": self.mdmName, "mdmGroupName": self.mdmGroupName, "mdmLabel": {"en-US": self.mdmLabel},
                   "mdmVerticalIds": [self.mdmVerticalIds],
                   "mdmEntityTemplateTypeIds": [self.mdmEntityTemplateTypeIds],
                   "mdmTransactionDataModel": self.mdmTransactionDataModel, "mdmProfileTitleFields": []}

        while True:
            url_filter = "https://{}.carol.ai{}/api/v1/entities/templates".format(self.token_object.domain , self.dev)
            self.lastResponse = requests.post(url=url_filter, headers=self.headers, json = payload)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token,
                                    'Content-Type': 'application/json'}
                    continue
                elif ('Record already exists' in self.lastResponse.json()['errorMessage']) and (overwrite):
                    del_DM = deleteTemplate(self.token_object)
                    find_temp = entityTemplate(self.token_object)
                    find_temp.getByName(self.mdmName)
                    entityTemplateId = find_temp.entityTemplate_.get(self.mdmName).get('mdmId',None)
                    if entityTemplateId is None: #if None
                        continue
                    entitySpace = find_temp.entityTemplate_.get(self.mdmName)['mdmEntitySpace']
                    del_DM.delete(entityTemplateId,entitySpace)
                    time.sleep(0.5) #waint for deletion
                    continue
                raise Exception(self.lastResponse.text)
            break

        self.lastResponse.encoding = 'utf8'
        response = json.loads(self.lastResponse.text)
        self.template_dict.update({response['mdmName']: response})


    def _profileTitle(self,profileTitle,entityTemplateId):
        if isinstance(profileTitle,str):
            profileTitle = [profileTitle]

            profileTitle = [i.lower() for i in profileTitle]

        while True:
            url = "https://{}.carol.ai{}/api/v1/entities/templates/{}/profileTitle".format(self.token_object.domain , self.dev,
                                                                                         entityTemplateId)

            payload = profileTitle  # list of fields
            self.lastResponse = requests.post(url, json=payload, headers=self.headers)
            if not self.lastResponse.ok:
                # error handler for token
                if self.lastResponse.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token,
                                    'Content-Type': 'application/json'}
                    continue
                raise Exception(self.lastResponse.text)
            break


    def addField(self,fieldName, entityTemplateId = None,parentFieldId = ""):

        if entityTemplateId is None:
            assert self.entityTemplateId
        else:
            est_ = entityTemplate(self.token_object)
            est_.getById(entityTemplateId)
            if est_.entityTemplate_ == {}:
                print('Template does not exisit')
                return
            self.entityTemplateId = entityTemplateId
            _ , template_ = est_.entityTemplate_.popitem()
            self.current_fields = [i for i in template_['mdmFieldsFull'].keys()]
            if fieldName.lower() in self.current_fields:
                print("'{}' already in the template".format(fieldName))
                return

        field_to_send = self.all_possible_fields.get(fieldName,[])
        if field_to_send == []:
            print('Field does not exist')
            return
        querystring = {"parentFieldId": parentFieldId}
        while True:
            url = "https://{}.carol.ai{}/api/v1/entities/templates/{}/onboardField/{}".format(self.token_object.domain , self.dev,
                                                                                            self.entityTemplateId, field_to_send['mdmId'])
            response = requests.request("POST", url, headers=self.headers, params=querystring)
            if not response.ok:
                # error handler for token
                if response.reason == 'Unauthorized':
                    self.token_object.refreshToken()
                    self.headers = {'Authorization': self.token_object.access_token,
                                    'Content-Type': 'application/json'}
                    continue
                raise Exception(response.text)
            break

    def _labelsAndDesc(self,prop):

        if self.mdmLabelMap is None:
            label = prop
        else:
            label = self.mdmLabelMap.get(prop,[])
            if label == []:
                label = prop
            else:
                label = {"en-US": label}

        if self.mdmDescriptionMap is None:
            description = prop
        else:
            description = self.mdmDescriptionMap.get(prop,[])
            if description == []:
                description = prop
            else:
                description = {"en-US": description}

        return label,  description



    def from_json(self,json_sample, profileTitle = None, publish = False, entityTemplateId = None, mdmLabelMap = None, mdmDescriptionMap = None ):

        if publish:
            assert profileTitle in json_sample

        self.mdmLabelMap = mdmLabelMap
        self.mdmDescriptionMap = mdmDescriptionMap

        if entityTemplateId is None:
            assert self.template_dict != {}
            templateName , templateJson = self.template_dict.copy().popitem()
            self.entityTemplateId = templateJson['mdmId']

        else:
            self.entityTemplateId = entityTemplateId

        self.json_sample = json_sample

        n_fields = len(list(self.json_sample))
        count = 0
        for prop, value in self.json_sample.items():
            count +=1
            print('Creating {}/{}'.format(count,n_fields))
            prop = prop.lower()
            entity_type = entType.get_ent_type_for(type(value))
            if prop in self.all_possible_fields.keys():
                if not entity_type.ent_type == 'nested':
                    ent_ = self.all_possible_fields.get(prop, []).copy()
                    ent_.pop('mdmCreated')
                    ent_.pop('mdmLastUpdated')
                    ent_.pop('mdmTenantId')
                    if (ent_['mdmMappingDataType'].lower() == entity_type.ent_type):
                        self.addField(prop, parentFieldId="")
                    else:
                        print('problem, {} not created, field name matches with an already'
                              'created field but different type'.format(prop))
                else:
                    print('Nested fields are not supported')
            else:
                if not entity_type.ent_type == 'nested':

                    currentLabel, currentDescription = self._labelsAndDesc(prop)
                    self.fields.create(mdmName = prop, mdmMappingDataType = entity_type.ent_type, mdmFieldType= 'PRIMITIVE',
                                       mdmLabel = currentLabel, mdmDescription = currentDescription)
                    self.all_possible_fields = self.fields.fields_dict
                    self.addField(prop, parentFieldId="")
                else:
                    print('Nested fields are not supported')

        if publish:
            self._profileTitle(profileTitle, self.entityTemplateId)
            self.publish_template(self.entityTemplateId)

                #to_create = create_field(prop, value)
                #print(to_create)

    #not done
    def _nested(self,mdmName, value, parentId=''):
        raise ValueError('not implemented')
        payload = {"mdmName": mdmName, "mdmMappingDataType": entity_type.ent_type,
                   "mdmLabel": {"en-US": mdmName}, "mdmDescription": {"en-US": mdmName}}
        entity_type = entType.get_ent_type_for(type(value))

        if entity_type == entObjectType and len(value) > 0:
            for key, val in value.items():
                payload['mdmFieldType'] = 'NESTED'
                print('criando NESTED')
                parentId = 1234
                create_field(key, val, parentId=parentId)

        elif entity_type == entArrayType and len(value) > 0:
            pass

        if entity_type.ent_type == 'nested':

            payload['mdmFieldType'] = 'NESTED'
            print('criando NESTED')
            parentId = 1234
            _, parentId = create_field()
        else:
            payload['mdmFieldType'] = 'PRIMITIVE'
            print('criando PRIMITIVE')

        return payload, parentId

