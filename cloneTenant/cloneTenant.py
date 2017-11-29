#from cloneTenant.functions import *
from pycarol import entityTemplateCarol as ett
from pycarol import applicationsCarol as appl
from pycarol import stagingCarol as stg
from pycarol import entityMappingsCarol as etm
from collections import defaultdict


class cloneTenant(object):
    def __init__(self, token_from, token_to):
        self.token_from = token_from
        self.token_to = token_to
        if self.token_from.access_token is None:
            self.token_from.newToken()
        if self.token_to.access_token is None:
            self.token_to.newToken()

        self.headers_from = {'Authorization': self.token_from.access_token, 'Content-Type': 'application/json'}
        self.headers_to = {'Authorization': self.token_to.access_token, 'Content-Type': 'application/json'}

    def copyDMs(self,dm_list = None, overwrite = False):

        assert dm_list is not None
        DMsTenant = ett.entityTemplate(self.token_from)

        if isinstance(dm_list,str):
            dm_list = [dm_list]

        dm_to_copy = {}
        snapshot_dict = {}
        dm_tocreate = ett.createTemplate(self.token_to)


        for dm_name in dm_list:
            DMsTenant.getByName(dm_name)
            current_template = DMsTenant.entityTemplate_.get(dm_name)
            dm_to_copy.update({ dm_name : { 'mdmId': current_template['mdmId'],
                                       'mdmEntitySpace': current_template['mdmEntitySpace']}})

            DMsTenant.getSnapshot(current_template['mdmId'],current_template['mdmEntitySpace'] )
            current_snap = DMsTenant.snapshot_
            snapshot_dict.update({dm_name :  current_snap[dm_name] })
            dm_tocreate.fromSnapshot(current_snap[dm_name],publish=True,overwrite = overwrite)
            dm_tocreate.template_dict[dm_name]['mdmId']

    def copyAllDMs(self,overwrite = False):
        DMsTenant = ett.entityTemplate(self.token_from)
        DMsTenant.getAll()
        dm_list = DMsTenant.template_dict
        snapshot_dict = {}
        dm_tocreate = ett.createTemplate(self.token_to)

        for dm_name, params in dm_list.items():
            DMsTenant.getSnapshot(params['mdmId'], params['mdmEntitySpace'])
            current_snap = DMsTenant.snapshot_
            snapshot_dict.update({dm_name: current_snap[dm_name]})
            dm_tocreate.fromSnapshot(current_snap[dm_name], publish=True, overwrite=overwrite)
            dm_tocreate.template_dict[dm_name]['mdmId']


    def copyAllConnectors(self,staging=True, mapping = True, overwrite=False):

        conn = appl.connectorsCarol(self.token_from)
        conn.getAll(includeMappings=True)
        conn_to_create = conn.connectors

        conn_id = {}

        stag = stg.stagingSchema(self.token_from)
        mappings_to_get = etm.entityMapping(self.token_from)
        self.stag_mapp_to_use = defaultdict(list)

        for connector in conn_to_create:

            connectorName = connector.get('mdmName',None)
            connectorLabel = connector.get('mdmLabel',None)
            if connectorLabel:
                connectorLabel= connectorLabel['en-US']
            else:
                connectorLabel = None
            groupName = connector.get('mdmGroupName',None)

            conn_to = appl.connectorsCarol(self.token_to)
            conn_to.createConnector(connectorName,connectorLabel,groupName,overwrite=overwrite)
            conn_id.update({connectorName : conn_to.connectorId})
            self.token_to.newToken(connectorId=conn_to.connectorId)

            #self.token_to.newToken(connectorId='188a65d0d52c11e7b5090242ac110003')#deletar e descomnetar acima
            #conn_id.update({connectorName: '188a65d0d52c11e7b5090242ac110003'}) #deletar e descomnetar acima

            for schema_name, mapping_fields in connector.get('mdmEntityMappings', None).items():
                stag.getSchema(schema_name,connector.get('mdmId'))

                aux_schema = stag.schema
                aux_schema.pop('mdmTenantId')
                aux_schema.pop('mdmStagingApplicationId')
                aux_schema.pop('mdmId')
                aux_schema.pop('mdmCreated')
                aux_schema.pop('mdmLastUpdated')

                mapping_fields.pop('mdmTenantId')
                entityMappingsId = mapping_fields.pop('mdmId')
                entitySpace = mapping_fields.get('mdmEntitySpace')
                mapping_fields.pop('mdmCreated')
                mapping_fields.pop('mdmLastUpdated')
                connectorId = mapping_fields.pop('mdmApplicationId')

                mappings_to_get.getSnapshot(connectorId,entityMappingsId,entitySpace)

                _, aux_map = mappings_to_get.snap.popitem()

                self.stag_mapp_to_use[connectorName].append({"schema": aux_schema, "mapping": aux_map})

                stg_to = stg.stagingSchema(self.token_to)
                stg_to.sendSchema(fields_dict = aux_schema, connectorId = conn_id.get(connectorName),
                                  overwrite=overwrite)

                mapping_to = etm.entityMapping(self.token_to)
                mapping_to.createFromSnnapshot(aux_map,conn_id.get(connectorName),overwrite=overwrite)






import json
from pycarol import loginCarol, applicationsCarol

with open('../../carol-ds-retail/config.json') as json_data:
    d = json.loads(json_data.read())
token_object = loginCarol.loginCarol(**d['todimot'])
token_object.newToken()

token_to = loginCarol.loginCarol(**d['mario'])
token_to.newToken()
print(token_object.access_token)




ct = cloneTenant(token_object,token_to)

#ct.copyAllDMs()


#ct.copyDMs(['customer','product'])
ct.copyAllConnectors(overwrite=True)


#ct.copyAllDMs()
#ct.copyDMs(['productforecast','customer'],overwrite= True)







