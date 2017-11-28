#from cloneTenant.functions import *
from pycarol import entityTemplateCarol as ett


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
        self.all_dms = False

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
        self.all_dms = True
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



import json
from pycarol import loginCarol

with open('../../carol-ds-retail/config.json') as json_data:
    d = json.loads(json_data.read())
token_object = loginCarol.loginCarol(**d['todimot'])
token_object.newToken()

token_to = loginCarol.loginCarol(**d['mario'])
token_to.newToken()


print(token_object.access_token)




ct = cloneTenant(token_object,token_to)

ct.copyAllDMs()

ct.copyDMs(['productforecast','customer'],overwrite= True)



