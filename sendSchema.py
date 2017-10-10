import pandas as pd
import numpy as np
import json
import requests


class Table(object):
    def __init__(self, name, token, subdomain, connector_id):



        self.name = name
        self.token = token  # format 02845jfjj3420382
        self.subdomain = 'https://%s.carol.ai' % subdomain
        self.conn_id = connector_id

    def prepBody(self, primary_key, fields_dict):
        if type(primary_key) != list:
            primary_key = [primary_key]

        fields = set(fields_dict.keys())
        for key_field in primary_key:
            if key_field not in fields:
                raise Exception('Your key field %s is not in your fields!' % (key_field))
            if type(key_field) != str:
                raise Exception('Field %s type is not string, must be!' % key_field)

        query = {
            "mdmCrosswalkTemplate": {
                "mdmCrossreference": {
                    self.name: primary_key
                }
            },
            "mdmStagingMapping": {
                "properties": fields_dict

            }
        }

        body = json.dumps(query)

        return body

    def submitSchema(self, primary_key, fields_dict):
        body = self.prepBody(primary_key, fields_dict)

        url = self.subdomain + '/api/v2/staging/tables/%s/schema?applicationId=%s' % (self.name, self.conn_id)

        headers = {'Authorization': self.token,
                   'content-type': 'application/json'}
        res = requests.post(url, data=body, headers=headers)

        if res.status_code != 200:
            print('Error - Response: %d' % res.status_code)
        else:
            print('Schema sent succesfully!')

        # token = res.json()['access_token']
