import pandas as pd
import numpy as np

from pycarol import loginCarol, stagingCarol


import json

'''
with open('E:\\carol-ds-retail\\RMS-project\\config.json') as data_file:
    login_info = json.load(data_file)
login = loginCarol.loginCarol(**login_info['pcsistemas'])
login.newToken()

products =[]
dtypes = {'categoryid':str, 'departmentid':str, 'ean':str, 'groupid': str, 'mdmdescription':str, 'tag': str ,
          'producterpid':str, 'sectionid':str,'shortdescription':str, 'shortdescription2':str,'subgroupid':str}
cont = 0
with open('E:\\carol-ds-retail\\RMS-project\\data\\productGolden.json') as ff:
    for file in ff.readlines():
        cont+=1
        df = pd.read_json(file,orient='records',dtype=dtypes)
        products.append(df)
products= pd.concat(products,ignore_index=True)
products.rename(columns={'producterpid':'product_id','mdmdescription':'product_name','departmentid':'department_id',
                        'categoryid':'category_id','groupid':'group_id','sectionid':'section_id',
                         'subgroupid':'subgroup_id'},inplace=True)
products['subgroup_id']=products['subgroup_id'].astype('category')
products['group_id'] = products['group_id'].astype('category')
products['section_id']=products['section_id'].astype('category')
products['department_id']=products['department_id'].astype('category')
products['category_id'] =products['category_id'].astype('category')

send_data = stagingCarol.sendDataCarol(login)

send_data = send_data.sendData(stagingName= 'products',data = products,
                                                step_size = 1000, applicationId='48940be08f5a11e7b76ddec7170128a9',
                                                print_stats = True)

'''

with open('E:\\carol-ds-retail\\RMS-project\\config.json') as data_file:
    login_info = json.load(data_file)
login = loginCarol.loginCarol(**login_info['todimo'])
login.newToken()

import pandas as pd
import pickle
with open('E:\\carol-ds-retail\\todimo-project\data\\forecasts_all.pickle', 'rb') as f:
    df_forecast2 = pickle.load(f)

df_forecast2 = pd.concat(df_forecast2,ignore_index=True)
mapp = {'fit':'past'}
df_forecast2.replace({'recordtype':mapp},inplace=True)
df_forecast2.branch = 'firecast_all'

send_data = stagingCarol.sendDataCarol(login)
send_data = send_data.sendData(stagingName= 'forecast', data = df_forecast2,
                               step_size = 1000, connectorId='03d0ef30b40511e78d1ddec7170128a9',
                               print_stats = True)

with open('E:\\carol-ds-retail\\todimo-project\data\\data\\forecasts.pickle', 'rb') as f:
    df_forecast = pickle.load(f)
df_forecast.replace({'recordtype':mapp},inplace=True)