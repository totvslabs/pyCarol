# pyCarol   <img src="https://github.com/TOTVS/carol-ds-retail/blob/master/recommender-project/image/pycarol.jpg" alt="Carol" width="32" data-canonical-src="http://svgur.com/i/ue.svg"> 



This package implements some of Carol's APIs. The following endpoints are implemented: 
    
    - V2 OAuth2: (loginCarol.py)
        1. POST - /api/v2/oauth2/token
        2. POST/GET - /api/v2/oauth2/token/{access_token}
        
    - v2 Tenants (utils.py)
        1. GET - /api/v2/tenants/domain/{domain}
        
    - v2 Queries (queriesCarol.py)
        1. POST - /api/v2/queries/filter
        2. POST - /api/v2/queries/named/{query_name}
        2. DELETE - /api/v2/queries/filter 
        2. POST - /api/v2/queries/filter/{scrollId}
        
    - v2 Named Queries (namedQueryCarol.py)
        1. GET/POST - /api/v2/named_queries
        2. DELETE - /api/v2/named_queries/name/{name}
        3. GET - /api/v2/named_queries/name/{name}
        
    - v2 Staging (stagingCarol.py)
        1. GET/POST - /api/v2/staging/tables/{table}
        2. POST - /api/v2/staging/tables/{table}/schema
        
    - v1 Entity Template Types (entityTemplateTypesCarol.py)
        1. GET - /api/v1/entityTemplateTypes
        
    - v1 Fields (fieldsCarol.py)
        1. GET - /api/v1/admin/fields
        2. GET - /api/v1/admin/fields/{mdmId}
        3. GET - /api/v1/admin/fields/possibleTypes
        4. GET - /api/v1/fields
        5. GET - /api/v1/fields/{mdmId}
        6. GET - /api/v1/fields/possibleTypes
    
    - v1 Verticals (verticalsCarol.py)
        1. GET - /api/v1/verticals
        2. GET - /api/v1/verticals/{verticalId}
        3. GET - /api/v1/verticals/name/{verticalName}

    - v1 Applications (applicationsCarol.py)
        1. POST - /api/v1/applications
        2. DELETE - /api/v1/applications/{applicationId}
        3. GET - /api/v1/applications/name/{applicationName}

    - v1 Database Toolbelt Admin (toolbeltAdmin.py)
        1. DELETE - /api/v1/databaseToolbelt/filter  (deprecated)
        
 We also have a Schema Generator (schemaGenerator.py).
 
 
 ### Using pyCarol:
 

 All APIs need a login object. It creates/refreshes tokens.
```python
from pycarol import loginCarol
token_object = loginCarol.loginCarol(username= username, password=my_password, 
                                     domain = my_domain, connectorId=my_connectorId)
token_object.newToken()
print('This is a valid access token {}'.format(token_object.access_token))
token_object.refreshToken()
print('This is refreshed access token {}'.format(token_object.access_token))
```  

##### Processing filter queries. 


```python
from pycarol import loginCarol, queriesCarol
token_object = loginCarol.loginCarol(username= username, password=my_password, 
                                     domain = my_domain, connectorId=my_connectorId)
                                    
token_object.newToken()


json_query = {
          "mustList": [
            {
              "mdmFilterType": "TYPE_FILTER",
              "mdmValue": "ratingsGolden"
            },
            {
              "mdmFilterType": "TERM_FILTER",
              "mdmKey": "mdmGoldenFieldAndValues.userid",
              "mdmValue": 406
            }
          ]
        }
        
query_response = queriesCarol.queryCarol(token_object)
#To get all records returned:
query_response.newQuery(json_query= json_query, max_hits = float('inf'),only_hits=True)
#To get only 1000
query_response.newQuery(json_query= json_query, max_hits = 1000, only_hits=True)
#the response is here
query_response.query_data
#If I want to save the response 
query_response.newQuery(json_query= json_query, max_hits = 1000, only_hits=True, save_results = True,
                        filename = 'PATCH/response.json')
```  
The parameter `only_hits = True` will make sure that the only records into the path `$hits.mdmGoldenFieldAndValues`.
 If you want all the response use `only_hits = False`. Also, if your filter has aggregation, you should use 
 `only_hits = False`. 


##### Processing named queries. 

```python
from pycarol import loginCarol, queriesCarol
token_object = loginCarol.loginCarol(username= username, password=my_password, 
                                     domain = my_domain, connectorId=my_connectorId)
                                    
token_object.newToken()


named_query = 'revenueHist'
payload = {"bin":"1d","cnpj":"24386434000130"} 
named_query_resp = queriesCarol.queryCarol(token_object)
#To get all records returned:
named_query_resp.namedQuery(named_query = named_query, json_query = payload)
#the response is here
named_query_resp.query_data

```
It is possible to use all the parameters used in the filter query, i.e., `only_hits` , `only_hits`, etc.
For more information for the possible input parameters check the docstring.
 
 







