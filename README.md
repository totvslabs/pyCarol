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
        
    - v2 Staging (stagingCarol.py)
        1. POST - /api/v2/staging/tables/{table}
        2. GET - /api/v2/staging/tables/{table}
        3. POST - /api/v2/staging/tables/{table}/schema
        
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
        
    - v1 Database Toolbelt Admin (toolbeltAdmin.py)
        1. DELETE - /api/v1/databaseToolbelt/filter  (deprecated)
        
 We also have a Schema Generator (schemaGenerator.py).







