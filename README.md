# pyCarol

This package implements some of Carol's APIs. The following endpoints are implemented: 
    
    - V2 OAuth2: (loginCarol.py)
        1. POST - /api/v2/oauth2/token
        2. POST/GET - /api/v2/oauth2/token/{access_token}
        
    - v2 Tenants (utils.py)
        1. GET - /api/v2/tenants/domain/{domain}
        
    - v2 Queries (queriesCarol.py)
        1. POST - /api/v2/queries/filter
        2. POST - /api/v2/queries/named/{query_name}
        
    - v2 Named Queries (namedQueryCarol.py)
        1. GET/POST - /api/v2/named_queries
        2. DELETE - /api/v2/named_queries/name/{name}
        
    - v2 Staging (stagingCarol.py)
        1. POST - /api/v2/staging/tables/{table}
        2. GET - /api/v2/staging/tables/{table}
        3. POST - /api/v2/staging/tables/{table}/schema
        
    - v1 Database Toolbelt Admin (toolbeltAdmin.py)
        1. DELETE - /api/v1/databaseToolbelt/filter
        
 We also have a Schema Generator (schemaGenerator.py).







