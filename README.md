# pyCarol

This package implements some of the Carol's APIs. The following endpoints are implemented: 
    
    - V2 OAuth2:
        1. POST - /api/v2/oauth2/token
        2. POST - /api/v2/oauth2/token/{access_token}
        3. GET - /api/v2/oauth2/token/{access_token}
        
    - v2 Tenants
        1. GET - /api/v2/tenants/domain/{domain}
        
    - v2 Queries
        1. POST - /api/v2/queries/filter
        2. POST - /api/v2/queries/named/{query_name}
        
    - v2 Named Queries
        1. GET/POST - /api/v2/named_queries
        2. DELETE - /api/v2/named_queries/name/{name}
        
    - v2 Staging
        1. POST - /api/v2/staging/tables/{table}
        
    - v1 Database Toolbelt Admin
        1. DELETE - /api/v1/databaseToolbelt/filter
        
 We also have a Schema Generator.







