

class ApiKeyAuthCloner:
    def __init__(self, auth):
        self.api_key = auth.api_key

    def build(self):
        from pycarol.auth.ApiKeyAuth import ApiKeyAuth
        return ApiKeyAuth(self.api_key)
