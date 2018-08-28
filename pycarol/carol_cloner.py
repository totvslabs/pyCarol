class Cloner:
    def __init__(self, carol):
        self.domain = carol.domain
        self.app_name = carol.app_name
        self.port = carol.port
        self.verbose = carol.verbose
        self.connector_id = carol.connector_id
        self.auth = carol.auth.cloner()

    def build(self):
        from .carol import Carol
        return Carol(self.domain, self.app_name, self.auth.build(), connector_id=None, port=self.port, verbose=self.verbose)
