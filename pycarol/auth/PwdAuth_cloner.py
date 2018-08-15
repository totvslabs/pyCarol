

class PwdAuthCloner:
    def __init__(self, auth):
        self.user = auth.user
        self.password = auth.password

    def build(self):
        from pycarol.auth.PwdAuth import PwdAuth
        return PwdAuth(self.user, self.password)
