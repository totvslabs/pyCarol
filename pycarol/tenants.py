
class Tenants:
    def __init__(self, carol):
        self.carol = carol

    def get_tenant_by_domain(self, domain):
        return self.carol.call_api('v2/tenants/domain/{}'.format(domain), auth=False)


