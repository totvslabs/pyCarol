from .query import Query
from .filter import *
import itertools
from .data_models import DataModel

class Tenant:
    def __init__(self, carol):
        self.carol = carol

    def get_tenant_by_domain(self, domain):
        return self.carol.call_api('v2/tenants/domain/{}'.format(domain), auth=False)


    def _get_staging(self):

        query = Query(self.carol, index_type='CONFIG', only_hits=False)

        json_q = Filter.Builder(key_prefix="") \
            .must(TYPE_FILTER(value="mdmStagingDataExport")).build().to_json()

        query.query(json_q, ).go()
        staging_results = query.results
        staging_results = [elem.get('hits', elem) for elem in staging_results
                           if elem.get('hits', None)]
        staging_results = list(itertools.chain(*staging_results))
        if staging_results is not None:
            return {i['mdmStagingType'] : i for i in staging_results}


    def _get_dm(self):

        json_q  = Filter.Builder(key_prefix="")\
            .must(TYPE_FILTER(value="mdmEntityTemplateExport")).build().to_json()

        query = Query(self.carol, index_type='CONFIG', page_size=2, only_hits=False)
        query.query(json_q,).go()

        dm_results = query.results
        dm_results = [elem.get('hits', elem) for elem in dm_results
                               if elem.get('hits', None)]
        dm_results = list(itertools.chain(*dm_results))

        dm = DataModel(self.carol).get_all().template_data
        dm = {i['mdmId'] : i['mdmName'] for i in dm}

        if dm_results is not None:
            return {dm[i['mdmEntityTemplateId']] : i for i in dm_results}

        return dm_results


    def check_exports(self, staging=True, data_model=True):

        staging_results = None
        dm_results = None
        if staging:
            staging_results = self._get_staging()

        if data_model:
            dm_results = self._get_dm()


        return staging_results, dm_results


