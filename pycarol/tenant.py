from .query import Query
from .filter import *
import itertools
from .data_models import DataModel

class Tenant:
    def __init__(self, carol):
        self.carol = carol

    def get_tenant_by_domain(self, domain):
        """
        Get tenant information.

        :param domain: `str`
            Tenant name
        :return:
            dict with the information about the tenant.
        """
        return self.carol.call_api('v2/tenants/domain/{}'.format(domain), auth=False)


    def _get_staging(self):
        """
        Get export status for data models

        :return: `dict`
            dict with the information of which staging table is exporting its data.
        """

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
        """
        Get export status for data models

        :return: `dict`
            dict with the information of which data model is exporting its data.
        """

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
        """
        Find all data models or stating tables that have export active.

        :param staging: `bool`, default `True`
            Get status for exports in staging.
        :param data_model: `bool`, default `True`
            Get status for exports in data models.
        :return: `tuple`
            staging stats, data model stats
        """

        staging_results = None
        dm_results = None
        if staging:
            staging_results = self._get_staging()

        if data_model:
            dm_results = self._get_dm()


        return staging_results, dm_results


