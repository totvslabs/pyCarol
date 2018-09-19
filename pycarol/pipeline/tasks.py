from ..task import Task, Parameter
from pycarol.query import Query
from pycarol.carol import Carol
import pandas as pd


def type_filter(table, suffix="Golden"):
    json = {
        "mdmFilterType": "TYPE_FILTER",
        "mdmValue": table + suffix
    }
    return json


def term_filter(field, value):
    json = {
        "mdmFilterType": "TERM_FILTER",
        "mdmKey": "mdmGoldenFieldAndValues." + field,
        "mdmValue": value
    }
    return json


class MDMIngestion(Task):
    tenant = Parameter()
    data_model = Parameter()
    set_target = Task.sqlite_target

    def easy_run(self, inputs):
        login = Carol(**auth_dict[self.tenant])

        q = dict(mustList=[type_filter(self.data_model), ])

        query = Query(
            login,
            page_size=4999,
            save_results=False,
            print_status= True,
            fields='mdmGoldenFieldAndValues'
        )
        query_response = query.query(q).go().results

        return pd.DataFrame(query_response)


class MDMIngestionFiltered(Task):
    tenant = Parameter()
    data_model = Parameter()
    query_filter = Parameter()
    set_target = Task.sqlite_target

    def easy_run(self, inputs):

        login = Carol(**auth_dict[self.tenant])

        q = dict(mustList=[
            type_filter(self.data_model),
            term_filter(*self.query_filter)
        ])

        query = Query(
            login,
            page_size=4999,
            save_results=False,
            print_status=True,
            fields='mdmGoldenFieldAndValues'
        )
        query_response = query.query(q).go().results

        return pd.DataFrame(query_response)