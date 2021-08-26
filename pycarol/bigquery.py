import json

import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials


SA_FILEPATH = "/home/jro/wk/totvs/sa.json"
PROJECT = "labs-app-mdm-production"


class BigQuery:
    """"""

    def __init__(self) -> None:
        token = self._get_token()
        with open(SA_FILEPATH, "r") as f:
            cred_dict = json.loads(f.read())
        credentials = Credentials.from_service_account_info(token)
        self.client = bigquery.Client(project=PROJECT, credentials=credentials)

    def _get_token(self):
        """Get API from Carol."""
        ...

    def query(self, query: str) -> pd.DataFrame:
        # sql = """
        #     SELECT *
        #     FROM `4c2c9090e7c611e893bf0e900682978b.dm_clockinrecords`
        #     LIMIT 100
        # """
        return self.client.query(query).to_dataframe()


def get_staging_handle(staging_name: str) -> str:
    """Convert a staging name to its handle on Big Query structure."""
    ...

def get_model_handle(model_name: str) -> str:
    """Convert a model name to its handle on Big Query structure."""
    ...
