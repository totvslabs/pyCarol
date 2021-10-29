from ..flow.commons import Task

from pycarol import Carol, Staging, ApiKeyAuth, Storage
import luigi
import logging

logger = logging.getLogger(__name__)
luigi.auto_namespace(scope=__name__)
class IngestRecords(Task):
    # forcing the execution: 
    # even if the connector and stag remains the same
    # they may contain different data from previous runs
    #---------------------------------------------------
    datetime = luigi.Parameter()
    staging_name = luigi.Parameter() 
    connector_name = luigi.Parameter() 

    def easy_run(self, inputs):

        login = Carol()
        stg = Staging(login)

        X_cols = ["CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE", "DIS", "RAD", "TAX", "PTRATIO", "B", "LSTAT"] 
        y_col = ["target"]
        roi_cols = X_cols + y_col

        logger.info(f'Reading data from {self.connector_name}/{self.staging_name}.')
        data = stg.fetch_parquet(staging_name=self.staging_name,
                        connector_name=self.connector_name,
                        cds=True,
                        columns=roi_cols)

        logger.info(f'A total of {data.shape[0]} records were fetch from {self.connector_name}/{self.staging_name}.')

        return data