from pycarol.query import *
import pandas as pd
from luigi import Parameter
from pycarol.pipeline.task import Task


class MDMIngestion(Task):
    data_model = Parameter()

    def easy_run(self, inputs):
        data = Query(self.carol()).all(self.data_model).go().results
        return pd.DataFrame(data)
