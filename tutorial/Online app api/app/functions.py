import logging
from pycarol import Storage, Staging

logger = logging.getLogger(__name__)


class Functions:
    """
    Class that contains functions that may be run when endpoints are hit by requests.

    Args:

        carol: carol: Carol object
            Carol object.

    """
    
    def __init__(self, carol):
        self.carol = carol

    def load_model(self, model_filename, app_name, model_app_name):

        if model_app_name and model_app_name != app_name:
            self.carol.app_name = model_app_name
        storage =  Storage(self.carol)
        self.carol.app_name = app_name
        model = storage.load(model_filename, format='pickle', cache=False, chunk_size=None)
        return model

    def model_predict(self, model, crim, zn, indus, chas, nox, rm, age, dis, rad, tax, pratio, b, lstat):
        if model is None:
            model = self.load_model()
        price = model.predict([[crim, zn, indus, chas, nox, rm, age, dis, rad, tax, pratio, b, lstat]])
        return round(float(price), 2)

    def send_data_to_carol(self, staging_name, connector_name,  df):

        staging = Staging(self.carol)
        staging.send_data(
            staging_name=staging_name, connector_name=connector_name, data=df, force=True
        )
