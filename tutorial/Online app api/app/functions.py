import logging
from pycarol import Storage, Staging

logger = logging.getLogger(__name__)


class Functions:
    '''
    Class that contains functions that may be run when endpoints are hit by requests.

    Args:

        carol: carol: Carol object
            Carol object.

    '''
    
    def __init__(self, carol):
        self.carol = carol

    def load_model(self, model_filename, app_name, model_app_name):
        '''

        Load the trained model from a Carol app storage.
        
        Args:
            model_filename: name of the file that the model has been saved in storage.
                string
           app_name: name of the current Carol app.
                string
           model_app_name: name of the app in which the model has been saved.
            string

        Return:
            model: the trained Boston House Price model

        '''

        if model_app_name and model_app_name != app_name:
            self.carol.app_name = model_app_name
        storage =  Storage(self.carol)
        self.carol.app_name = app_name
        model = storage.load(model_filename, format='pickle', cache=False, chunk_size=None)
        return model

    def model_predict(self, model, crim, zn, indus, chas, nox, rm, age, dis, rad, tax, pratio, b, lstat):
        '''
        Use the loaded model and the features sent by the consumer to predict the price of a house.

        Args:

            model: the trained Boston House Price model
            crim, zn, indus, chas, nox, rm, age, dis, rad, tax, pratio, b, lstat

        Returns: 
        
            price: predicted price of a house
                float.

        '''
        if model is None:
            model = self.load_model()
        price = model.predict([[crim, zn, indus, chas, nox, rm, age, dis, rad, tax, pratio, b, lstat]])
        return round(float(price), 2)
