import logging
import streamlit as st
import os
from pycarol import Carol, Apps, Storage

# Logger
logger = logging.getLogger(__name__)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(levelname)s: %(message)s")
console.setFormatter(formatter)
logger.addHandler(console)

# Logging in to Carol
carol = Carol()
_settings = Apps(carol).get_settings()
# Name of the model file has been saved in Carol App Storage
model_filename = _settings.get('model_filename')
# App's name in which the model has been saved
model_app_name = _settings.get('model_app_name')
# Reading the name of current name sent to our code by the Carol platform.
app_name = os.environ.get('CAROLAPPNAME')

def load_model():
    '''
        
        Load the trained model from a Carol app storage.

        Return:
            model: the trained Boston House Price model

    '''
    global model
    global model_filename
    global model_app_name

    if model_app_name and model_app_name != app_name:
        carol.app_name = model_app_name
    storage =  Storage(carol)
    carol.app_name = app_name
    model = storage.load(model_filename, format='pickle', cache=False, chunk_size=None)
    return model

def model_predict(crim, zn, indus, chas, nox, rm, age, dis, rad, tax, pratio, b, lstat):
    '''
        Use the loaded model and the features sent by the consumer to predict the price of a house.

        Args:

            model: the trained Boston House Price model
            crim, zn, indus, chas, nox, rm, age, dis, rad, tax, pratio, b, lstat

        Returns: 
        
            price: predicted price of a house
                float.
    '''
    global model
    if model is None:
        model = load_model()
    price = model.predict([[crim, zn, indus, chas, nox, rm, age, dis, rad, tax, ptratio, b, lstat]])
    return round(float(price), 2)

# Loads the model saved in the storage by the app in which this model has been trained.
logger.info('Loading model.')
model = load_model()
logger.info('Loading model: Done.')

# Add title on the page.
st.title("Boston House Prices")

# Add a text in our page.
st.write("Here, we can help you to find at what price it's recommended that you sell home.")

# Creates a form and all the necessary fields to be completed by user.
form = st.form(key='my_form')
crim = form.number_input('Per capita crime rate by town', format='%f', min_value=0) 
zn = form.number_input('Proportion of residential land zoned for lots over 25,000 sq.ft.', format='%f', min_value=0.0)
indus = form.number_input('Proportion of non-retail business acres per town.', format='%f', min_value=0.0)
chas = form.number_input('Charles River dummy variable', format='%d', min_value=0, max_value=1, help='1 if tract bounds river; 0 otherwise') 
nox = form.number_input('Nitric oxides concentration (parts per 10 million)', format='%f', min_value=0.0)
rm = form.number_input('Average number of rooms per dwelling.', format='%f', min_value=0.0)
age = form.number_input('Proportion of owner-occupied units built prior to 1940.', format='%f', min_value=0.0) 
dis = form.number_input('Weighted distances to five Boston employment centres.', format='%f', min_value=0.0)
rad = form.number_input('Index of accessibility to radial highways.', format='%d', min_value=1)
tax = form.number_input('Full-value property-tax rate per $10,000', format='%f', min_value=0.0) 
ptratio = form.number_input('Pupil-teacher ratio by town', format='%f', min_value=0.0, help='Eg: If ratio is 15 students to 1 teacher then insert 15.')
b = form.number_input('1000(Bk - 0.63)^2 where Bk is the proportion of blacks by town.', format='%f', min_value=0.0)
lstat = form.number_input(r'% lower status of the population', format='%f', min_value=0.0) 
submit_button = form.form_submit_button(label='Search')

# When a user clicks on the submit button we predict and present in the screen the price of the house based on the information
# that they have inputed in the interface.
if submit_button:
    price = model_predict(crim, zn, indus, chas, nox, rm, age, dis, rad, tax, ptratio, b, lstat)
    st.write(f'Predicted selling price for home: ${price}')