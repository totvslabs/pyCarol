import logging
import streamlit as st
import os
from pycarol import Carol, Apps, Storage, PwdAuth

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
# Reading the name of current enviroment sent to our code by the Carol platform.
env_name = os.environ.get('CAROLTENANT')
# Reading the name of current organization sent to our code by the Carol platform.
org_name = os.environ.get('CAROLORGANIZATION')

def load_model():
    '''
        
        Load the trained model from a Carol app storage.

        Return:
            model: the trained Boston House Price model

    '''
    global model_filename
    global model_app_name

    if model_app_name and model_app_name != app_name:
        carol.app_name = model_app_name
    storage =  Storage(carol)
    carol.app_name = app_name
    return storage.load(model_filename, format='pickle', cache=False, chunk_size=None)

def model_predict(crim, zn, indus, chas, nox, rm, age, dis, rad, tax, ptratio, b, lstat):
    '''
        Use the loaded model and the features sent by the consumer to predict the price of a house.

        Args:

            model: the trained Boston House Price model
            crim, zn, indus, chas, nox, rm, age, dis, rad, tax, ptratio, b, lstat

        Returns: 
        
            price: predicted price of a house
                float.
    '''
    model = st.session_state.get('model')
    price = model.predict([[crim, zn, indus, chas, nox, rm, age, dis, rad, tax, ptratio, b, lstat]])
    return round(float(price), 2)

# If the model has not been loaded yet we need to load it.
# The model is saved in the storage by the app in which this model has been trained.
if not st.session_state.get('model'):
    logger.info('Loading model.')
    model = load_model()
    # If the loading succedeed we save this information in session state so we do not need to reload it everytime
    # the user interacts with our app.
    st.session_state['model'] = model
    logger.info('Loading model: Done.')

# Add title on the page.
st.title("Boston House Prices")

def main():
    '''
    Main page.
    '''
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
    submit_button = form.form_submit_button(label='Predict house price')

    # When a user clicks on the submit button we predict and present in the screen the price of the house based on the information
    # that they have inputed in the interface.
    if submit_button:
        price = model_predict(crim, zn, indus, chas, nox, rm, age, dis, rad, tax, ptratio, b, lstat)
        st.write(f'Predicted selling price for home: ${price}')

def login():
    '''
    Login page.
    '''
    # Creates a form and all the necessary fields to be completed by user.
    login_placeholder = st.empty()
    login_form = login_placeholder.form(key='login_form')
    email = login_form.text_input('Email', value='') 
    pwd = login_form.text_input('Password:', value='', type="password")
    login_button = login_form.form_submit_button(label='Login')
    # When a user clicks on the submit button we predict and present in the screen the price of the house based on the information
    # that they have inputed in the interface.
    if login_button:
        # If any login field is empty we present an error message.
        if not email or not pwd:
            st.error('All fields are required.')
        # If the combination of email and password is valid to Carol we empty the login page and load the main page.
        # We save the authenticated state in the session state so it can be read through the interactions.
        elif validate_user(email, pwd):
            login_placeholder.empty()
            st.session_state['authenticated'] = True
            main()
        else:
            st.error('Incorrect login was used.')

def validate_user(user, pwd):
    '''
    Log in to Carol using username and password.
    
    Args:
        user: email to access Carol
            string.
        pwd: password to access Carol
            string.

    Returns:
        Whether the login using the combination of user and password succeeded or not.
            boolean
    '''
    aux_login = Carol(domain=env_name,
              app_name=app_name,
              organization=org_name,
              auth=PwdAuth(user, pwd))

    try:
        aux_login.get_current()
        return True
    except:
        return False

# Check in the session state if the user has already been authenticated.
authenticated = st.session_state.get('authenticated')

# If the user has not been authenticated we present the login screen. Otherwise, we present our main page.
if not authenticated:
    login()
else:
    main()