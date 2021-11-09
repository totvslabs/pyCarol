import faulthandler
faulthandler.enable()

import logging
import pandas as pd
import os
from flask import Blueprint, jsonify, request
from pycarol import Carol, Apps, ApiKeyAuth, PwdKeyAuth, PwdAuth
import json
from webargs import fields, ValidationError
from webargs.flaskparser import parser
from functools import wraps
from .functions import Functions

server_bp = Blueprint('main', __name__)

# Every run of an app in Carol happens in a task.
# The logger object logs information in the logs section in the task.
logger = logging.getLogger(__name__)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(levelname)s: %(message)s")
console.setFormatter(formatter)
logger.addHandler(console)

# Authenticating on Carol with default parameters from the environment
carol = Carol()
# Reading the settings defined in the Carol app.
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

# We instantiate the class Functions in which we stored all the functions that we want to use in our api.
functions = Functions(carol)

# Loads the model saved in the storage from the app in which this model has been trained.
logger.info('Loading model.')
model = functions.load_model(model_filename, app_name, model_app_name)
logger.info('Loading model: Done.')

logger.debug('App started.')

def error_authentication_message(message=None):
    '''
    Create an error message with 401 status code.

    Returns:
        resp: Response with error message and status code 401.
            json
    '''
    if not message:
        message = 'Authorization, or X-Auth-Key and X-Auth-ConnectorId is invalid. Please authenticate.'
    resp = jsonify({"message": message})
    resp.status_code = 401
    return resp

def validate_authorization_token(authorization_token):
    '''
    Log in to Carol using a Authorization token (web token).

    Returns:
        Whether the login using the token succeded or not.
            boolean
    '''

    try:
        Carol(domain=env_name,
              app_name=app_name,
              organization=org_name,
              auth=PwdKeyAuth(authorization_token))
        return True
    except:
        return False

def validate_api_key(api_key, connector_id):
    '''
    Log in to Carol using an api key (connector token).
    
    Args:
        api_key: key to access Carol's apis (connector token)
            string.
        connector_id: connector id attached to the api key
            string.

    Returns:
        Whether the login using the combination of api_key and connector_id succeeded or not.
            boolean
    '''
    aux_login = Carol(domain=env_name,
              app_name=app_name,
              organization=org_name,
              auth=ApiKeyAuth(api_key),
              connector_id=connector_id)

    try:
        aux_login.get_current()
        return True
    except:
        return False


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


# We want to validate if the consumer is authorize to access some of our endpoints. Because of that we created this decorator.
# It validates if the information sent in the authorization header is valid in Carol.
# If you want your endpoint to be validated you just need to add @requires_auth before the method's name of that endpoint.
def requires_auth(f):
    '''
    Determine if the access token is valid in Carol.
    It returns an error message with 401 status code in case the token is not valid.
    '''
    @wraps(f)
    def decorated(*args, **kwargs):
        headers = request.headers
        auth = request.authorization
        if not headers and not auth:
            return error_authentication_message()
        elif not headers.get('Authorization') and not headers.get('X-Auth-Key') and not headers.get('X-Auth-ConnectorId') \
            and not auth:
            return error_authentication_message()
        elif headers.get('Authorization') and 'Basic' not in headers.get('Authorization') and not validate_authorization_token(headers.get('Authorization')):
            return error_authentication_message()
        elif (headers.get('X-Auth-Key') and not headers.get('X-Auth-ConnectorId')) or (not headers.get('X-Auth-Key') and headers.get('X-Auth-ConnectorId')):
            return error_authentication_message()
        elif headers.get('X-Auth-Key') and headers.get('X-Auth-ConnectorId') and not validate_api_key(headers.get('X-Auth-Key'), headers.get('X-Auth-ConnectorId')):
            return error_authentication_message()
        elif auth and not validate_user(auth.username, auth.password):
            return error_authentication_message()


        return f(*args, **kwargs)

    return decorated


# This is a required route. Carol uses it for heartbeat. Usually we just add a message as a response.
@server_bp.route('/', methods=['GET'])
def ping():
    return jsonify('''App is running. Send a GET request to /load_model for reloading the model after it has been
        retrained or a POST request to /house_price for getting the estimated price for a house based on some features.''')

# Route to reload the model in case any changes is made on it.
# Using a route we can update the model without having to restart the app.
@server_bp.route('/load_model', methods=['GET'])
@requires_auth
def load_model():
    global model

    # We are loading the model from the app Storage
    logger.info('Loading model.')
    model = functions.load_model(model_filename, app_name, model_app_name)
    logger.info('Loading model: Done.')

    return jsonify('Model loaded.')

# Route for getting the price of a house predicted by the ML model based on the provided features.
@server_bp.route('/house_price', methods=['POST'])
@requires_auth
def house_price():

    # Here we defined the contract of our api. If the consumer sends something in their request that is not defined in this contract
    # they will get an error with status code 422.
    # We are using the marshmallow package for that. To see other type of fields please refer to https://marshmallow.readthedocs.io/en/stable/marshmallow.fields.html#marshmallow.fields.Field
    # The validate parameter is set with the name of the method in which we validate the content of that specific field.
    # The missing parameter sets a default value in case the specific input is not sent by the consumer.
    query_arg = {
        "crim": fields.Float(required=True, validate=validate_numbers,
            description='Per capita crime rate by town.'),
        "zn": fields.Float(required=True, validate=validate_numbers,
            description='Proportion of residential land zoned for lots over 25,000 sq.ft.'),
        "indus": fields.Float(required=True, validate=validate_numbers, 
            description='Proportion of non-retail business acres per town.'),
        "chas": fields.Integer(required=False, missing=0, validate=validate_numbers, 
            description='Charles River dummy variable (= 1 if tract bounds river; 0 otherwise).'),
        "nox": fields.Float(required=True, validate=validate_numbers, 
            description='Nitric oxides concentration (parts per 10 million)..'),
        "rm": fields.Float(required=True, validate=validate_numbers, 
            description='Average number of rooms per dwelling.'),
        "age": fields.Float(required=True, validate=validate_numbers, 
            description='Proportion of owner-occupied units built prior to 1940.'),
        "dis": fields.Float(required=True, validate=validate_numbers, 
            description='Weighted distances to ﬁve Boston employment centers.'),
        "rad": fields.Integer(required=True, validate=validate_numbers, 
            description='Full-value property-tax rate per $10,000.'),
        "tax": fields.Float(required=True, validate=validate_numbers, 
            description='Proportion of residential land zoned for lots over 25,000 sq.ft.'),
        "ptratio": fields.Float(required=True, validate=validate_numbers, 
            description='Pupil-teacher ratio by town 12. B: 1000(Bk−0.63)2 where Bk is the proportion of blacks by town 13. LSTAT: % lower status of the population.'),
        "b": fields.Float(required=True, validate=validate_numbers, 
            description='1000(Bk - 0.63)^2 where Bk is the proportion of blacks by town.'),
        "lstat": fields.Float(required=True, validate=validate_numbers, 
            description=r'% lower status of the population.'),
    }

    # When parsing the request we validate its inputs and the values sent by the api consumer is stored in a dictionary, which here we call args.
    args = parser.parse(query_arg, request)
    # We read each of the inputs into their respective variable
    crim = args['crim']
    zn = args['zn']
    indus = args['indus']
    chas = args['chas']
    nox = args['nox']
    rm = args['rm']
    age = args['age']
    dis = args['dis']
    rad = args['rad']
    tax = args['tax']
    ptratio = args['ptratio']
    b = args['b']
    lstat = args['lstat']

    # We finally predict the price of the house using the model_predict method that we created in the functions class.
    global model
    price = functions.model_predict(model, crim, zn, indus, chas, nox, rm, age, dis, rad, tax, ptratio, b, lstat)

    # Once we got the house price we sent it back to the consumer using a json structure.
    return jsonify({'price': price})


@server_bp.errorhandler(422)
@server_bp.errorhandler(400)
def handle_error(err):
    '''
    We check whether the request and its inputs were sent as we defined in the contract of each endpoint.
    In case is not valid we sent back an error message with status code 422.

    Args:
        err: error instance.

    Returns:
        error message
            json
        status code
            int
        headers
            json
    '''
    headers = err.data.get("headers", None)
    messages = err.data.get("messages", ["Invalid request."])
    messages = messages.get('json', messages)
    if headers:
        return jsonify(messages), err.code, headers
    else:
        return jsonify(messages), err.code, headers


def validate_numbers(val):
    '''
    Check whether the value sent by the consumer is negative or not.
    In case it is negative we sent back an error message with status code 422.

    Args:
        val: endpoint input
            any.
    '''
    if isinstance(val, float):
        if val < 0.0:
            raise ValidationError("All float parameters must be greater than 0.0.") 
    elif isinstance(val, int):
        if val < 0:
            raise ValidationError("All integer parameters must be greater than 0.") 