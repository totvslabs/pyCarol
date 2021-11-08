import faulthandler
faulthandler.enable()

import logging
import pandas as pd
import os
from flask import Blueprint, jsonify, request
from pycarol import Carol, Storage, Apps
from webargs import fields, ValidationError
from webargs.flaskparser import parser
import re
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
_settings = Apps(carol).get_settings()
# Name of the model file has been saved in Carol App Storage
model_filename = _settings.get('model_filename')
# App's name in which the model has been saved
model_app_name = _settings.get('model_app_name')
# Reading the name of current name sent to our code by the Carol platform.
app_name = os.environ.get('CAROLAPPNAME')

functions = Functions(carol)

# Loads the model saved in the storage by the app in which this model has been trained.
logger.info('Loading model.')
model = functions.load_model(model_filename, app_name, model_app_name)
logger.info('Loading model: Done.')

logger.debug('App started.')


def requires_auth(f):
    '''
    Determine if the access token is valid.
    '''
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            resp = jsonify({"message": "Please authenticate."})
            resp.status_code = 401
            resp.headers["WWW-Authenticate"] = 'Basic realm="Example"'
            return resp
        kwargs["user"] = User.get(User.email == auth.username)
        return f(*args, **kwargs)

    return decorated


# This is a required route. Carol uses it for heartbeat. Usually we just add a message as a response.
@server_bp.route('/', methods=['GET'])
def ping():
    return jsonify('''App is running. Send a POST request to /house for sending a new house to staging or 
        a GET request to /predict for getting the estimated price for a house based on some features.''')

# Route to reload the model in case any changes is made on it.
# Using a route we can update the model without having to restart the app.
@server_bp.route('/load_model', methods=['GET'])
@requires_auth
def load_model():
    global model

    # Load model
    logger.info('Loading model.')
    model = functions.load_model(model_filename, app_name, model_app_name)
    logger.info('Loading model: Done.')

    return jsonify('Model loaded.')

# Route for getting the price of a house predicted by the ML model based on the provided features.
@server_bp.route('/predict', methods=['POST'])
@requires_auth
def validate():

    query_arg = {
        "ZN": fields.Str(required=True, 
            description='Proportion of residential land zoned for lots over 25,000 sq.ft.'),
        "INDUS": fields.Str(required=True, 
            description='Proportion of non-retail business acres per town.'),
        "CHAS": fields.Str(required=True, 
            description='Charles River dummy variable (= 1 if tract bounds river; 0 otherwise).'),
        "NOX": fields.Str(required=True, 
            description='Nitric oxides concentration (parts per 10 million)..'),
        "RM": fields.Str(required=True, 
            description='Average number of rooms per dwelling.'),
        "AGE": fields.Str(required=True, 
            description='Proportion of owner-occupied units built prior to 1940.'),
        "DIS": fields.Str(required=True, 
            description='Weighted distances to ﬁve Boston employment centers.'),
        "RAD": fields.Str(required=True, 
            description='Full-value property-tax rate per $10,000.'),
        "TAX": fields.Str(required=True, 
            description='Proportion of residential land zoned for lots over 25,000 sq.ft.'),
        "PTRATIO": fields.Str(required=True, 
            description='Pupil-teacher ratio by town 12. B: 1000(Bk−0.63)2 where Bk is the proportion of blacks by town 13. LSTAT: % lower status of the population.'),
    }

    args = parser.parse(query_arg, request)
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
    pratio = args['pratio']
    b = args['b']
    lstat = args['lstat']

    global model
    price = functions.model_predict(model, crim, zn, indus, chas, nox, rm, age, dis, rad, tax, pratio, b, lstat)

    return jsonify({'price': price})


@server_bp.errorhandler(422)
@server_bp.errorhandler(400)
def handle_error(err):
    headers = err.data.get("headers", None)
    messages = err.data.get("messages", ["Invalid request."])
    messages = messages.get('json', messages)
    if headers:
        return jsonify(messages), err.code, headers
    else:
        return jsonify(messages), err.code


def validate_filter(val):
    '''
    Check whether the value is valid or not.
    '''
    logger.debug('Validating filter')
    filter_columns = []
    filters = list(val)
    for filter in filters:
        filter_field = filter.get('filter_field')
        if filter_field:
            filter_columns.append(filter_field)
        else:
            raise ValidationError("The key 'filter_field' must be filled when you are using filters.") 
    if filters and any(c not in df.columns for c in filter_columns):
        raise ValidationError("One or more columns that you are trying to filter does not exist in the documents base.")