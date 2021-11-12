# -*- coding: utf-8 -*-
from app import create_app
from flask_cors import CORS

application = create_app()
CORS(application)

if __name__ == "__main__":
    application.run(host='0.0.0.0', port=5000)