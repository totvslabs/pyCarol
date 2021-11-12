from flask import Flask

__version__ = '1.0.0'

def create_app():
    UPLOAD_FOLDER = '/app/files'

    app = Flask(__name__)
    app.secret_key = "secret key"

    register_blueprints(app)

    return app


def register_blueprints(app):
    from .routes import server_bp

    app.register_blueprint(server_bp)
