import os
from flask import Flask
from .config import Config
from .extensions import mongo, csrf
from .auth.routes import auth_bp
from .tickets.routes import tickets_bp
from .admin.routes import admin_bp
from .analytics.routes import analytics_bp

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config())

    mongo.init_app(app)
    csrf.init_app(app)

    app.register_blueprint(auth_bp)
    app.register_blueprint(tickets_bp, url_prefix="/tickets")
    app.register_blueprint(admin_bp)
    app.register_blueprint(analytics_bp)

    @app.context_processor
    def inject_globals():
        return {"APP_NAME": "Ticketing APP"}

    return app
