from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

db = SQLAlchemy()
migrate = Migrate()

def init_db(app):
    from models import User, Subscription  # Ensure tables are registered
    db.init_app(app)
    migrate.init_app(app, db)
