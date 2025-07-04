# fix_schema.py

from models import db
from app import app
from sqlalchemy import text

with app.app_context():
    db.session.execute(text('ALTER TABLE "user" ALTER COLUMN password_hash TYPE VARCHAR(512);'))
    db.session.commit()
    print("✅ password_hash column size updated to 512.")
