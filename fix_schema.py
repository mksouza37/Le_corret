from models import db
from app import app

with app.app_context():
    db.session.execute('ALTER TABLE "user" ALTER COLUMN password_hash TYPE VARCHAR(512);')
    db.session.commit()
    print("✅ Password hash column updated.")
