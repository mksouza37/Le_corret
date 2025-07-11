# app.py

from flask import Flask, request, send_file, render_template, redirect, url_for, make_response, flash
from flask_login import LoginManager, login_required, current_user
from models import db, User, Subscription
from auth import auth_bp
from admin import admin_bp
import os
import pandas as pd
from trade_parser import TradeProcessor
from datetime import datetime
from subscribe import subscribe_bp
from webhook import webhook_bp


app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "super-secret-key")

# Upload folder setup
UPLOAD_FOLDER = './tmp'
OUTPUT_FILE_BASE = os.path.join(UPLOAD_FOLDER, 'trades_output')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Database config
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'postgresql://localhost/mydb')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

# Auth Blueprint
app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(subscribe_bp)
app.register_blueprint(webhook_bp)

# Flask-Login config
login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.init_app(app)

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# === Protected Upload Route ===
@app.route('/', methods=['GET', 'POST'])
@login_required
def upload_files():
    # 👇 Check subscription only here
    if current_user.email.strip().lower() != "markusn37@gmail.com":
        subscription = current_user.subscriptions[0] if current_user.subscriptions else None
        if not subscription or not subscription.is_active():
            flash("Assinatura inativa ou expirada. Renove para continuar.", "error")
            return redirect(url_for('subscribe.create_checkout_session'))

    if request.method == 'POST':
        uploaded_files = request.files.getlist('files')
        saved_files = []

        for file in uploaded_files:
            if file.filename.lower().endswith('.pdf'):
                file_path = os.path.join(UPLOAD_FOLDER, file.filename)
                file.save(file_path)
                saved_files.append(file_path)

        df = TradeProcessor.process_directory(UPLOAD_FOLDER)

        # Look for Excel file saved by the parser
        generated_files = [f for f in os.listdir() if f.startswith("output_all_invoices") and f.endswith(".xlsx")]
        if generated_files:
            latest_file = max(generated_files, key=os.path.getctime)
            app.config['GENERATED_FILE'] = latest_file
            return redirect(url_for('download_file'))

        return render_template('index.html', uploaded_files=[])


@app.route('/download')
@login_required
def download_file():
    output_filename = app.config.get('GENERATED_FILE', OUTPUT_FILE_BASE + '.xlsx')
    if os.path.exists(output_filename):
        try:
            response = make_response(send_file(output_filename, as_attachment=True, download_name=os.path.basename(output_filename)))
            return response
        finally:
            try:
                os.remove(output_filename)
                for f in os.listdir(UPLOAD_FOLDER):
                    os.remove(os.path.join(UPLOAD_FOLDER, f))
            except Exception as e:
                app.logger.error(f"Erro ao limpar arquivos: {e}")
    else:
        return "Arquivo não encontrado", 404

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
