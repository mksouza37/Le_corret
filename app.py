# app.py

from flask import Flask, request, send_file, render_template, redirect, url_for, make_response, flash
from flask_login import LoginManager, login_required, current_user
from models import db, User, Subscription
from auth import auth_bp
from admin import admin_bp
from trade_parser import TradeProcessor
from subscribe import subscribe_bp
from webhook import webhook_bp
import os
import pandas as pd
from datetime import datetime
import threading
import traceback

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "super-secret-key")

# Upload folder setup
UPLOAD_FOLDER = './tmp'
OUTPUT_FILE_BASE = os.path.join(UPLOAD_FOLDER, 'trades_output')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Background processing status store
processing_status = {}

# Database config
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'postgresql://localhost/mydb')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

# Register Blueprints
app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(subscribe_bp)
app.register_blueprint(webhook_bp)

# Flask-Login setup
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
    user_id = current_user.id

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

        # Mark as processing
        processing_status[user_id] = "processing"

        def background_task():
            try:
                df = TradeProcessor.process_directory(UPLOAD_FOLDER)

                if df is None:
                    raise ValueError("TradeProcessor returned None")

                if df.empty:
                    raise ValueError("TradeProcessor returned empty DataFrame")

                cpf_value = df['CPF'].iloc[0].replace('.', '').replace('-', '')
                output_filename = f"{OUTPUT_FILE_BASE} - {cpf_value}.xlsx"
                app.config['GENERATED_FILE'] = output_filename
                processing_status[user_id] = "ready"

            except Exception as e:
                print(f"❌ Background processing error:\n{traceback.format_exc()}")
                processing_status[user_id] = "error"

        threading.Thread(target=background_task).start()
        return render_template('index.html', uploaded_files=[], processing=True)


    return render_template('index.html', uploaded_files=[], processing=False)

# === Polling route to check processing status ===
@app.route('/check_status')
@login_required
def check_status():
    user_id = current_user.id
    status = processing_status.get(user_id, "idle")
    return {"status": status}

# === Download route ===
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
                processing_status[current_user.id] = "idle"
            except Exception as e:
                app.logger.error(f"Erro ao limpar arquivos: {e}")
    else:
        return "Arquivo não encontrado", 404

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
