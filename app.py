from flask import Flask, request, send_file, render_template, redirect, url_for, make_response
import os
import pandas as pd
from dotenv import load_dotenv
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

from trade_parser import TradeProcessor
from models import User, Subscription  # Register models so Flask-Migrate can see them

# Load env variables from .env if running locally
load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv("SECRET_KEY")
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("DATABASE_URL")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize DB and migration
db = SQLAlchemy()
migrate = Migrate()
db.init_app(app)
migrate.init_app(app, db)

UPLOAD_FOLDER = './tmp'
OUTPUT_FILE_BASE = os.path.join(UPLOAD_FOLDER, 'trades_output')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route('/', methods=['GET', 'POST'])
def upload_files():
    if request.method == 'POST':
        uploaded_files = request.files.getlist('files')
        saved_files = []

        for file in uploaded_files:
            if file.filename.lower().endswith('.pdf'):
                file_path = os.path.join(UPLOAD_FOLDER, file.filename)
                file.save(file_path)
                saved_files.append(file_path)

        df = TradeProcessor.process_directory(UPLOAD_FOLDER)

        if df is not None and not df.empty:
            cpf_value = df['client_cpf'].iloc[0].replace('.', '').replace('-', '')
            output_filename = f"{OUTPUT_FILE_BASE} - {cpf_value}.xlsx"

            with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
                workbook = writer.book

                vista_cols = [
                    'broker', 'invoice', 'date', 'market',
                    'direction', 'type', 'ticker', 'quantity', 'price', 'value', 'dc'
                ]
                bmf_cols = [
                    'broker', 'invoice', 'date', 'market',
                    'direction', 'ticker', 'maturity', 'quantity', 'price', 'trade_type', 'value', 'dc'
                ]

                if 'A vista' in df['market'].values:
                    vista_df = df[df['market'] == 'A vista']
                    if not vista_df.empty:
                        sheet_name = 'A Vista'
                        client_cpf = vista_df['client_cpf'].iloc[0]
                        client_line = f"CPF: {client_cpf}"
                        common_vista_cols = [col for col in vista_cols if col in vista_df.columns]
                        worksheet = workbook.add_worksheet(sheet_name)
                        worksheet.write_string(0, 0, client_line)
                        vista_df[common_vista_cols].to_excel(writer, sheet_name=sheet_name, startrow=2, index=False)

                if 'BMF' in df['market'].values:
                    bmf_df = df[df['market'] == 'BMF']
                    if not bmf_df.empty:
                        sheet_name = 'BMF'
                        client_cpf = bmf_df['client_cpf'].iloc[0]
                        client_line = f"CPF: {client_cpf}"
                        common_bmf_cols = [col for col in bmf_cols if col in bmf_df.columns]
                        worksheet = workbook.add_worksheet(sheet_name)
                        worksheet.write_string(0, 0, client_line)
                        bmf_df[common_bmf_cols].to_excel(writer, sheet_name=sheet_name, startrow=2, index=False)

            app.config['GENERATED_FILE'] = output_filename
            return redirect(url_for('download_file'))

        return render_template('index.html', uploaded_files=[])

    # GET method: show page with no files
    return render_template('index.html', uploaded_files=[])


@app.route('/download')
def download_file():
    output_filename = app.config.get('GENERATED_FILE', OUTPUT_FILE_BASE + '.xlsx')
    if os.path.exists(output_filename):
        try:
            response = make_response(send_file(output_filename, as_attachment=True, download_name=os.path.basename(output_filename)))
            return response
        finally:
            # Cleanup after download
            try:
                os.remove(output_filename)
                for f in os.listdir(UPLOAD_FOLDER):
                    os.remove(os.path.join(UPLOAD_FOLDER, f))
            except Exception as e:
                app.logger.error(f"Error cleaning files: {e}")
    else:
        return "File not found", 404

if __name__ == '__main__':
    with app.app_context():
        from flask_migrate import upgrade
        upgrade()

    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)

