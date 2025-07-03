from flask import Flask, request, send_file, render_template, redirect, url_for
import os
import pandas as pd
from trade_parser import TradeProcessor

app = Flask(__name__)
UPLOAD_FOLDER = './tmp'
OUTPUT_FILE = os.path.join(UPLOAD_FOLDER, 'trades_output.xlsx')
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

        for path in saved_files:
            os.remove(path)

        if df is not None and not df.empty:
            with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter') as writer:
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

            return redirect(url_for('download_file'))

        return render_template('index.html')

    return render_template('index.html')

@app.route('/download')
def download_file():
    if os.path.exists(OUTPUT_FILE):
        return send_file(OUTPUT_FILE, as_attachment=True, download_name='Notas_Corretagem.xlsx')
    else:
        return "Arquivo não encontrado", 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
