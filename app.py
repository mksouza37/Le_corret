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

        if df is not None and not df.empty:
            print('Trades selecionados com sucesso.')
        else:
            print('Nenhum trade encontrado.')

        for path in saved_files:
            os.remove(path)

        if not df.empty:
            with pd.ExcelWriter(OUTPUT_FILE) as writer:
                vista_cols = ['broker', 'date', 'market', 'direction', 'type', 'ticker', 'quantity', 'price', 'value', 'dc']
                bmf_cols = ['broker', 'date', 'market', 'direction', 'ticker', 'maturity', 'quantity', 'price', 'trade_type', 'value', 'dc']

                if 'A vista' in df['market'].values:
                    df[df['market'] == 'A vista'][vista_cols].to_excel(writer, sheet_name='A Vista', index=False)
                if 'BMF' in df['market'].values:
                    df[df['market'] == 'BMF'][bmf_cols].to_excel(writer, sheet_name='BMF', index=False)

            return redirect(url_for('download_file'))

        return render_template('index.html')  # show page again if no trades

    return render_template('index.html')

@app.route('/download')
def download_file():
    if os.path.exists(OUTPUT_FILE):
        return send_file(OUTPUT_FILE, as_attachment=True, download_name='Notas_Corretagem.xlsx')
    else:
        return "Arquivo não encontrado", 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
