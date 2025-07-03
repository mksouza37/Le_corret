import os
import re
import pdfplumber
import pandas as pd
from typing import List, Dict, Optional

class BrokerParser:
    BROKER_NAME = "GENERIC"

    @classmethod
    def match_broker(cls, text: str) -> bool:
        return cls.BROKER_NAME.upper() in text.upper()

    @classmethod
    def extract_date(cls, text: str) -> Optional[str]:
        raise NotImplementedError

    @classmethod
    def extract_invoice_number(cls, text: str) -> Optional[str]:
        match = re.search(r'Nr.?\s*nota\s*(\d+)', text, re.IGNORECASE)
        return match.group(1) if match else None

    @classmethod
    def extract_client_info(cls, text: str) -> Dict[str, str]:
        name_match = re.search(r'(?i)(MARKUS SOUZA DO NASCIMENTO)', text)
        cpf_match = re.search(r'(\d{3}\.\d{3}\.\d{3}-\d{2}|\d{11})', text)
        name = name_match.group(1) if name_match else ""
        cpf = cpf_match.group(1) if cpf_match else ""
        return {"client_name": name, "client_cpf": cpf}

    @classmethod
    def extract_trades(cls, text: str) -> List[Dict]:
        raise NotImplementedError

    @classmethod
    def clean_numeric(cls, value: str) -> float:
        if not value:
            return 0.0
        return float(value.replace('.', '').replace(',', '.'))

# Parsers for each broker (similar to original code, omitted here for brevity)

class TradeProcessor:
    PARSERS = [ItauParser, AgoraParser, XPParser, BTGParser]

    @classmethod
    def process_pdf(cls, file_path: str) -> Dict[str, any]:
        all_trades = []
        client_info = {"client_name": "", "client_cpf": ""}

        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if not page_text:
                    continue
                for parser in cls.PARSERS:
                    if parser.match_broker(page_text):
                        date = parser.extract_date(page_text)
                        invoice = parser.extract_invoice_number(page_text)
                        info = parser.extract_client_info(page_text)
                        client_info = info if info['client_name'] else client_info
                        trades = parser.extract_trades(page_text)
                        for trade in trades:
                            trade['broker'] = parser.BROKER_NAME
                            trade['date'] = date
                            trade['invoice'] = invoice
                        all_trades.extend(trades)
                        break

        return {"trades": all_trades, "client_info": client_info}

    @classmethod
    def process_directory(cls, directory: str) -> None:
        all_trades = []
        client_name = ""
        client_cpf = ""

        for filename in os.listdir(directory):
            if filename.lower().endswith('.pdf'):
                filepath = os.path.join(directory, filename)
                print(f"Processing {filename}...")
                result = cls.process_pdf(filepath)
                if not client_name and result['client_info']['client_name']:
                    client_name = result['client_info']['client_name']
                    client_cpf = result['client_info']['client_cpf']
                all_trades.extend(result['trades'])

        if not all_trades:
            print("No trades found in PDF files")
            return

        df = pd.DataFrame(all_trades)
        desired_order = ['broker', 'date', 'invoice', 'market'] + [col for col in df.columns if col not in ['broker', 'date', 'invoice', 'market']]
        df = df[desired_order]
        df.sort_values(by=['broker', 'date'], inplace=True)

        output_file = "consolidated_trades.xlsx"
        with pd.ExcelWriter(output_file) as writer:
            vista_cols = ['broker', 'date', 'invoice', 'market', 'direction', 'type', 'ticker', 'quantity', 'price', 'value', 'dc']
            bmf_cols = ['broker', 'date', 'invoice', 'market', 'direction', 'ticker', 'maturity', 'quantity', 'price', 'trade_type', 'value', 'dc']

            if client_name or client_cpf:
                client_info_df = pd.DataFrame([[client_name, client_cpf]], columns=['Client Name', 'CPF'])

            vista_df = df[df['market'] == 'A vista']
            bmf_df = df[df['market'] == 'BMF']

            if not vista_df.empty:
                client_info_df.to_excel(writer, sheet_name='A Vista', index=False, startrow=0)
                vista_df[vista_cols].to_excel(writer, sheet_name='A Vista', index=False, startrow=2)

            if not bmf_df.empty:
                client_info_df.to_excel(writer, sheet_name='BMF', index=False, startrow=0)
                bmf_df[bmf_cols].to_excel(writer, sheet_name='BMF', index=False, startrow=2)

        print(f"Saved {len(df)} trades to {output_file}")

if __name__ == "__main__":
    TradeProcessor.process_directory("./")
