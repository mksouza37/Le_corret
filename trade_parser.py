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

    '''
    @classmethod
    def extract_invoice_number(cls, text: str) -> Optional[str]:
        match = re.search(r'Nr\.?\s*nota\s*(\d+)', text, re.IGNORECASE)
        if not match:
            match = re.search(r'nota\s*de\s*corretagem\s*(\d+)', text, re.IGNORECASE)
        return match.group(1) if match else None
    '''

    @classmethod
    def extract_invoice_number(cls, text: str) -> Optional[str]:
        # Try patterns in order of specificity
        patterns = [
            r'Nr\.?\s*nota\s*[:\|]?\s*(\d+)\s*[\/\|]',  # For cases with separators
            r'Nr\.?\s*nota\s*(\d+)\s+Folha',  # Followed by "Folha"
            r'Nr\.?\s*nota\s*(\d+)',  # Basic pattern
            r'nota\s*de\s*corretagem\s*(\d+)',  # Alternative format
            r'Nr\.Nota\s*(\d+)',  # XP format
            r'N\.\s*atual\s*(\d+)'  # Another XP format variation
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)

        return None

    @classmethod
    def extract_client_info(cls, text: str) -> Dict[str, str]:
        cpf_match = re.search(r'(\d{3}\.\d{3}\.\d{3}-\d{2}|\d{11})', text)
        name_match = re.search(r'Cliente\s*\n\s*(.*?)\n', text, re.IGNORECASE)
        cpf = cpf_match.group(1) if cpf_match else ""
        name = name_match.group(1).strip().title() if name_match else ""
        return {"client_name": name, "client_cpf": cpf}

    @classmethod
    def extract_trades(cls, text: str) -> List[Dict]:
        raise NotImplementedError

    @classmethod
    def clean_numeric(cls, value: str) -> float:
        if not value:
            return 0.0
        return float(value.replace('.', '').replace(',', '.'))

class ItauParser(BrokerParser):
    BROKER_NAME = "ITAÚCORRETORA"

    extract_invoice_number = BrokerParser.extract_invoice_number
    extract_client_info = BrokerParser.extract_client_info

    @classmethod
    def extract_date(cls, text: str) -> Optional[str]:
        match = re.search(r'Data Pregão\s*\n\s*(\d{2}/\d{2}/\d{4})', text, re.IGNORECASE)
        return match.group(1) if match else None

    @classmethod
    def extract_trades(cls, text: str) -> List[Dict]:
        trades = []
        section = cls._find_trade_section(text)
        for line in section.split('\n'):
            parts = line.split()
            if len(parts) >= 8 and parts[0] == 'BOVESPA':
                try:
                    idx = 3
                    ticker_parts = []
                    while idx < len(parts) and not parts[idx].replace('.', '').replace(',', '').isdigit():
                        ticker_parts.append(parts[idx])
                        idx += 1
                    trades.append({
                        'market': 'A vista',
                        'direction': parts[1],
                        'type': parts[2],
                        'ticker': ' '.join(ticker_parts),
                        'quantity': int(parts[idx]),
                        'price': cls.clean_numeric(parts[idx+1]),
                        'value': cls.clean_numeric(parts[idx+2]),
                        'dc': parts[idx+3] if idx+3 < len(parts) else ""
                    })
                except:
                    continue
        return trades

    @classmethod
    def _find_trade_section(cls, text: str) -> str:
        start = text.find("Negócios Realizados")
        if start == -1:
            start = text.find("Q Negociação C/V Tipo Mercado")
        end = text.find("Resumo de negócios", start)
        return text[start:end] if start != -1 and end != -1 else ""

class AgoraParser(BrokerParser):
    BROKER_NAME = "AGORA"

    extract_invoice_number = BrokerParser.extract_invoice_number
    extract_client_info = BrokerParser.extract_client_info

    @classmethod
    def extract_date(cls, text: str) -> Optional[str]:
        match = re.search(r'Data\s+pregão\s*\n\s*(\d{2}/\d{2}/\d{4})', text, re.IGNORECASE)
        return match.group(1) if match else None

    @classmethod
    def extract_trades(cls, text: str) -> List[Dict]:
        trades = []
        section = text[text.find("Negocios Realizados"):text.find("Resumo dos Negócios")]
        pattern = re.compile(r'^B3\s+RV\s+LISTADO\s+([CV])\s+(FRACIONARIO|VISTA)\s+(.+?)\s+(\d+)\s+([\d.,]+)\s+([\d.,]+)\s+([DC])$', re.MULTILINE)
        for line in section.split('\n'):
            line = ' '.join(line.strip().split())
            match = pattern.match(line)
            if match:
                trades.append({
                    'market': 'A vista',
                    'direction': match.group(1),
                    'type': match.group(2),
                    'ticker': match.group(3),
                    'quantity': int(match.group(4)),
                    'price': cls.clean_numeric(match.group(5)),
                    'value': cls.clean_numeric(match.group(6)),
                    'dc': match.group(7)
                })
        return trades

class XPParser(BrokerParser):
    BROKER_NAME = "XP INVESTIMENTOS"

    extract_invoice_number = BrokerParser.extract_invoice_number
    extract_client_info = BrokerParser.extract_client_info

    @classmethod
    def extract_date(cls, text: str) -> Optional[str]:
        match = re.search(r'Data pregão\s*\n\s*(\d{2}/\d{2}/\d{4})', text, re.IGNORECASE)
        return match.group(1) if match else None

    @classmethod
    def extract_trades(cls, text: str) -> List[Dict]:
        trades = []
        section = text[text.find("Negócios realizados"):text.find("Resumo dos Negócios")]
        pattern = re.compile(r'^\d+-BOVESPA\s+([CV])\s+(VISTA|FRACIONARIO)\s+(.+?)\s+(\d+)\s+([\d.,]+)\s+([\d.,]+)\s+([DC])$', re.MULTILINE)
        for line in section.split('\n'):
            line = ' '.join(line.strip().split())
            match = pattern.match(line)
            if match:
                trades.append({
                    'market': 'A vista',
                    'direction': match.group(1),
                    'type': match.group(2),
                    'ticker': match.group(3),
                    'quantity': int(match.group(4)),
                    'price': cls.clean_numeric(match.group(5)),
                    'value': cls.clean_numeric(match.group(6)),
                    'dc': match.group(7)
                })
        return trades

class BTGParser(BrokerParser):
    BROKER_NAME = "BTG"

    extract_invoice_number = BrokerParser.extract_invoice_number
    extract_client_info = BrokerParser.extract_client_info

    @classmethod
    def extract_date(cls, text: str) -> Optional[str]:
        match = re.search(r'Data\s+pregão\s*\n\s*(\d{2}/\d{2}/\d{4})', text, re.IGNORECASE)
        return match.group(1) if match else None

    @classmethod
    def extract_trades(cls, text: str) -> List[Dict]:
        trades = []
        if "C/V Mercadoria Vencimento" in text:
            section = text[text.find("C/V Mercadoria Vencimento"):text.find("+Custos", text.find("C/V Mercadoria Vencimento"))]
            pattern = re.compile(r'^([CV])\s+(\S+)\s+(\d{2}/\d{2}/\d{4})\s+(\d+)\s+([\d.,]+)\s+(\S+)\s+([\d.,]+)\s+([DC])$', re.MULTILINE)
            market_label = 'BMF'
        else:
            section = text[text.find("Negócios realizados"):text.find("Resumo dos Negócios")]
            pattern = re.compile(r'^\d+-BOVESPA\s+([CV])\s+(VISTA|FRACIONARIO)\s+(.+?)\s+(\d+)\s+([\d.,]+)\s+([\d.,]+)\s+([DC])$', re.MULTILINE)
            market_label = 'A vista'

        for line in section.split('\n'):
            line = ' '.join(line.strip().split())
            match = pattern.match(line)
            if match:
                if market_label == 'BMF':
                    trades.append({
                        'market': market_label,
                        'direction': match.group(1),
                        'ticker': match.group(2),
                        'maturity': match.group(3),
                        'quantity': int(match.group(4)),
                        'price': cls.clean_numeric(match.group(5)),
                        'trade_type': match.group(6),
                        'value': cls.clean_numeric(match.group(7)),
                        'dc': match.group(8)
                    })
                else:
                    trades.append({
                        'market': market_label,
                        'direction': match.group(1),
                        'type': match.group(2),
                        'ticker': match.group(3),
                        'quantity': int(match.group(4)),
                        'price': cls.clean_numeric(match.group(5)),
                        'value': cls.clean_numeric(match.group(6)),
                        'dc': match.group(7)
                    })
        return trades

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
                        if info['client_name']:
                            client_info = info
                        trades = parser.extract_trades(page_text)
                        for trade in trades:
                            trade['broker'] = parser.BROKER_NAME
                            trade['date'] = date
                            trade['invoice'] = invoice
                        all_trades.extend(trades)
                        break

        return {"trades": all_trades, "client_info": client_info}

    @classmethod
    def process_directory(cls, directory: str) -> pd.DataFrame:
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
            return None

        df = pd.DataFrame(all_trades)
        desired_order = ['broker', 'date', 'invoice', 'market'] + [col for col in df.columns if col not in ['broker', 'date', 'invoice', 'market']]
        df = df[desired_order]
        df.sort_values(by=['broker', 'date'], inplace=True)

        output_file = "consolidated_trades.xlsx"
        with pd.ExcelWriter(output_file) as writer:
            client_info_df = pd.DataFrame([[client_name, client_cpf]], columns=['Client Name', 'CPF'])

            vista_df = df[df['market'] == 'A vista']
            bmf_df = df[df['market'] == 'BMF']

            if not vista_df.empty:
                client_info_df.to_excel(writer, sheet_name='A Vista', index=False, startrow=0)
                vista_df.to_excel(writer, sheet_name='A Vista', index=False, startrow=2)

            if not bmf_df.empty:
                client_info_df.to_excel(writer, sheet_name='BMF', index=False, startrow=0)
                bmf_df.to_excel(writer, sheet_name='BMF', index=False, startrow=2)

        print(f"Saved {len(df)} trades to {output_file}")
        return df

if __name__ == "__main__":
    TradeProcessor.process_directory("./")
