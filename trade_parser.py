import os
import re
import pdfplumber
import pandas as pd
import tabula
from dataclasses import dataclass
from typing import List, Dict, Optional

@dataclass
class BrokerConfig:
    name: str
    invoice_patterns: List[str]
    date_patterns: List[str]
    client_patterns: Dict[str, str]
    trade_start_marker: str
    trade_end_marker: str
    trade_patterns: List[re.Pattern]
    columns: List[str]

BTG_CONFIG = BrokerConfig(
    name="BTG",
    invoice_patterns=[r"Nota\s+de\s+Negociação\s+Nº\s*(\d+)", r"Nr\. nota\s*(\d+)"],
    date_patterns=[r'Data\s+pregão\s*(?:\n|\r|\s)*(\d{2}/\d{2}/\d{4})', r'(\d{2}/\d{2}/\d{4})'],
    client_patterns={
        "name": r"Cliente\s+\d+\s+([A-Z\s]+)\n",
        "cpf": r"CPF[./\s]*(\d{3}\.\d{3}\.\d{3}-\d{2})"
    },
    trade_start_marker="Negócios realizados",
    trade_end_marker="Resumo dos Negócios",
    trade_patterns=[
        re.compile(r'^([CV])\s+(\S+)\s+(\d{2}/\d{2}/\d{4})\s+(\d+)\s+([\d.,]+)\s+(\S+)\s+([\d.,]+)\s+([DC])'),
        re.compile(r'^(\d+)-BOVESPA\s+([CV])\s+(VISTA|FRACIONARIO)\s+(.+?)\s+(\d+)\s+([\d.,]+)\s+([\d.,]+)\s+([DC])')
    ],
    columns=['market', 'direction', 'type', 'ticker', 'quantity', 'price', 'value', 'dc']
)

class GenericParser:
    def __init__(self, config: BrokerConfig):
        self.config = config

    def parse_pdf(self, file_path: str) -> Dict:
        text = self._extract_text(file_path)
        tables = self._extract_tables(file_path)

        result = {
            "broker": self.config.name,
            "invoice": self._extract_first_match(text, self.config.invoice_patterns),
            "date": self._extract_first_match(text, self.config.date_patterns),
            "client": self._extract_client_info(text),
            "trades": self._extract_trades(text, tables)
        }
        return result

    def _extract_text(self, file_path: str) -> str:
        with pdfplumber.open(file_path) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)

    def _extract_tables(self, file_path: str) -> List[pd.DataFrame]:
        try:
            return tabula.read_pdf(file_path, pages='all', multiple_tables=True, lattice=True)
        except:
            return []

    def _extract_first_match(self, text: str, patterns: List[str]) -> str:
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return ""

    def _extract_client_info(self, text: str) -> Dict[str, str]:
        info = {}
        for key, pattern in self.config.client_patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            info[key] = match.group(1).strip() if match else ""
        return info

    def _extract_trades(self, text: str, tables: List[pd.DataFrame]) -> List[Dict]:
        trades = []

        if "C/V Mercadoria Vencimento" in text:
            start = text.find("C/V Mercadoria Vencimento")
            end = text.find("+Custos BM&F", start)
            section = text[start:end] if start != -1 and end != -1 else ""

            pattern = re.compile(r'^([CV])\s+(\S+)\s+(\d{2}/\d{2}/\d{4})\s+(\d+)\s+([\d.,]+)\s+(\S+)\s+([\d.,]+)\s+([DC])')
            market_label = 'BMF'
        else:
            start = text.find(self.config.trade_start_marker)
            end = text.find(self.config.trade_end_marker, start)
            section = text[start:end] if start != -1 and end != -1 else ""

            pattern = re.compile(r'^(\d+)-BOVESPA\s+([CV])\s+(VISTA|FRACIONARIO)\s+(.+?)\s+(\d+)\s+([\d.,]+)\s+([\d.,]+)\s+([DC])')
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
                        'price': self._clean_numeric(match.group(5)),
                        'trade_type': match.group(6),
                        'value': self._clean_numeric(match.group(7)),
                        'dc': match.group(8)
                    })
                else:
                    trades.append({
                        'market': market_label,
                        'direction': match.group(2),
                        'type': match.group(3),
                        'ticker': match.group(4),
                        'quantity': int(match.group(5)),
                        'price': self._clean_numeric(match.group(6)),
                        'value': self._clean_numeric(match.group(7)),
                        'dc': match.group(8)
                    })

        for table in tables:
            for _, row in table.iterrows():
                trade = self._build_trade_from_table(row)
                if trade:
                    trades.append(trade)

        return trades

    def _build_trade_from_table(self, row: pd.Series) -> Optional[Dict]:
        try:
            ticker = str(row.get('Especificação do título') or row.get('Mercadoria', '')).strip()
            quantity = int(str(row.get('Quantidade', '0')).replace('.', '').replace(',', '').strip() or 0)
            price = self._clean_numeric(str(row.get('Preço / Ajuste', '0')))
            value = self._clean_numeric(str(row.get('Valor Operação / Ajuste', '0')))
            direction = str(row.get('C/V', '')).strip().upper()

            market = 'BMF' if 'WIN' in ticker or 'IND' in ticker or 'FUT' in ticker else 'A vista'

            if ticker and quantity > 0:
                return {
                    'market': market,
                    'direction': direction,
                    'ticker': ticker,
                    'quantity': quantity,
                    'price': price,
                    'value': value,
                    'dc': str(row.get('D/C', '')).strip()
                }
        except:
            return None
        return None

    def _clean_numeric(self, value: str) -> float:
        try:
            return float(value.replace('.', '').replace(',', '.'))
        except:
            return 0.0

class TradeProcessor:
    PARSERS = [GenericParser(BTG_CONFIG)]

    @classmethod
    def process_pdf(cls, file_path: str) -> List[Dict]:
        all_trades = []

        for parser in cls.PARSERS:
            result = parser.parse_pdf(file_path)
            for trade in result['trades']:
                trade.update({
                    'broker': result['broker'],
                    'date': result['date'],
                    'invoice': result['invoice'],
                    'client_name': result['client'].get('name', ''),
                    'client_cpf': result['client'].get('cpf', '')
                })
                all_trades.append(trade)

        return all_trades

    @classmethod
    def process_directory(cls, directory: str) -> pd.DataFrame:
        all_trades = []

        for filename in os.listdir(directory):
            if filename.lower().endswith('.pdf'):
                filepath = os.path.join(directory, filename)
                trades = cls.process_pdf(filepath)
                all_trades.extend(trades)

        df = pd.DataFrame(all_trades)
        if not df.empty and 'invoice' not in df.columns:
            df['invoice'] = ''
        if not df.empty:
            df.sort_values(by=['broker', 'date'], inplace=True)
        return df
