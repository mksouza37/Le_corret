import os
import re
import pdfplumber
import pandas as pd
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
    invoice_patterns=[
        r"Nota\s+de\s+Negociação\s+N(?:º|o|\u00b0)\s*[:\-]?\s*(\d+)",
        r"Nr\.?\s*nota\s*[:\-]?\s*(\d+)",
        r"Nota\s*[:\-]?\s*(\d+)"
    ],
    date_patterns=[r'Data\s+pregão\s*(?:\n|\r|\s)*(\d{2}/\d{2}/\d{4})', r'(\d{2}/\d{2}/\d{4})'],
    client_patterns={},
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
        top_fields = self._extract_top_table_fields(text)
        cpf = self._extract_top_client_fields(text)

        return {
            "broker": self.config.name,
            "invoice": top_fields.get("invoice") or self._extract_first_match(text, self.config.invoice_patterns),
            "date": self._extract_first_match(text, self.config.date_patterns),
            "client_cpf": cpf,
            "trades": self._extract_trades(text)
        }

    def _extract_text(self, file_path: str) -> str:
        with pdfplumber.open(file_path) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)

    def _extract_top_table_fields(self, text: str) -> Dict[str, str]:
        lines = text.splitlines()
        info = {}

        for i, line in enumerate(lines):
            line_clean = line.strip().lower()
            if "nr. nota" in line_clean or "nr nota" in line_clean or "nº nota" in line_clean:
                if i + 1 < len(lines):
                    invoice_candidate = re.search(r'\d{5,}', lines[i + 1])
                    if invoice_candidate:
                        info['invoice'] = invoice_candidate.group(0)
                        break
        return info

    def _extract_top_client_fields(self, text: str) -> str:
        lines = text.splitlines()
        for i, line in enumerate(lines):
            lower_line = line.lower()
            if "c.p.f" in lower_line or "cpf" in lower_line:
                cpf_match = re.search(r'(\d{3}[\.\s]?\d{3}[\.\s]?\d{3}[-\s]?\d{2})', line)
                if not cpf_match and i + 1 < len(lines):
                    cpf_match = re.search(r'(\d{3}[\.\s]?\d{3}[\.\s]?\d{3}[-\s]?\d{2})', lines[i + 1])
                if cpf_match:
                    return cpf_match.group(1).replace(" ", "").strip()
        return ""

    def _extract_first_match(self, text: str, patterns: List[str]) -> str:
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL | re.MULTILINE)
            if match:
                invoice = match.group(1).strip()
                if invoice:
                    return invoice
        return ""

    def _extract_client_info(self, text: str) -> Dict[str, str]:
        return {"cpf": self._extract_top_client_fields(text)}

    def _extract_trades(self, text: str) -> List[Dict]:
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

        return trades

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
                    'client_cpf': result.get('client_cpf', '')
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
