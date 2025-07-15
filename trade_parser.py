"""
trade_parser_refactored.py

Refactored module for parsing brokerage PDF invoices into structured Excel files.
Supports multiple brokers and handles both A VISTA and BM&F market types.
"""

import os
import re
import gc
import shutil
import unicodedata
import pdfplumber
import pandas as pd
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment
from PyPDF2 import PdfReader, PdfWriter

# === INITIAL CLEANUP ===
# Remove old Excel output if present
if os.path.exists("output_all_invoices.xlsx"):
    os.remove("output_all_invoices.xlsx")

# Clear temporary directory for multi-date processing
TEMP_DIR = "split_by_date_temp"
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)

# Clean global DataFrames (e.g., in Colab context)
try:
    del df_trades, df_summary, df_consistency
except:
    pass
gc.collect()


# === CONSTANTS ===
# Maps normalized summary keys to various aliases found in invoices
RESUMO_KEY_MAP = {
    "Debêntures": ["Debêntures"],
    "Vendas à Vista": ["Vendas à vista", "Venda à Vista", "Vendas à Vista"],
    "Compras à Vista": ["Compras à vista", "Compra à Vista"],
    "Opções - compras": ["Opções - compras", "Compra Opções"],
    "Opções - vendas": ["Opções - vendas", "Venda Opções"],
    "Operações à termo": ["Operações à termo", "Operação a Termo"],
    "Valor das oper. c/ títulos públ. (v. nom.)": [
        "Valor das oper. c/ títulos públ. (v. nom.)",
        "Valor das oper. com títulos públicos",
        "Valor das operações com títulos públicos"
    ],
    "Valor das operações": ["Valor das operações", "Total das operações"],
    "Valor líquido das operações": ["Valor líquido das operações", "Líquido operações"],
    "Taxa de liquidação": ["Taxa de liquidação", "Taxa liquidação"],
    "Taxa de Registro": ["Taxa de Registro", "Registro"],
    "Total CBLC": ["Total CBLC"],
    "Taxa de termo/opções": ["Taxa de termo/opções", "Taxa termo/opções"],
    "Taxa A.N.A.": ["Taxa A.N.A.", "Taxa ANA"],
    "Emolumentos": ["Emolumentos"],
    "Total Bovespa / Soma": ["Total Bovespa / Soma", "Total Bovespa/Soma"],
    "Clearing": ["Clearing", "Taxa Operacional"],
    "Execução": ["Execução"],
    "Execução casa": ["Execução casa", "Taxa de Custódia"],
    "Corretagem": ["Corretagem"],
    "ISS": ["ISS(SÃO PAULO)", "ISS (SÃO PAULO)", "ISS", "ISS* (SÃO PAULO - SP)", "Impostos"],
    "IRRF sobre operações": ["I.R.R.F s/operações", "IRRF s/operações", "I.R.R.F. s/ operações", "IRRF s/ operações"],
    "Outras": ["Outras", "Outros"],
    "Total corretagem / Despesas": ["Total corretagem / Despesas", "Total Custos / Despesas"],
    "Valor a ser Liquidado": ["Valor a ser Liquidado", "Líquido para"]
}


# === UTILITY FUNCTIONS ===

def remove_accents(text: str) -> str:
    """Removes accents from a given text for normalization."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )


def classify_invoice_type(text: str) -> str:
    """
    Classifies an invoice as 'avista', 'bmf', or 'unknown' based on keywords.
    """
    normalized = remove_accents(text.lower())

    if any(keyword in normalized for keyword in [
        "negocios realizados", "resumo dos negocios", "negocios efetuados"
    ]):
        return "avista"
    elif "c/v mercadoria vencimento" in normalized and "ajuste de posicao" in normalized:
        return "bmf"
    return "unknown"


# === MULTI-DATE PDF HELPERS ===

def extract_dates_per_page(file_path: str) -> List[Optional[str]]:
    """Extracts a list of trading dates from each page of a PDF."""
    dates = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            found_date = None
            for pattern in [
                r"Data\s+Preg[aã]o\s*(?:\n|\r|\s)*(\d{2}/\d{2}/\d{4})",
                r"(\d{2}/\d{2}/\d{4})"
            ]:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    found_date = match.group(1)
                    break
            dates.append(found_date)
    return dates


def group_pages_by_date(dates: List[Optional[str]]) -> List[Tuple[str, List[int]]]:
    """
    Groups PDF page indexes by date. Useful for splitting multi-invoice PDFs.
    """
    groups = []
    current_date = None
    current_pages = []

    for i, date in enumerate(dates):
        if date is None:
            continue
        if date != current_date:
            if current_pages:
                groups.append((current_date, current_pages))
            current_date = date
            current_pages = [i]
        else:
            current_pages.append(i)

    if current_pages:
        groups.append((current_date, current_pages))
    return groups


def prepare_files_for_processing(pdf_files: List[str]) -> List[str]:
    """
    Prepares PDFs for processing by splitting multi-date PDFs into single-date files.
    """
    output_dir = TEMP_DIR
    os.makedirs(output_dir, exist_ok=True)
    files_to_process = []

    for file_path in pdf_files:
        dates = extract_dates_per_page(file_path)
        unique_dates = list(set(d for d in dates if d))

        if len(unique_dates) <= 1:
            files_to_process.append(file_path)
        else:
            groups = group_pages_by_date(dates)
            reader = PdfReader(file_path)
            for date_str, page_numbers in groups:
                writer = PdfWriter()
                for page_num in page_numbers:
                    writer.add_page(reader.pages[page_num])
                new_filename = f"{os.path.splitext(os.path.basename(file_path))[0]}_{date_str.replace('/', '-')}.pdf"
                out_path = os.path.join(output_dir, new_filename)
                with open(out_path, "wb") as f:
                    writer.write(f)
                files_to_process.append(out_path)

    return files_to_process

# === BROKER CONFIGURATION ===

@dataclass
class BrokerConfig:
    name: str
    invoice_patterns: List[str]
    date_patterns: List[str]
    client_patterns: Dict[str, str]
    trade_start_marker: str
    trade_end_marker: str
    signature_patterns: List[str]

# === BROKER CONFIGURATIONS ===

BTG_CONFIG = BrokerConfig(
    name="BTG",
    invoice_patterns=[
        r"Nota\s+de\s+Negociação\s+N(?:º|o|\u00b0)?\s*[:\-]?\s*(\d+)",
        r"Nr\.?\s*nota\s*[:\-]?\s*(\d+)",
        r"Nota\s*[:\-]?\s*(\d+)"
    ],
    date_patterns=[
        r'Data\s+preg[aã]o\s*(?:\n|\r|\s)*(\d{2}/\d{2}/\d{4})',
        r'(\d{2}/\d{2}/\d{4})'
    ],
    client_patterns={},
    trade_start_marker="Negócios realizados",
    trade_end_marker="Resumo dos Negócios",
    signature_patterns=[r"BTG\s+Pactual", r"BTG\s+Corretora"]
)

ITAU_CONFIG = BrokerConfig(
    name="ITAU",
    invoice_patterns=[
        r"Nr\.?\s*Nota\s*(?:Folha)?\s*(?:\d+\s+)?(\d+)",
    ],
    date_patterns=[
        r"Data\s+Preg[aã]o\s*(?:\n|\r|\s)*(\d{2}/\d{2}/\d{4})",
        r'(\d{2}/\d{2}/\d{4})'
    ],
    client_patterns={},
    trade_start_marker="Negócios Realizados",
    trade_end_marker="Resumo de negócios",
    signature_patterns=[r"Ita[úu]\s+Corretora", r"ITA[ÚU] UNIBANCO"],
)

AGORA_CONFIG = BrokerConfig(
    name="AGORA",
    invoice_patterns=[
        r"Nota\s+de\s+Corretagem\s*Nr\.?\s*Nota\s*(?:Folha)?\s*(?:\d+\s+)?(\d+)",
        r"Nr\.?\s*Nota\s*(?:\d+\s+)?(\d+)"
    ],
    date_patterns=[
        r"Data\s+preg[aã]o\s*(?:\n|\r|\s)*(\d{2}/\d{2}/\d{4})",
        r'(\d{2}/\d{2}/\d{4})'
    ],
    client_patterns={},
    trade_start_marker="Negocios Realizados",
    trade_end_marker="Resumo dos Negócios",
    signature_patterns=[
        r"AGORA\s+CORRETORA", r"agorainvestimentos\.com\.br"],
)

XP_CONFIG = BrokerConfig(
    name="XP",
    invoice_patterns=[
        r"NOTA\s+DE\s+NEGOCIA[ÇC][AÃ]O\s*Nr\.?\s*nota\s*(\d+)",
        r"Nr\.?\s*nota\s*(\d+)"
    ],
    date_patterns=[
        r"Data\s+preg[aã]o\s*(?:\n|\r|\s)*(\d{2}/\d{2}/\d{4})",
        r'(\d{2}/\d{2}/\d{4})'
    ],
    client_patterns={},
    trade_start_marker="Negócios realizados",
    trade_end_marker="Resumo dos Negócios",
    signature_patterns=[
        r"XP\s+INVESTIMENTOS\s+CORRETORA", r"xpi\.com\.br"]
)

# === BASE PARSER CLASS ===

class GenericParser:
    """
    Base class for parsing brokerage PDFs. Designed to be extended by broker-specific parsers.
    """

    def __init__(self, config: BrokerConfig):
        self.config = config

    def parse_pdf(self, file_path: str) -> Dict:
        """
        Parses a PDF and returns extracted metadata, trades, and summary.
        """
        text = self._extract_text(file_path)
        top_fields = self._extract_top_table_fields(text)
        cpf = self._extract_top_client_fields(text)

        invoice = top_fields.get("invoice")
        if not invoice:
            invoice = self._extract_first_match(text, self.config.invoice_patterns)

        return {
            "broker": self.config.name,
            "invoice": invoice,
            "date": self._extract_first_match(text, self.config.date_patterns),
            "client_cpf": cpf,
            "trades": self._extract_trades(text),
            "summary": self._extract_summary_values(text)
        }

    def _extract_text(self, file_path: str) -> str:
        """Extracts raw text from all pages of a PDF."""
        with pdfplumber.open(file_path) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)

    def _extract_top_client_fields(self, text: str) -> str:
        """Extracts CPF from top client information block."""
        lines = text.splitlines()
        for i, line in enumerate(lines):
            if "cpf" in line.lower():
                cpf_match = re.search(r'(\d{3}[\.\s]?\d{3}[\.\s]?\d{3}[-\s]?\d{2})', line)
                if not cpf_match and i + 1 < len(lines):
                    cpf_match = re.search(r'(\d{3}[\.\s]?\d{3}[\.\s]?\d{3}[-\s]?\d{2})', lines[i + 1])
                if cpf_match:
                    return cpf_match.group(1).replace(" ", "").strip()
        return ""

    def _extract_top_table_fields(self, text: str) -> Dict[str, str]:
        """Attempts to extract invoice number and metadata from known patterns."""
        lines = text.splitlines()
        info = {}

        for i, line in enumerate(lines):
            normalized = line.lower()

            if "nr.nota" in normalized and "data pregão" in normalized:
                if i + 1 < len(lines):
                    next_line = lines[i + 1]
                    matches = re.findall(r'\d{2}/\d{2}/\d{4}|\d+', next_line)
                    if matches:
                        info["invoice"] = matches[0]
                        break

            match_inline = re.search(r"nota\s*(?:nº|no|n°|num)?[\s:]*\s*(\d{4,})", line, re.IGNORECASE)
            if match_inline:
                info["invoice"] = match_inline.group(1)
                break

            if any(k in normalized for k in ["nr. nota", "nr nota", "nº nota", "nota de negociação"]):
                if i + 1 < len(lines):
                    nextline_match = re.search(r'\d{4,}', lines[i + 1])
                    if nextline_match:
                        info["invoice"] = nextline_match.group(0)
                        break

        return info

    def _extract_first_match(self, text: str, patterns: List[str]) -> str:
        """Searches for the first regex match from a list of patterns."""
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL | re.MULTILINE)
            if match:
                return match.group(1).strip()
        return ""

    def _clean_numeric(self, value: str) -> float:
        """Converts a numeric string to float, handling Brazilian formatting."""
        try:
            return float(value.replace('.', '').replace(',', '.'))
        except Exception:
            return 0.0

    # Placeholder: these will be overridden in subclasses or extended later
    def _extract_trades(self, text: str) -> List[Dict]:
        return []

    def _extract_summary_values(self, text: str) -> Dict[str, float]:
        return {key: 0.0 for key in RESUMO_KEY_MAP}


class AVistaParser(GenericParser):
    """Parser for A VISTA invoices (inherits GenericParser behavior)."""
    pass


class BMFParser(GenericParser):
    """
    Specialized parser for BM&F-style invoices.
    Handles positional value extraction.
    """

    def parse_pdf(self, file_path: str) -> Dict:
        text = self._extract_text(file_path)
        top_fields = self._extract_top_table_fields(text)
        cpf = self._extract_top_client_fields(text)

        invoice = top_fields.get("invoice")
        if not invoice:
            invoice = self._extract_first_match(text, self.config.invoice_patterns)

        return {
            "broker": self.config.name,
            "invoice": invoice,
            "date": self._extract_first_match(text, self.config.date_patterns),
            "client_cpf": cpf,
            "trades": self._extract_trades(text),
            "summary": self._extract_summary_values(text, file_path)
        }

    def _extract_trades(self, text: str) -> List[Dict]:
        trades = []
        lines = text.splitlines()

        for line in lines:
            tokens = line.strip().split()
            if len(tokens) >= 9 and re.match(r"^[CV]$", tokens[0]) and re.match(r"\d{2}/\d{2}/\d{4}", tokens[2]):
                try:
                    trade = {
                        "Tipo": "BM&F",
                        "C/V": tokens[0],
                        "Mercadoria": tokens[1],
                        "Vencimento": tokens[2],
                        "Quantidade": int(tokens[3]),
                        "Preço / Ajuste": self._clean_numeric(tokens[4]),
                        "Tipo Negócio": tokens[5],
                        "Valor Operação": self._clean_numeric(tokens[6]),
                        "D/C": tokens[7],
                        "Taxa Operacional": self._clean_numeric(tokens[8])
                    }
                    trades.append(trade)
                except Exception as e:
                    print(f"⚠️ BM&F trade parse error: {line}\n{e}")
        return trades

    def _extract_summary_values(self, text: str, file_path: str) -> Dict[str, float]:
        # Simplified placeholder; real version will use positional extraction
        return super()._extract_summary_values(text)

# === TRADE PROCESSOR ===

class TradeProcessor:
    """
    Coordinates end-to-end PDF processing: parsing, aggregation, and consistency checks.
    """

    PARSERS = []

    @classmethod
    def register_parsers(cls):
        from copy import deepcopy
        from_types = [BTG_CONFIG, ITAU_CONFIG, AGORA_CONFIG, XP_CONFIG]
        cls.PARSERS = [GenericParser(deepcopy(cfg)) for cfg in from_types]

    @classmethod
    def process_pdfs(cls, file_paths: List[str]):
        """
        Parses a list of PDF files and returns trades and summary records.
        """
        cls.register_parsers()
        all_trades, all_summaries = [], []

        for file_path in file_paths:
            try:
                with pdfplumber.open(file_path) as pdf:
                    text = "\n".join(page.extract_text() or "" for page in pdf.pages)

                matched_parser = None
                for parser in cls.PARSERS:
                    if cls._match_broker_by_signature(text, parser):
                        invoice_type = classify_invoice_type(text)
                        if invoice_type == "avista":
                            matched_parser = AVistaParser(parser.config)
                        elif invoice_type == "bmf":
                            matched_parser = BMFParser(parser.config)
                        else:
                            matched_parser = parser  # fallback
                        break

                if not matched_parser:
                    print(f"⚠️ No matching parser found for {file_path}")
                    continue

                result = matched_parser.parse_pdf(file_path)

                for trade in result.get("trades", []):
                    trade.update({
                        "broker": result["broker"],
                        "date": result["date"],
                        "invoice": result["invoice"],
                        "client_cpf": result["client_cpf"]
                    })
                    all_trades.append(trade)

                tipo_val = result["trades"][0].get("Tipo") if result.get("trades") else "Unknown"

                if result.get("summary"):
                    all_summaries.append({
                        "invoice": result["invoice"],
                        "broker": result["broker"],
                        "Tipo": tipo_val,
                        **result["summary"]
                    })

            except Exception as e:
                print(f"❌ Error processing {file_path}: {e}")

        return all_trades, all_summaries

    @staticmethod
    def _match_broker_by_signature(text: str, parser: GenericParser) -> bool:
        """Matches text content with known broker signature patterns."""
        for pattern in parser.config.signature_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False

    @classmethod
    def process_directory(cls, directory: str) -> pd.DataFrame:
        pdf_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.lower().endswith(".pdf")]
        if not pdf_files:
            print("❌ No PDF files found in directory.")
            return pd.DataFrame()

        prepared_files = prepare_files_for_processing(pdf_files)
        trades, summaries = cls.process_pdfs(prepared_files)
        df_trades = pd.DataFrame(trades)

        if "Valor Operação" in df_trades.columns and "Valor Operação / Ajuste" in df_trades.columns:
            df_trades["valor_trades"] = df_trades.apply(
                lambda row: row["Valor Operação"] if str(row.get("Tipo", "")).strip().lower() == "bm&f" else row[
                    "Valor Operação / Ajuste"],
                axis=1
            )
        elif "Valor Operação" in df_trades.columns:
            df_trades["valor_trades"] = df_trades["Valor Operação"]
        elif "Valor Operação / Ajuste" in df_trades.columns:
            df_trades["valor_trades"] = df_trades["Valor Operação / Ajuste"]
        else:
            df_trades["valor_trades"] = 0

        df_summary = pd.DataFrame(summaries).dropna(how='all')

        # === CONSISTENCY SHEET (with broker column, handles missing summary/trades) ===
        if "invoice" in df_trades.columns or "invoice" in df_summary.columns:

            trade_value_column = "valor_trades"

            if trade_value_column:
                trade_totals = (
                    df_trades.groupby(["invoice", "broker", "Tipo"])[trade_value_column]
                    .sum()
                    .reset_index()
                )

                summary_candidates = ["Valor das operações", "Valor dos negócios"]
                valor_col = None
                for col in summary_candidates:
                    if col in df_summary.columns:
                        valor_col = col
                        break

                if valor_col:
                    resumo_valores = df_summary[["invoice", "broker", "Tipo", valor_col]].rename(
                        columns={valor_col: "valor_das_operacoes"}
                    )

                    all_invoices = pd.DataFrame(
                        pd.concat([
                            trade_totals[["invoice", "broker", "Tipo"]],
                            resumo_valores[["invoice", "broker", "Tipo"]]
                        ]).drop_duplicates(), columns=["invoice", "broker", "Tipo"]
                    )

                    df_consistency = all_invoices \
                        .merge(trade_totals, on=["invoice", "broker", "Tipo"], how="left") \
                        .merge(resumo_valores, on=["invoice", "broker", "Tipo"], how="left")

                    df_consistency[trade_value_column] = df_consistency[trade_value_column].fillna(0)
                    df_consistency["valor_das_operacoes"] = df_consistency["valor_das_operacoes"].fillna(0)
                    df_consistency["Diferença"] = (
                        df_consistency[trade_value_column] - df_consistency["valor_das_operacoes"]
                    ).round(2)
                    df_consistency["Status"] = df_consistency["Diferença"].apply(
                        lambda x: "OK" if abs(x) < 0.01 else "Inconsistência")
                else:
                    df_consistency = pd.DataFrame()
            else:
                df_consistency = pd.DataFrame()
        else:
            df_consistency = pd.DataFrame()

        df_trades.rename(columns={
            "invoice": "Número da Nota", "broker": "Corretora",
            "client_cpf": "CPF", "date": "Data da Operação"
        }, inplace=True)

        df_summary.rename(columns={
            "invoice": "Número da Nota", "broker": "Corretora"
        }, inplace=True)

        if not df_consistency.empty:
            df_consistency.rename(columns={
                "invoice": "Número da Nota", "broker": "Corretora"
            }, inplace=True)

        cpf_value = df_trades["CPF"].iloc[0].replace('.', '').replace('-', '')
        output_file = os.path.join("tmp", f"trades_output - {cpf_value}.xlsx")

        export_to_excel(df_trades, df_summary, df_consistency, output_file)

        return df_trades

# === EXCEL EXPORT PLACEHOLDER ===

def export_to_excel(df_trades: pd.DataFrame, df_summary: pd.DataFrame, output_path: str) -> None:
    """
    Placeholder: Exports given DataFrames to Excel file with formatted sheets.
    Should handle Negócios, Resumo, Consistência.
    """
    from openpyxl import Workbook

    wb = Workbook()
    default_sheet = wb.active
    wb.remove(default_sheet)  # ⛔ Prevent "no visible sheet" Excel error

    if not df_trades.empty:
        ws = wb.create_sheet(title="Negócios")

    # Simple dump: replace this with structured export logic
    for r_idx, row in enumerate(df_trades.itertuples(index=False), start=2):
        for c_idx, value in enumerate(row, start=1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    for c_idx, col_name in enumerate(df_trades.columns, start=1):
        ws.cell(row=1, column=c_idx, value=col_name).font = Font(bold=True)

    wb.save(output_path)
    print(f"✅ Excel saved to {output_path}")

# === FULL EXCEL EXPORT ===

def export_to_excel(df_trades: pd.DataFrame, df_summary: pd.DataFrame, df_consistency: pd.DataFrame, output_file: str) -> None:
    """
    Writes the trades, summary, and consistency checks into an Excel workbook.
    Includes formatting and structured layout by trade type.
    """

    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment

    def autofit_columns(worksheet, df):
        from openpyxl.utils import get_column_letter

        column_widths = {}
        for row in worksheet.iter_rows():
            for cell in row:
                if cell.value:
                    col_letter = get_column_letter(cell.column)
                    current_width = column_widths.get(col_letter, 0)
                    value_length = len(str(cell.value))
                    column_widths[col_letter] = max(current_width, value_length)
                    if cell.font and cell.font.bold:
                        cell.alignment = Alignment(horizontal="center")

        for col_letter, width in column_widths.items():
            worksheet.column_dimensions[col_letter].width = width + 2

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        workbook = writer.book


        cpf_value = df_trades["CPF"].iloc[0].replace('.', '').replace('-', '')
        output_file = os.path.join("tmp", f"trades_output - {cpf_value}.xlsx")

        def autofit_columns(worksheet):
            from openpyxl.utils import get_column_letter

            column_widths = {}

            for row in worksheet.iter_rows():
                for cell in row:
                    if cell.value:
                        col_letter = get_column_letter(cell.column)
                        current_width = column_widths.get(col_letter, 0)
                        value_length = len(str(cell.value))
                        column_widths[col_letter] = max(current_width, value_length)

                        # Center align headers (bold cells)
                        if cell.font and cell.font.bold:
                            cell.alignment = Alignment(horizontal="center")

            for col_letter, width in column_widths.items():
                worksheet.column_dimensions[col_letter].width = width + 2  # +2 for padding

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            workbook = writer.book

            # === Negócios ===
            ws = workbook.create_sheet(title="Negócios")
            row_idx = 1

            if not df_trades.empty:
                tipos_ordenados = ["A Vista", "BM&F"]
                for tipo in tipos_ordenados:
                    tipo_lower = tipo.strip().lower()
                    if tipo_lower not in df_trades["Tipo"].str.lower().str.strip().unique():
                        continue

                    block = df_trades[df_trades["Tipo"].str.lower().str.strip() == tipo_lower].copy()
                    block = block.sort_values(by=["Corretora", "Data da Operação"])

                    if tipo_lower == "a vista":
                        block_columns = [
                            "CPF", "Tipo", "Corretora", "Data da Operação", "Número da Nota",
                            "Negociação", "C/V", "Tipo Mercado", "Especificação do Título",
                            "Quantidade", "Preço / Ajuste", "Valor Operação / Ajuste", "D/C"
                        ]
                    elif tipo_lower == "bm&f":
                        block_columns = [
                            "CPF", "Tipo", "Corretora", "Data da Operação", "Número da Nota",
                            "C/V", "Mercadoria", "Vencimento", "Quantidade", "Preço / Ajuste",
                            "Tipo Negócio", "Valor Operação", "D/C", "Taxa Operacional"
                        ]
                    else:
                        block_columns = block.columns.tolist()

                    block = block[[col for col in block_columns if col in block.columns]]

                    ws.cell(row=row_idx, column=1, value=f"*** {tipo.upper()} ***").font = Font(bold=True)

                    row_idx += 2

                    for col_idx, col_name in enumerate(block.columns, start=1):
                        ws.cell(row=row_idx, column=col_idx, value=col_name).font = Font(bold=True)
                    row_idx += 1

                    for _, row in block.iterrows():
                        for col_idx, col_name in enumerate(block.columns, start=1):
                            ws.cell(row=row_idx, column=col_idx, value=row[col_name])
                        row_idx += 1

                    row_idx += 1

            autofit_columns(ws)

            # === Resumo ===
            ws_resumo = workbook.create_sheet(title="Resumo")
            row_idx = 1

            if not df_summary.empty:
                tipos_disponiveis = df_summary["Tipo"].dropna().unique().tolist()  # Get unique types from summary data

                for tipo_lower in tipos_disponiveis:
                    tipo_lower = tipo_lower.lower().strip()
                    block = df_summary[df_summary["Tipo"].str.lower().str.strip() == tipo_lower].copy()

                    if block.empty:
                        continue

                    block = block.sort_values(by=["Corretora", "Número da Nota"])

                    if tipo_lower == "a vista":
                        block_columns = [
                            "Corretora", "Número da Nota", "Debêntures", "Vendas à Vista", "Compras à Vista",
                            "Opções - compras", "Opções - vendas", "Operações à termo",
                            "Valor das oper. c/ títulos públ. (v. nom.)",
                            "Valor das operações", "Valor líquido das operações", "Taxa de liquidação",
                            "Taxa de Registro",
                            "Total CBLC", "Taxa de termo/opções", "Taxa A.N.A.", "Emolumentos", "Total Bovespa / Soma",
                            "Clearing", "Execução", "Execução casa", "Corretagem", "ISS", "IRRF sobre operações",
                            "Outras",
                            "Total corretagem / Despesas", "Valor a ser Liquidado"
                        ]
                    elif tipo_lower == "bm&f":
                        block_columns = [
                            "Corretora", "Número da Nota", "Venda disponível", "Compra disponível", "Venda Opções",
                            "Compra Opções", "Valor dos negócios",
                            "IRRF", "IRRF Day Trade (proj.)", "Taxa operacional", "Taxa registro BM&F",
                            "Taxas BM&F (emol+f.gar)",
                            "Outros Custos", "ISS", "Ajuste de posição", "Ajuste day trade", "Total das despesas",
                            "Outros",
                            "IRRF Corretagem", "Total Conta Investimento", "Total Conta Normal", "Total líquido (#)",
                            "Total líquido da nota"
                        ]

                    else:
                        block_columns = block.columns.tolist()

                    block = block[[col for col in block_columns if col in block.columns]]

                    ws_resumo.cell(row=row_idx, column=1, value=f"*** {tipo_lower.upper()} ***").font = Font(bold=True)
                    row_idx += 2

                    for col_idx, col_name in enumerate(block.columns, start=1):
                        ws_resumo.cell(row=row_idx, column=col_idx, value=col_name).font = Font(bold=True)
                    row_idx += 1

                    for _, row in block.iterrows():
                        for col_idx, col_name in enumerate(block.columns, start=1):
                            ws_resumo.cell(row=row_idx, column=col_idx, value=row[col_name])
                        row_idx += 1

                    row_idx += 1

            autofit_columns(ws_resumo)

            # === Consistência ===
            if not df_consistency.empty:
                ws_consistencia = workbook.create_sheet(title="Consistência")
                row_idx = 1

                if "Tipo" in df_consistency.columns:
                    tipos_consistencia = df_consistency["Tipo"].dropna().unique().tolist()
                else:
                    tipos_consistencia = ["Unknown"]  # Default if 'Tipo' is not in consistency

                for tipo in tipos_consistencia:
                    tipo_lower = tipo.strip().lower()
                    block = df_consistency[df_consistency["Tipo"].str.lower().str.strip() == tipo_lower].copy()

                    if block.empty:
                        continue

                    block = block.sort_values(by=["Corretora", "Número da Nota"])

                    block_columns = ["Corretora", "Número da Nota", "Tipo"]

                    # Show correct raw value column for each Tipo

                    if tipo_lower == "bm&f":
                        temp = df_trades[df_trades["Tipo"].str.lower().str.strip() == tipo_lower]
                        if "Valor Operação" in temp.columns:
                            grouped = temp.groupby(["Número da Nota", "Corretora"])[
                                "Valor Operação"].sum().reset_index()
                            block = block.merge(grouped, on=["Número da Nota", "Corretora"], how="left")
                            block_columns.append("Valor Operação")

                    elif tipo_lower == "a vista":
                        temp = df_trades[df_trades["Tipo"].str.lower().str.strip() == tipo_lower]
                        if "Valor Operação / Ajuste" in temp.columns:
                            grouped = temp.groupby(["Número da Nota", "Corretora"])[
                                "Valor Operação / Ajuste"].sum().reset_index()
                            block = block.merge(grouped, on=["Número da Nota", "Corretora"], how="left")
                            block_columns.append("Valor Operação / Ajuste")

                    block_columns += ["valor_das_operacoes", "Diferença", "Status"]
                    block = block[[col for col in block_columns if col in block.columns]]

                    ws_consistencia.cell(row=row_idx, column=1, value=f"*** {tipo.upper()} ***").font = Font(bold=True)
                    row_idx += 2

                    # === Cabeçalhos formatados com \n e alinhamento central
                    for col_idx, col_name in enumerate(block.columns, start=1):
                        new_label = col_name

                        if tipo_lower == "a vista":
                            if col_name == "Valor Operação / Ajuste":
                                new_label = "Valor Operação / Ajuste\n (Negócios Individuais)"
                            elif col_name == "valor_das_operacoes":
                                new_label = "Valor das Operações\n (Resumo da Nota)"
                        elif tipo_lower == "bm&f":
                            if col_name == "Valor Operação":
                                new_label = "Valor Operação\n (Negócios Individuais)"
                            elif col_name == "valor_das_operacoes":
                                new_label = "Valor das Operações\n (Resumo da Nota)"

                        cell = ws_consistencia.cell(row=row_idx, column=col_idx, value=new_label)
                        cell.font = Font(bold=True)
                        cell.alignment = Alignment(wrap_text=True, horizontal="center")

                    row_idx += 1  # Avança para a linha de dados
                    # === Dados da consistência
                    for _, row in block.iterrows():
                        for col_idx, col_name in enumerate(block.columns, start=1):
                            ws_consistencia.cell(row=row_idx, column=col_idx, value=row[col_name])
                        row_idx += 1

                    row_idx += 1  # Espaçamento entre blocos

                    # === Autoajuste de colunas no final
                    autofit_columns(ws_consistencia)

        return df_trades

