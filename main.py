import os
import io
import re
from typing import Dict, List, Tuple
from dotenv import load_dotenv

from telebot import TeleBot, types
from openai import OpenAI
import numpy as np
import pandas as pd
from pandas import ExcelWriter
from typing import Optional

from datetime import datetime
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import PatternFill

load_dotenv()

# ===================== CONFIG =====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
MAIN_SHEET_NAME = "BUDG"  # read this sheet from the main file
ALLOWED_MAIN_COLUMNS = ["Назва Офферу", "ГЕО", "Загальні витрати"]
ADDITIONAL_REQUIRED_COLS = ["Країна", "Сума депозитів"]

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5")
OPENAI_MAX_CHARS = 60_000  # безпечний ліміт для одного запиту
OPENAI_OUTPUT_COLUMN = "Total spend"  # колонка, яку модель має заповнити/перерахувати

bot = TeleBot(BOT_TOKEN, parse_mode="HTML")

H_THRESH = 9.0  # H <= 9 is "good"
L_THRESH = 39.99  # L > 39.99 is "good" (strict >)
CPA_CAP = 11.0
EPS = 1e-12
EPS_YEL = 1e-6

# CPA Target defaults and overrides
CPA_DEFAULT_TARGET = 8
CPA_OVERRIDES: Dict[str, float] = {
    "Аргентина": 20,
    "Болівія": 15,
    "Венесуела": 5,
    "Габон": 7,
    "Гана": 5,
    "Еквадор": 15,
    "Йорданія": 40,
    "Ірак": 40,
    "Казахстан": 30,
    "Колумбія": 11,
    "Малайзія": 40,
    "Парагвай": 15,
    "Пакистан": 15,
    "Перу": 12,
    "Таїланд": 22,
    "Уругвай": 12,
    "Філіппіни": 10,
}


# ===================== STATE =====================
class UserState:
    def __init__(self):
        self.alloc_mode = None
        self.phase = "WAIT_MAIN"  # WAIT_MAIN -> WAIT_ADDITIONAL, plus allocate flow
        self.main_agg_df: Optional[pd.DataFrame] = None
        self.offers: List[str] = []
        self.current_offer_index: int = 0
        self.offer_deposits: Dict[str, Dict[str, Dict[str, float]]] = {}

        # country maps
        self.country_map_uk_to_en = build_country_map_uk_to_en()
        self.country_canon = build_country_canonical()

        # --- allocate flow state ---
        self.alloc_df: Optional[pd.DataFrame] = None  # parsed result.xlsx
        self.alloc_budget: Optional[float] = None


user_states: Dict[int, UserState] = {}

# ===== ACCESS CONTROL =====
# Заповни своїми Telegram ID (int). Можна зберігати у .env і парсити з ENV.
ALLOWED_USER_IDS = {
    155840708,
    7877906786,
    817278554,
    480823885
}


def _deny_access_message():
    return (
        "⛔ <b>Доступ заборонено.</b>\n"
        "Якщо вам потрібен доступ — зверніться до адміністратора бота."
    )


def _is_allowed_user(user_id: int) -> bool:
    return user_id in ALLOWED_USER_IDS


# Для message-хендлерів
def require_access(handler_func):
    def wrapper(message, *args, **kwargs):
        user_id = getattr(message.from_user, "id", None)
        if user_id is None or not _is_allowed_user(user_id):
            bot.reply_to(message, _deny_access_message())
            return
        return handler_func(message, *args, **kwargs)

    return wrapper


# Для callback-query (інлайн-кнопки)
def require_access_cb(handler_func):
    def wrapper(call, *args, **kwargs):
        user_id = getattr(call.from_user, "id", None)
        if user_id is None or not _is_allowed_user(user_id):
            try:
                bot.answer_callback_query(call.id, "⛔ Немає доступу.")
            except Exception:
                pass
            bot.send_message(call.message.chat.id, _deny_access_message())
            return
        return handler_func(call, *args, **kwargs)

    return wrapper


def _df_to_csv(df: pd.DataFrame) -> str:
    # Без індексу, максимально “плоско”
    bio = io.StringIO()
    df.to_csv(bio, index=False)
    return bio.getvalue()


def _split_df_by_size(df: pd.DataFrame, max_chars: int = OPENAI_MAX_CHARS) -> list[pd.DataFrame]:
    """
    Ділимо датафрейм на чанки, щоб CSV кожного не перевищував ліміт символів.
    """
    # груба оцінка середнього розміру рядка
    sample = min(len(df), 20)
    avg_row_len = len(_df_to_csv(df.head(sample))) / max(sample, 1)
    # запас: шапка + промпт
    rows_per_chunk = max(5, int((max_chars - 3000) / max(avg_row_len, 1)))
    chunks = []
    for i in range(0, len(df), rows_per_chunk):
        chunks.append(df.iloc[i:i + rows_per_chunk].copy())
    return chunks


def _csv_from_text(text: str) -> str:
    """
    Витягує CSV з відповіді (підтримка варіантів без код-блоків та з ```csv ... ```).
    """
    t = text.strip()
    if "```" in t:
        # намагаємося знайти fenced block
        parts = t.split("```")
        # шукаємо блок з csv або перший код-блок
        best = None
        for i in range(1, len(parts), 2):
            block = parts[i]
            if block.lstrip().lower().startswith("csv"):
                best = block.split("\n", 1)[1] if "\n" in block else ""
                break
        if best is None:
            # беремо перший код-блок як fallback
            best = parts[1]
        return best.strip()
    return t


# ===================== NORMALIZATION (countries) =====================

def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip().lower()


def build_country_map_uk_to_en() -> Dict[str, str]:
    m = {
        "Бенін": "Benin",
        "Буркіна-Фасо": "Burkina Faso",
        "Габон": "Gabon",
        "Гаїті": "Haiti",
        "Гана": "Ghana",

        # ❗ уточнення: у XLSX зустрічається "Guinea-Conakry"
        # щоб зв’язати з дод. таблицями, мапимо укр "Гвінея" саме на "Guinea-Conakry"
        # (далі в каноні зведемо це до "Guinea")
        "Гвінея": "Guinea-Conakry",

        # ---- DRC/ROC ----
        "Демократична Республіка Конґо": "Congo (Kinshasa)",
        "ДР Конго": "Congo (Kinshasa)",
        "Конго (Кіншаса)": "Congo (Kinshasa)",
        "Республіка Конго": "Congo (Brazzaville)",
        "Конго-Браззавіль": "Congo (Brazzaville)",
        # якщо просто "Конго" — за замовчуванням DRC
        "Конго": "Congo (Kinshasa)",

        "Камерун": "Cameroon",

        # Кот-д’Івуар — одразу кілька апострофних варіантів
        "Кот-д'Івуар": "Cote d'Ivoire",
        "Кот-д’Івуар": "Cote d'Ivoire",
        "Кот д’Івуар": "Cote d'Ivoire",

        "Кенія": "Kenya",
        "Сенегал": "Senegal",
        "Сьєрра-Леоне": "Sierra Leone",
        "Танзанія": "Tanzania",
        "Того": "Togo",
        "Уганда": "Uganda",
        "Замбія": "Zambia",
        "Ефіопія": "Ethiopia",
        "Нігер": "Niger",
        "Нігерія": "Nigeria",
        "Малі": "Mali",
        "Казахстан": "Kazakhstan",
        "Іспанія": "Spain",
        "Франція": "France",
        "Італія": "Italy",
        "Португалія": "Portugal",
        "Домініканська Республіка": "Dominican Republic",
        "Канада": "Canada",
        "Філіппіни": "Philippines",

        # 🔹 додано з вашого списку «missing»
        "Болівія": "Bolivia",
        "Еквадор": "Ecuador",
        "Колумбія": "Colombia",
        "Парагвай": "Paraguay",
        "Перу": "Peru",
    }
    return {normalize_text(k): v for k, v in m.items()}


def build_country_canonical() -> Dict[str, str]:
    canon = {
        # самоканонічні EN
        "Benin": "Benin",
        "Burkina Faso": "Burkina Faso",
        "Gabon": "Gabon",
        "Haiti": "Haiti",
        "Ghana": "Ghana",
        "Guinea": "Guinea",
        "Congo (Kinshasa)": "Congo (Kinshasa)",
        "Congo (Brazzaville)": "Congo (Brazzaville)",
        "Cameroon": "Cameroon",
        "Cote d'Ivoire": "Cote d'Ivoire",
        "Kenya": "Kenya",
        "Senegal": "Senegal",
        "Sierra Leone": "Sierra Leone",
        "Tanzania": "Tanzania",
        "Togo": "Togo",
        "Uganda": "Uganda",
        "Zambia": "Zambia",
        "Ethiopia": "Ethiopia",
        "Niger": "Niger",
        "Nigeria": "Nigeria",
        "Mali": "Mali",
        "Kazakhstan": "Kazakhstan",
        "Spain": "Spain",
        "France": "France",
        "Italy": "Italy",
        "Portugal": "Portugal",
        "Dominican Republic": "Dominican Republic",
        "Canada": "Canada",
        "Philippines": "Philippines",

        # 🔹 додано з вашого списку «missing»
        "Bolivia": "Bolivia",
        "Ecuador": "Ecuador",
        "Colombia": "Colombia",
        "Paraguay": "Paraguay",
        "Peru": "Peru",

        # синоніми/варіанти написання → канон
        # Cote d'Ivoire
        "Cote DIvoire": "Cote d'Ivoire",
        "Cote dIvoire": "Cote d'Ivoire",
        "Cote D Ivoire": "Cote d'Ivoire",
        "Cote d’ivoire": "Cote d'Ivoire",
        "Côte d’Ivoire": "Cote d'Ivoire",
        "Ivory Coast": "Cote d'Ivoire",

        # Guinea-Conakry → Guinea
        "Guinea-Conakry": "Guinea",
        "Guinea Conakry": "Guinea",
        "Guinea, Conakry": "Guinea",

        # DRC/ROC варіанти
        "DRC": "Congo (Kinshasa)",
        "DR Congo": "Congo (Kinshasa)",
        "Congo (DRC)": "Congo (Kinshasa)",
        "Democratic Republic of the Congo": "Congo (Kinshasa)",
        "Democratic Republic of Congo": "Congo (Kinshasa)",
        "Congo-Kinshasa": "Congo (Kinshasa)",

        "Republic of the Congo": "Congo (Brazzaville)",
        "Congo Republic": "Congo (Brazzaville)",
        "Congo-Brazzaville": "Congo (Brazzaville)",

        # UA → EN канон (на випадок, якщо десь просочиться укр у дод. таблицях)
        "кот-д'івуар": "Cote d'Ivoire",
        "кот-д’івуар": "Cote d'Ivoire",
        "кот д’івуар": "Cote d'Ivoire",
        "гвінея": "Guinea",
        "болівія": "Bolivia",
        "еквадор": "Ecuador",
        "колумбія": "Colombia",
        "парагвай": "Paraguay",
        "перу": "Peru",
    }
    return {normalize_text(k): v for k, v in canon.items()}


def to_canonical_en(country: str, uk_to_en: Dict[str, str], canonical: Dict[str, str]) -> str:
    key = normalize_text(country)
    if key in uk_to_en:
        return canonical.get(normalize_text(uk_to_en[key]), uk_to_en[key])
    if key in canonical:
        return canonical[key]
    if key in {"конго", "congo"}:
        return "Congo (Kinshasa)"
    return country


def cpa_target_for_geo(geo: Optional[str]) -> float:
    try:
        key = str(geo).strip()
    except Exception:
        key = ""
    return CPA_OVERRIDES.get(key, CPA_DEFAULT_TARGET)


# ===================== FLEXIBLE HEADER / COLUMN MATCH =====================

def detect_header_row(df: pd.DataFrame, required: List[str], max_scan: int = 60,
                      scan_rows: Optional[int] = None) -> int:
    # accept either max_scan or scan_rows for backward compatibility
    if scan_rows is not None:
        max_scan = scan_rows
    req_norm = [normalize_text(r) for r in required]
    for i in range(min(max_scan, len(df))):
        row_vals = [normalize_text(x) for x in list(df.iloc[i].values)]
        if all(any(h == v for v in row_vals) for h in req_norm):
            return i
    return 0


def match_columns(actual_cols, required_labels: List[str]) -> Optional[Dict[str, str]]:
    norm_actual = {normalize_text(str(c)): str(c) for c in actual_cols}
    out = {}
    for need in required_labels:
        key = normalize_text(need)
        if key in norm_actual:
            out[need] = norm_actual[key]
            continue
        # punctuation-insensitive
        found = None
        for k, v in norm_actual.items():
            if normalize_text(re.sub(r"[^\w\s'’]", "", k)) == normalize_text(re.sub(r"[^\w\s'’]", "", key)):
                found = v
                break
        if not found:
            return None
        out[need] = found
    return out


# ===================== FILE READERS =====================
# ADD
def read_excel_robust(file_bytes: bytes, sheet_name: str, header: int = 0) -> pd.DataFrame:
    """
    Robust Excel reader with multiple fallback strategies.
    Filters to the current month:
      - Prefer column 'Місяць' (numeric month: 1..12).
      - Fallback to column 'Дата' (dd/mm/YYYY).
    """
    bio = io.BytesIO(file_bytes)
    errors = []

    # Helper: filter to current month
    def filter_current_month(df: pd.DataFrame) -> pd.DataFrame:
        cur_month = datetime.now().month

        if "Місяць" in df.columns:
            # Accept strings like "09", numbers like 9.0, etc.
            month_series = pd.to_numeric(df["Місяць"], errors="coerce")
            return df[month_series == cur_month]

        return df

    # Strategy 1: openpyxl
    try:
        bio.seek(0)
        df = pd.read_excel(bio, sheet_name=sheet_name, header=header, engine="openpyxl")
        return filter_current_month(df)
    except Exception as e:
        errors.append(f"openpyxl: {e}")
        print(f"openpyxl failed: {e}")

    # Strategy 2: openpyxl without header, set manually
    try:
        bio.seek(0)
        df_raw = pd.read_excel(bio, sheet_name=sheet_name, header=None, engine="openpyxl")
        if header > 0:
            new_header = df_raw.iloc[header].tolist()
            df = df_raw.iloc[header + 1:].reset_index(drop=True)
            df.columns = new_header
        else:
            df = df_raw
        return filter_current_month(df)
    except Exception as e:
        errors.append(f"openpyxl manual header: {e}")
        print(f"Manual header setting failed: {e}")

    # Strategy 3: calamine
    try:
        bio.seek(0)
        df = pd.read_excel(bio, sheet_name=sheet_name, header=header, engine="calamine")
        return filter_current_month(df)
    except Exception as e:
        errors.append(f"calamine: {e}")
        print(f"calamine failed: {e}")

    # Strategy 4: xlrd
    try:
        bio.seek(0)
        df = pd.read_excel(bio, sheet_name=sheet_name, header=header, engine="xlrd")
        return filter_current_month(df)
    except Exception as e:
        errors.append(f"xlrd: {e}")
        print(f"xlrd failed: {e}")

    raise ValueError(f"Could not read Excel file with any engine. Errors: {'; '.join(errors)}")


def load_main_budg_table(file_bytes: bytes, filename: str = "uploaded") -> pd.DataFrame:
    df = None
    errors = []

    if filename.lower().endswith((".xlsx", ".xls", ".xlsm")):
        try:
            df = read_excel_robust(file_bytes, sheet_name="BUDG", header=1)
        except Exception as e1:
            errors.append(f"header=1: {e1}")
            # (your existing Excel fallback logic remains unchanged)
    else:
        # CSV support
        bio = io.BytesIO(file_bytes)
        try:
            df_raw = pd.read_csv(bio, header=None)
        except Exception:
            bio.seek(0)
            try:
                df_raw = pd.read_csv(bio, header=None, encoding="cp1251")
            except Exception:
                bio.seek(0)
                df_raw = pd.read_csv(bio, header=None, encoding="utf-8")

        # detect header row (similar to Excel logic)
        header_row = -1
        for i in range(min(10, len(df_raw))):
            row_values = [str(v).lower().strip() for v in df_raw.iloc[i].values]
            if any("назва" in val and "оффер" in val for val in row_values) and \
                    any("гео" in val for val in row_values) and \
                    any("витрат" in val for val in row_values):
                header_row = i
                break

        if header_row >= 0:
            new_header = df_raw.iloc[header_row].tolist()
            df = df_raw.iloc[header_row + 1:].reset_index(drop=True)
            df.columns = new_header
        else:
            df = df_raw

        # --- filter current month ---
        # cur_month = datetime.now().month
        cur_month = 9

        if "Місяць" in df.columns:
            df["Місяць"] = pd.to_numeric(df["Місяць"], errors="coerce")
            df = df[df["Місяць"] == cur_month]
        elif "Дата" in df.columns:
            df["Дата"] = pd.to_datetime(df["Дата"], format="%d/%m/%Y", errors="coerce")
            df = df[(df["Дата"].dt.month == cur_month) & (df["Дата"].dt.year == datetime.now().year)]

    if df is None:
        raise ValueError(f"Could not load BUDG sheet. Errors: {'; '.join(errors)}")

    # clean column names
    df.columns = [str(c).replace("\xa0", " ").replace("\u00A0", " ").strip() for c in df.columns]

    # validate and map columns
    colmap = match_columns(df.columns, ALLOWED_MAIN_COLUMNS)
    if not colmap:
        available = [str(c) for c in df.columns]
        required = ALLOWED_MAIN_COLUMNS
        raise ValueError(
            f"У BUDG мають бути колонки: {required}.\n"
            f"Доступні колонки: {available}\n"
            f"Перевір назви колонок у файлі."
        )

    df = df[[colmap["Назва Офферу"], colmap["ГЕО"], colmap["Загальні витрати"]]].copy()
    df.columns = ["Назва Офферу", "ГЕО", "Загальні витрати"]

    return df


def read_additional_table(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Read additional table with improved error handling
    """
    bio = io.BytesIO(file_bytes)

    if filename.lower().endswith((".xlsx", ".xls", ".xlsm")):
        # Try multiple strategies for Excel files
        try:
            df_raw = pd.read_excel(bio, header=None, engine="openpyxl")
        except Exception:
            try:
                bio.seek(0)
                df_raw = pd.read_excel(bio, header=None, engine="calamine")
            except Exception:
                bio.seek(0)
                df_raw = pd.read_excel(bio, header=None, engine="xlrd")
    else:
        # CSV files
        try:
            df_raw = pd.read_csv(bio, header=None)
        except Exception:
            bio.seek(0)
            try:
                df_raw = pd.read_csv(bio, header=None, encoding="cp1251")
            except Exception:
                bio.seek(0)
                df_raw = pd.read_csv(bio, header=None, encoding="utf-8")

    # Find header row
    header_idx = detect_header_row(df_raw, [normalize_text(x) for x in ADDITIONAL_REQUIRED_COLS])

    # Validate header row exists
    if header_idx >= len(df_raw):
        raise ValueError(f"Не знайдено заголовків у файлі {filename}")

    headers = df_raw.iloc[header_idx].tolist()
    data = df_raw.iloc[header_idx + 1:].reset_index(drop=True)
    data.columns = headers

    # Clean column names
    data.columns = [str(c).replace("\xa0", " ").replace("\u00A0", " ").strip() for c in data.columns]

    col_map = match_columns(data.columns, ADDITIONAL_REQUIRED_COLS)
    if not col_map:
        available = [str(c) for c in data.columns]
        required = ADDITIONAL_REQUIRED_COLS
        raise ValueError(
            f"У файлі {filename} мають бути колонки: {required}.\n"
            f"Доступні колонки: {available}\n"
            f"Перевір назви колонок у файлі."
        )

    df = data[[col_map["Країна"], col_map["Сума депозитів"]]].copy()
    df.columns = ["Країна", "Сума депозитів"]
    return df


# ===================== HELPERS =====================

def inject_formulas_and_cf(
        ws,
        *,
        header_row: int = 1,
        first_data_row: int = 2,
        last_data_row: int | None = None,
):
    """
    Додає формули у колонки та CF-підсвітку.
    Очікується, що у шапці вже є принаймні колонки:
    - 'Total spend' (F), 'FTD qty' (E), 'Total Dep Amount' (K), 'My deposit amount' (L)
    Якщо інших колонок (Total+%, CPA, СP/Ч, Target 40/50%) немає — створимо.

    Формули:
      G: Total+%                 = Total spend * 1.3
      H: CPA                     = Total+% / FTD qty
      (кирилиця) 'СP/Ч'          = Total Dep Amount / FTD qty
      'C. profit Target 40%'     = Total+% * 0.4
      'C. profit Target 50%'     = Total+% * 0.5
      L: My deposit amount       = Total Dep Amount / Total+% * 100

    """

    if last_data_row is None:
        last_data_row = ws.max_row
    if last_data_row < first_data_row:
        return

    # --- Map header name -> column index (створюємо, якщо треба)
    headers = {ws.cell(row=header_row, column=c).value: c for c in range(1, ws.max_column + 1) if
               ws.cell(row=header_row, column=c).value}

    def ensure_col(name: str) -> int:
        if name in headers:
            return headers[name]
        col = ws.max_column + 1
        ws.cell(row=header_row, column=col, value=name)
        headers[name] = col
        return col

    col_idx = {
        "Total spend": ensure_col("Total spend"),
        "FTD qty": ensure_col("FTD qty"),
        "Total Dep Amount": ensure_col("Total Dep Amount"),
        "My deposit amount": ensure_col("My deposit amount"),
        "Total+%": ensure_col("Total+%"),
        "CPA": ensure_col("CPA"),
        "CPA Target": ensure_col("CPA Target"),
        "СP/Ч": ensure_col("СP/Ч"),  # перша літера — кирилична "С"
        "C. profit Target 40%": ensure_col("C. profit Target 40%"),
        "C. profit Target 50%": ensure_col("C. profit Target 50%"),
    }

    # У зручні змінні — кол. літери
    letter = {k: get_column_letter(v) for k, v in col_idx.items()}

    F = letter["Total spend"]
    E = letter["FTD qty"]
    K = letter["Total Dep Amount"]
    L = letter["My deposit amount"]
    G = letter["Total+%"]
    H = letter["CPA"]
    I = letter["CPA Target"]
    CPCH = letter["СP/Ч"]
    C40 = letter["C. profit Target 40%"]
    C50 = letter["C. profit Target 50%"]

    # --- Прописуємо формули по рядках
    for r in range(first_data_row, last_data_row + 1):
        ws[f"{G}{r}"] = f"={F}{r}*1.3"
        ws[f"{H}{r}"] = f"={G}{r}/{E}{r}"
        ws[f"{CPCH}{r}"] = f"={K}{r}/{E}{r}"
        ws[f"{C40}{r}"] = f"={G}{r}*0.4"
        ws[f"{C50}{r}"] = f"={G}{r}*0.5"
        # L як формула (перезаписуємо значення, якщо були)
        ws[f"{L}{r}"] = f"={K}{r}/{G}{r}*100"

    # --- Conditional Formatting (оновлені правила) ---
    first_col_letter = get_column_letter(1)
    last_col_letter = get_column_letter(ws.max_column)
    data_range = f"{first_col_letter}{first_data_row}:{last_col_letter}{last_data_row}"

    grey = PatternFill(start_color="BFBFBF", end_color="BFBFBF", fill_type="solid")
    green = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    yellow = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
    red = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

    # Dynamic threshold per GEO (Габон=59, else 39)
    try:
        GEO_col_idx = headers.get("ГЕО") or headers.get("Geo") or headers.get("GEO") or headers.get("Країна")
        GEO = get_column_letter(GEO_col_idx) if GEO_col_idx else None
    except Exception:
        GEO = None
    THR = f'IF(${GEO}{first_data_row}="Габон",59,39)' if GEO else "39"

    # Grey: E = 0
    ws.conditional_formatting.add(
        data_range,
        FormulaRule(formula=[f"${E}{first_data_row}=0"], fill=grey, stopIfTrue=True),
    )

    # Green: INT(H) <= INT(I) AND L > 39
    ws.conditional_formatting.add(
        data_range,
        FormulaRule(
            formula=[
                f"AND(${E}{first_data_row}>0,INT(${H}{first_data_row})<=INT(${I}{first_data_row}),${L}{first_data_row}>{THR})"],
            fill=green,
            stopIfTrue=True
        ),
    )

    # Yellow: (INT(H) <= INT(I)) OR (L > 39 AND H < I*1.31)
    ws.conditional_formatting.add(
        data_range,
        FormulaRule(
            formula=[
                f"AND(${E}{first_data_row}>0,OR(INT(${H}{first_data_row})<=INT(${I}{first_data_row}),AND(${L}{first_data_row}>{THR},${H}{first_data_row}<${I}{first_data_row}*1.31)))"],
            fill=yellow,
            stopIfTrue=True
        ),
    )

    # Red: (E > 0 AND H > I*1.3 AND L > 39) OR (E > 0 AND INT(H) > INT(I) AND L < 39)
    ws.conditional_formatting.add(
        data_range,
        FormulaRule(
            formula=[
                f"OR(AND(${E}{first_data_row}>0,${H}{first_data_row}>${I}{first_data_row}*1.3,${L}{first_data_row}>{THR}),AND(${E}{first_data_row}>0,INT(${H}{first_data_row})>INT(${I}{first_data_row}),${L}{first_data_row}<{THR}))"],
            fill=red,
            stopIfTrue=True
        ),
    )

    # --- Number format: 2 decimals for all numeric cells in data range
    # (краще з розділювачем тисяч)
    num_fmt = "#,##0.00"
    for row in ws.iter_rows(
            min_row=first_data_row,
            max_row=last_data_row,
            min_col=1,
            max_col=ws.max_column
    ):
        for cell in row:
            cell.number_format = num_fmt


def ask_additional_table_with_skip(message: types.Message, state: UserState):
    offer = state.offers[state.current_offer_index]
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Пропустити цей офер", callback_data="skip_offer"))
    bot.send_message(
        message.chat.id,
        (
            f"Надішліть додаткову таблицю для оферу:\n"
            f"<b>{offer}</b>\n\n"
            "Очікувані колонки: <b>Країна</b>, <b>Сума депозитів</b>.\n"
            "Або натисніть «Пропустити цей офер», щоб не включати його у фінальний звіт."
        ),
        reply_markup=kb,
    )


# ===================== ALLOCATION HELPERS =====================

def _classify_status(E: float, F: float, K: float) -> str:
    if E <= 0:
        return "Grey"
    H = 1.3 * F / E if E else float("inf")
    L = (100.0 * K) / (1.3 * F) if F else float("inf")

    # Green: E>0 and H<=9 and L>39.99
    green = (H <= H_THRESH + EPS) and (L > L_THRESH + EPS)
    if green:
        return "Green"

    # Yellow: E>0 and ((H<=9) or (L>39.99)) and not Green
    yellow = (H <= H_THRESH + EPS) or (L > L_THRESH + EPS)
    return "Yellow" if yellow else "Red"  # Red: H>9 and L<=39.99


def _fmt(v: float, suf: str = "", nan_text: str = "-") -> str:
    if not np.isfinite(v):
        return nan_text
    return f"{v:.2f}{suf}"


def build_allocation_explanation(df_source: pd.DataFrame,
                                 alloc_vec: pd.Series,
                                 budget: float,
                                 max_lines: int = 20) -> str:
    """
    Створює текстовий звіт:
      - скільки бюджету використано і залишок
      - скільки рядків змінили статус, скільки жовтих у підсумку
      - список топ-рядків з алокацією (Offer ID / Назва / ГЕО, +сума, нові H і L, статус: ДО → ПІСЛЯ)

    max_lines — обмеження на кількість детальних рядків у списку (щоб не перевантажувати чат).
    """
    df = df_source.copy()
    # нормалізуємо назви колонок на всяк випадок
    df.columns = [str(c).replace("\xa0", " ").replace("\u00A0", " ").strip() for c in df.columns]

    # Витягуємо потрібні колонки з дефолтами, якщо відсутні
    subid = df.get("Subid", pd.Series([""] * len(df)))
    offer = df.get("Offer ID", df.get("Назва Офферу", pd.Series([""] * len(df))))
    name = df.get("Назва Офферу", pd.Series([""] * len(df)))
    geo = df.get("ГЕО", pd.Series([""] * len(df)))
    E = pd.to_numeric(df.get("FTD qty", 0), errors="coerce").fillna(0.0)
    F = pd.to_numeric(df.get("Total spend", 0), errors="coerce").fillna(0.0)
    K = pd.to_numeric(df.get("Total Dep Amount", 0), errors="coerce").fillna(0.0)

    alloc = pd.to_numeric(alloc_vec, errors="coerce").reindex(df.index).fillna(0.0)
    F_new = (F + alloc)

    # Статуси ДО/ПІСЛЯ
    before = [_classify_status(float(E[i]), float(F[i]), float(K[i])) for i in df.index]
    after = [_classify_status(float(E[i]), float(F_new[i]), float(K[i])) for i in df.index]

    # Метрики
    total_budget = float(budget)
    used = float(alloc.sum())
    left = max(0.0, total_budget - used)

    yellow_before = sum(1 for s in before if s == "Yellow")
    yellow_after = sum(1 for s in after if s == "Yellow")
    green_to_yellow = sum(1 for i in df.index if (before[i] == "Green" and after[i] == "Yellow"))

    # Побудова списку рядків з алокацією
    rows = []
    for i in alloc.index:
        if alloc[i] <= 0:
            continue

        Ei = float(E[i]);
        Fi = float(F[i]);
        Ki = float(K[i]);
        Fni = float(F_new[i])

        # ДО
        H_before = (1.3 * Fi / Ei) if Ei > 0 else float("inf")
        L_before = (100.0 * Ki) / (1.3 * Fi) if Fi > 0 else float("inf")

        # ПІСЛЯ
        H_after = (1.3 * Fni / Ei) if Ei > 0 else float("inf")
        L_after = (100.0 * Ki) / (1.3 * Fni) if Fni > 0 else float("inf")

        line = (
            f"- {str(offer[i]) or ''} / {str(name[i]) or ''} / {str(geo[i]) or ''}: "
            f"+{alloc[i]:.2f} → Total Spend {Fi:.2f}→{Fni:.2f}; "
            f"CPA {_fmt(H_before)}→{_fmt(H_after)}, "
            f"My deposit amount {_fmt(L_before, '%')}→{_fmt(L_after, '%')} | "
            f"{before[i]} → {after[i]}"
        )
        rows.append((alloc[i], line))

    # Сортуємо за найбільшою алокацією і обрізаємо
    rows.sort(key=lambda x: (-x[0], x[1]))
    detail_lines = [ln for _, ln in rows[:max_lines]]

    header = (
        f"Розподіл бюджету: {used:.2f} / {total_budget:.2f} використано; залишок {left:.2f}\n"
        f"Жовтих ДО/ПІСЛЯ: {yellow_before} → {yellow_after} (зел.→жовт.: {green_to_yellow})"
    )

    if not detail_lines:
        return header + "\n\n(Алокації по рядках відсутні — бюджет не було куди розподілити за правилами.)"

    return header + "\n\nТоп розподілів:\n" + "\n".join(detail_lines) + \
        ("\n\n…Список обрізано." if len(rows) > max_lines else "")


def allocate_with_openai(df: pd.DataFrame, rules_text: str, model: str | None = None) -> pd.DataFrame:
    """
    Надсилає таблицю і правила в OpenAI, отримує назад оновлену таблицю.
    Модель має ПОВЕРНУТИ CSV з тими ж колонками + колонка NEW SPEND (або оновити target-колонку).
    """
    if df.empty:
        raise ValueError("Пуста таблиця для алокації.")

    model = model or OPENAI_MODEL
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # 1) Ділимо великий DF на чанки
    chunks = _split_df_by_size(df)

    updated_chunks: list[pd.DataFrame] = []

    for idx, chunk in enumerate(chunks, start=1):
        csv_in = _df_to_csv(chunk)

        system_msg = (
            "You are a meticulous data allocator. "
            "Follow the given allocation rules exactly. "
            "Return ONLY a CSV table with the SAME columns as input, "
            f"and include a numeric column '{OPENAI_OUTPUT_COLUMN}' with the recomputed spend per row. "
            "Do not add extra commentary. Use dot as decimal separator."
        )

        user_msg = (
            "Rules:\n"
            f"{rules_text}\n\n"
            "Instructions:\n"
            f"- Input table is a CSV.\n"
            f"- Keep all original columns unchanged.\n"
            f"- Add or overwrite a column named '{OPENAI_OUTPUT_COLUMN}' with the new per-row allocation (number only).\n"
            "- Return ONLY the CSV (no explanations).\n\n"
            "Input CSV:\n"
            f"{csv_in}"
        )

        resp = client.chat.completions.create(
            model=model,
            temperature=0.0,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
        )

        content = resp.choices[0].message.content or ""
        csv_out = _csv_from_text(content)

        # читаємо назад у DF
        try:
            out_df = pd.read_csv(io.StringIO(csv_out))
        except Exception as e:
            raise RuntimeError(f"Не вдалося розпарсити CSV від моделі на чанку {idx}: {e}")

        # базова валідація
        missing_cols = [c for c in chunk.columns if c not in out_df.columns]
        if missing_cols:
            raise RuntimeError(f"Модель не повернула всі колонки (чанк {idx}). Відсутні: {missing_cols}")

        if OPENAI_OUTPUT_COLUMN not in out_df.columns:
            raise RuntimeError(f"Модель не повернула колонку '{OPENAI_OUTPUT_COLUMN}' (чанк {idx}).")

        # Зберігаємо порядок рядків: приєднаємо по індексу
        # (очікується той самий порядок — але на всякий випадок приведемо довжини)
        if len(out_df) != len(chunk):
            # як fallback — підрізаємо/доповнювати не будемо; вважаємо помилкою
            raise RuntimeError(
                f"Розмір чанка змінився (очікував {len(chunk)}, отримав {len(out_df)}) на чанку {idx}."
            )

        # беремо тільки колонку з новими значеннями і змерджимо
        chunk[OPENAI_OUTPUT_COLUMN] = out_df[OPENAI_OUTPUT_COLUMN].values
        updated_chunks.append(chunk)

    # збираємо назад
    result = pd.concat(updated_chunks, axis=0)
    result.reset_index(drop=True, inplace=True)
    return result


def allocate_total_spend_alternative(
        df: pd.DataFrame,
        *,
        col_total_spend: str = "Total spend",      # F
        col_ftd_qty: str = "FTD qty",              # E
        col_cpa_target: str = "CPA Target",        # I
        col_my_deposit: str = "My deposit amount", # L (перезаписуємо формулою)
        col_total_dep_amount: str = "Total Dep Amount",  # K
        col_geo: str = "ГЕО",
        in_place: bool = False,
        round_decimals: Optional[int] = 2,
        excel_path: Optional[str] = None,
        sheet_name: str = "Result",
        header_row: int = 1,  # шапка в першому рядку
) -> pd.DataFrame:
    """
    Pass#1: роздаємо під "жовтий" множник (за L із вхідного df).
    Після Pass#1: перераховуємо L = (K / (F*1.3)) * 100, знаходимо рядки з L>THR,
      де THR = 59 для Габону і 39 для інших,
      рахуємо дві межі для F:
        F_cap_deposit = (K/THR*100) * (100/130)
        F_cap_cpa     = (E*I*1.3) * (100/130)
      target_F = min(двох меж).
      Якщо F < target_F — піднімаємо F у межах доступного бюджету.
      Якщо F > target_F — зменшуємо F і повертаємо різницю в бюджет.
    Pass#2: якщо ще є бюджет — піднімаємо F до "червоної" стелі: F_red = (E*I*1.8)*(100/130)
    """

    work = df if in_place else df.copy()

    # Перевірка колонок
    for c in (col_total_spend, col_ftd_qty, col_cpa_target, col_my_deposit, col_total_dep_amount):
        if c not in work.columns:
            raise KeyError(f"Відсутня колонка: {c}")
    if col_geo not in work.columns:
        raise KeyError(f"Відсутня колонка з ГЕО: {col_geo}")

    # Приведення типів
    for c in (col_total_spend, col_ftd_qty, col_cpa_target, col_my_deposit, col_total_dep_amount):
        work[c] = pd.to_numeric(work[c], errors="coerce").fillna(0.0)

    # Нормалізоване ГЕО і поріг L: 59 для Габону, 39 для інших
    geo_norm = work[col_geo].astype(str).str.strip().fillna("")
    L_threshold_series = np.where(geo_norm.eq("Габон"), 59.0, 39.0)

    # Кліпи
    work[col_ftd_qty] = work[col_ftd_qty].clip(lower=0)
    work[col_cpa_target] = work[col_cpa_target].clip(lower=0)
    work[col_my_deposit] = work[col_my_deposit].clip(lower=0)
    work[col_total_dep_amount] = work[col_total_dep_amount].clip(lower=0)

    mask_take = work[col_ftd_qty] > 0

    # Бюджет = сума старих F по E>0
    budget = float(work.loc[mask_take, col_total_spend].sum())
    if budget < 0:
        budget = 0.0
    print("Initial budget:", budget)

    # Обнуляємо F
    work[col_total_spend] = 0.0

    # Коефіцієнти
    CONV = 100.0 / 130.0  # == 1/1.3
    MULT_Y_HIGH = 1.3     # для L>=THR у Pass#1
    MULT_Y_LOW  = 1.1     # для L<THR  у Pass#1
    MULT_CPA_Y  = 1.3     # "жовта" CPA межа
    MULT_RED    = 1.8     # "червона" стеля для Pass#2

    # Використаємо L із ВХІДНОГО df для вибору множника в Pass#1
    L_for_threshold = pd.to_numeric(df[col_my_deposit], errors="coerce").fillna(0.0).clip(lower=0)

    # -------- Pass#1: до "жовтого" --------
    idx_pass1 = work.loc[mask_take].sort_values(by=col_ftd_qty, ascending=True).index

    for i in idx_pass1:
        if budget <= 0:
            break
        E = float(work.at[i, col_ftd_qty])
        I = float(work.at[i, col_cpa_target])
        Lthr_val = float(L_for_threshold.at[i])
        thr_i = float(L_threshold_series[work.index.get_loc(i)])  # 59 для Габону, 39 інакше

        mult = MULT_Y_HIGH if Lthr_val >= thr_i else MULT_Y_LOW
        target_F = E * I * mult * CONV  # == (E * I * mult)/1.3

        alloc = min(target_F, budget)
        work.at[i, col_total_spend] = alloc
        budget -= alloc

    # -------- Перерахунок L та "підняття/зменшення" F для L>THR --------
    # L_now = (K / (F*1.3)) * 100
    G_now = work[col_total_spend] * 1.3
    with np.errstate(divide='ignore', invalid='ignore'):
        L_now = np.where(G_now > 0, (work[col_total_dep_amount] / G_now) * 100.0, np.inf)
    work[col_my_deposit] = L_now  # записати для наглядності

    # Маска коригування: E>0 і L_now > THR(geo)
    mask_adjust = mask_take.values & (L_now > L_threshold_series)

    if mask_adjust.any():
        # Депозитна верхня межа: F_cap_dep = (K/THR*100) * CONV
        F_cap_dep = (work[col_total_dep_amount].values / L_threshold_series * 100.0) * CONV
        # CPA межа: F_cap_cpa = (E * I * 1.3) * CONV
        F_cap_cpa = (work[col_ftd_qty].values * work[col_cpa_target].values * MULT_CPA_Y) * CONV

        # Цільовий F — мінімум двох верхніх меж
        F_target = np.minimum(F_cap_dep, F_cap_cpa)

        curF = work[col_total_spend].values
        tgtF = F_target
        adj  = mask_adjust

        delta = np.zeros_like(curF, dtype=float)

        # Потрібно підняти (consume budget)
        need_up_mask = adj & (tgtF > curF)
        delta_up = tgtF - curF
        need_up_total = float(delta_up[need_up_mask].sum())

        if need_up_total > 0 and budget > 0:
            # Якщо бюджету мало — піднімаємо пропорційно
            ratio = min(1.0, budget / need_up_total)
            inc = np.zeros_like(curF, dtype=float)
            inc[need_up_mask] = delta_up[need_up_mask] * ratio
            curF += inc
            budget -= float(inc.sum())

        # Потрібно зменшити (free budget)
        need_down_mask = adj & (tgtF < curF)
        freed = float((curF[need_down_mask] - tgtF[need_down_mask]).sum())
        if freed > 0:
            curF[need_down_mask] = tgtF[need_down_mask]
            budget += freed

        work[col_total_spend] = curF
        print(f"[Adjust L>THR] budget after adjust: {budget:.2f}")

        # Оновимо L після зміни F (щоб бачити актуальні значення)
        G_now = work[col_total_spend] * 1.3
        with np.errstate(divide='ignore', invalid='ignore'):
            L_now = np.where(G_now > 0, (work[col_total_dep_amount] / G_now) * 100.0, np.inf)
        work[col_my_deposit] = L_now

    # -------- Pass#2: до "червоної" стелі --------
    if budget > 0:
        idx_pass2 = work.loc[mask_take].sort_values(by=col_total_spend, ascending=True).index
        for i in idx_pass2:
            if budget <= 0:
                break
            E = float(work.at[i, col_ftd_qty])
            I = float(work.at[i, col_cpa_target])

            red_cap = E * I * MULT_RED * CONV  # верхня межа F
            cur_F = float(work.at[i, col_total_spend])

            need = max(0.0, red_cap - cur_F)
            if need <= 0:
                continue

            add = min(need, budget)
            work.at[i, col_total_spend] = cur_F + add
            budget -= add

    # Округлення
    if round_decimals is not None:
        work[col_total_spend] = work[col_total_spend].round(round_decimals)

    if excel_path:
        # Запис у файл/аркуш
        with ExcelWriter(excel_path, engine="openpyxl", mode="w") as writer:
            work.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.book[sheet_name]
            first_data_row = header_row + 1
            last_data_row = header_row + len(work)
            inject_formulas_and_cf(
                ws,
                header_row=header_row,
                first_data_row=first_data_row,
                last_data_row=last_data_row,
            )
            writer._save()

    return work


def compute_optimal_allocation(df: pd.DataFrame, budget: float) -> Tuple[pd.DataFrame, str, pd.Series]:
    """
    Нова послідовність:
      A) Спочатку намагаємось мінімально перевести GREEN -> YELLOW (дотримуючись CPA<=CPA_CAP).
      B) Якщо залишився бюджет — насичуємо YELLOW максимально, але так, щоб вони залишались YELLOW (і CPA<=CPA_CAP).

    Позначення:
      E = FTD qty
      F = Total spend
      K = Total Dep Amount

    Межі/похідні:
      F_at_H = H_THRESH * E / 1.3
      F_at_L = (100 * K) / (1.3 * L_THRESH)  # L == L_THRESH при такому F
      F_cap  = CPA_CAP * E / 1.3
    """
    dfw = df.copy()

    # Числові колонки
    E = pd.to_numeric(dfw["FTD qty"], errors="coerce").fillna(0.0)
    F = pd.to_numeric(dfw["Total spend"], errors="coerce").fillna(0.0)
    K = pd.to_numeric(dfw["Total Dep Amount"], errors="coerce").fillna(0.0)

    # Поточні H, L
    with np.errstate(divide='ignore', invalid='ignore'):
        H = 1.3 * F / E.replace(0, np.nan)
        L = 100.0 * K / (1.3 * F.replace(0, np.nan))

    # Межі
    F_at_H = H_THRESH * E / 1.3
    F_at_L = (100.0 * K) / (1.3 * L_THRESH)
    F_cap = CPA_CAP * E / 1.3

    # Маски статусів (строго відповідно до правил/Excel)
    grey_mask = (E <= 0)
    green_mask = (~grey_mask) & (H <= H_THRESH + EPS) & (L > L_THRESH + EPS)
    yellow_mask = (~grey_mask) & ((H <= H_THRESH + EPS) | (L > L_THRESH + EPS)) & (~green_mask)
    # red_mask   = (~grey_mask) & (~green_mask) & (~yellow_mask)  # не потрібен явно

    alloc = pd.Series(0.0, index=dfw.index, dtype=float)
    rem = float(budget) if budget and budget > 0 else 0.0

    # -------------------------------
    # A) GREEN -> YELLOW (мінімальний spend)
    # -------------------------------
    # Кандидати цільових F:
    #   - перетнути межу H: F_cross_H = F_at_H + EPS_YEL (робить H трохи > H_THRESH)
    #   - перетнути межу L: F_cross_L = F_at_L + EPS_YEL (робить L трохи < L_THRESH)
    F_cross_H = F_at_H + EPS_YEL
    F_cross_L = F_at_L + EPS_YEL

    # Мінімальний F, який зламав "зеленість", але не робить рядок "червоним" і не перевищує CPA cap.
    candidates = pd.DataFrame({
        "F_now": F,
        "F_cap": F_cap,
        "F_cross_H": F_cross_H,
        "F_cross_L": F_cross_L,
        "E": E,
        "K": K
    })

    # Для кожного green обчислюємо найменшу допустиму ціль F_target
    F_target = F.copy()

    for i in candidates[green_mask].index:
        Fi = float(candidates.at[i, "F_now"])
        Fcap = float(candidates.at[i, "F_cap"])
        Fh = float(candidates.at[i, "F_cross_H"])
        Fl = float(candidates.at[i, "F_cross_L"])
        Ei = float(E.at[i])
        Ki = float(K.at[i])

        # Обидва потенційні цілі в межах CPA?
        options = []
        for Ft in (Fh, Fl):
            if np.isfinite(Ft) and Ft > Fi + EPS and Ft <= Fcap + EPS:
                # Перевіримо, що Ft не робить рядок "червоним"
                Ht = 1.3 * Ft / Ei if Ei > 0 else float("inf")
                Lt = (100.0 * Ki) / (1.3 * Ft) if Ft > 0 else float("inf")
                is_red = (Ht > H_THRESH + EPS) and (Lt <= L_THRESH + EPS)
                if not is_red:
                    options.append(Ft)

        if options:
            F_target.at[i] = min(options)  # найменша ціна переходу
        else:
            # неможливо легально зробити жовтим — залишаємо як є
            F_target.at[i] = Fi

    need_delta = (F_target - F).clip(lower=0.0)

    # Розподіл: за зростанням потрібної дельти
    for i in need_delta[green_mask].sort_values(ascending=True).index:
        if rem <= 1e-9:
            break
        need = float(need_delta.at[i])
        if need <= 0:
            continue
        take = min(rem, need)
        alloc.at[i] += take
        rem -= take

    # -------------------------------
    # B) Насичення YELLOW, щоб лишались YELLOW (CPA<=cap)
    # -------------------------------
    if rem > 1e-9:
        # Перерахувати F після кроку A
        F_mid = F + alloc
        with np.errstate(divide='ignore', invalid='ignore'):
            H_mid = 1.3 * F_mid / E.replace(0, np.nan)
            L_mid = 100.0 * K / (1.3 * F_mid.replace(0, np.nan))

        # Ті, хто зараз жовті (включно з новими з кроку A)
        is_green_mid = (~(E <= 0)) & (H_mid <= H_THRESH + EPS) & (L_mid > L_THRESH + EPS)
        is_yellow_mid = (~(E <= 0)) & (((H_mid <= H_THRESH + EPS) | (L_mid > L_THRESH + EPS)) & (~is_green_mid))

        # Межа "залишитись жовтим": до max(F_at_H, F_at_L - EPS_YEL), та ще й не перевищити cap
        F_yellow_limit_base = pd.Series(np.maximum(F_at_H, F_at_L - EPS_YEL), index=dfw.index)
        F_yellow_limit_final = pd.Series(np.minimum(F_yellow_limit_base, F_cap), index=dfw.index).fillna(0.0)

        headroom = (F_yellow_limit_final - F_mid).clip(lower=0.0)

        # Greedy за спаданням headroom
        for i in headroom[is_yellow_mid].sort_values(ascending=False).index:
            if rem <= 1e-9:
                break
            give = float(min(rem, headroom.at[i]))
            if give <= 0:
                continue
            alloc.at[i] += give
            rem -= give

    # ПІДСУМОК
    F_final = F + alloc
    with np.errstate(divide='ignore', invalid='ignore'):
        H_final = 1.3 * F_final / E.replace(0, np.nan)
        L_final = 100.0 * K / (1.3 * F_final.replace(0, np.nan))

    still_green = (E > 0) & (H_final <= H_THRESH + EPS) & (L_final > L_THRESH + EPS)
    still_yellow = (E > 0) & (((H_final <= H_THRESH + EPS) | (L_final > L_THRESH + EPS)) & (~still_green))

    kept_yellow = int(still_yellow.sum())
    total_posE = int((E > 0).sum())

    dfw["Allocated extra"] = alloc
    dfw["New Total spend"] = F_final
    dfw["Will be yellow"] = ["Yes" if x else "No" for x in still_yellow]

    summary = (
        f"Бюджет: {budget:.2f}\n"
        f"Жовтих після розподілу: {kept_yellow}/{total_posE}\n"
        f"Правила: спочатку переводимо зелені в жовті мінімальним spend (CPA≤{CPA_CAP:g}), "
        f"потім насичуємо жовті в межах жовтого (H≤{H_THRESH:g} або L>{L_THRESH:.2f}, CPA≤{CPA_CAP:g})."
    )
    return dfw, summary, alloc


def write_result_like_excel_with_new_spend(bio: io.BytesIO, df_source: pd.DataFrame, new_total_spend: pd.Series):
    """
    Build an Excel sheet identical to result.xlsx structure:
    Columns (A..P):
      A Subid | B Offer ID | C Назва Офферу | D ГЕО | E FTD qty | F Total spend | G Total+% | H CPA | I CPA Target |
      J СP/Ч | K Total Dep Amount | L My deposit amount | M C. profit Target 40% | N C. profit Target 50% | O CAP | P Остаток CAP
    Uses new_total_spend for column F, then writes the same formulas, formats and conditional formatting.
    """
    final_cols = [
        "Subid", "Offer ID", "Назва Офферу", "ГЕО",
        "FTD qty", "Total spend", "Total+%", "CPA", "CPA Target", "СP/Ч",
        "Total Dep Amount", "My deposit amount", "C. profit Target 40%", "C. profit Target 50%",
        "CAP", "Остаток CAP"
    ]

    # Start from source, ensure all columns exist
    df_out = df_source.copy()
    # normalize column names just in case
    df_out.columns = [str(c).replace("\xa0", " ").replace("\u00A0", " ").strip() for c in df_out.columns]

    # Coerce numbers
    df_out["FTD qty"] = pd.to_numeric(df_out.get("FTD qty", 0), errors="coerce").fillna(0)
    df_out["Total spend"] = pd.to_numeric(df_out.get("Total spend", 0), errors="coerce").fillna(0.0)
    df_out["Total Dep Amount"] = pd.to_numeric(df_out.get("Total Dep Amount", 0.0), errors="coerce").fillna(0.0)

    # Apply new spend
    # align by index; if shapes don't match, reindex new_total_spend to df_out
    new_total_spend = pd.to_numeric(new_total_spend, errors="coerce").fillna(0.0)
    new_total_spend = new_total_spend.reindex(df_out.index).fillna(0.0)
    df_out["Total spend"] = (df_out["Total spend"] + new_total_spend).round(2)

    # Ensure missing columns exist as blanks
    for col in final_cols:
        if col not in df_out.columns:
            df_out[col] = ""

    # Reorder
    df_out = df_out[final_cols].copy()

    # Round numeric display columns
    df_out["FTD qty"] = pd.to_numeric(df_out["FTD qty"], errors="coerce").fillna(0).astype(int)
    for col in ["Total spend", "Total Dep Amount"]:
        df_out[col] = pd.to_numeric(df_out[col], errors="coerce").round(2)

    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Result")
        wb = writer.book
        ws = writer.sheets["Result"]

        from openpyxl.styles import PatternFill, Alignment, Font
        from openpyxl.formatting.rule import FormulaRule

        first_row = 2
        last_row = ws.max_row

        # Re-insert formulas (same as send_final_table)
        for r in range(first_row, last_row + 1):
            ws[f"G{r}"].value = f"=F{r}*1.3"  # Total+%
            ws[f"H{r}"].value = f"=IFERROR(G{r}/E{r},\"\")"  # CPA
            ws[f"I{r}"].value = cpa_target_for_geo(ws[f"D{r}"].value)  # CPA Target
            ws[f"J{r}"].value = f"=IFERROR(K{r}/E{r},\"\")"  # СP/Ч
            ws[f"L{r}"].value = f"=IFERROR(K{r}/G{r}*100,0)"  # My deposit amount
            ws[f"M{r}"].value = f"=G{r}*0.4"  # Profit 40%
            ws[f"N{r}"].value = f"=G{r}*0.5"  # Profit 50%

        # Header styling
        for col in range(1, 17):  # A..P
            ws.cell(row=1, column=col).font = Font(bold=True)
            ws.cell(row=1, column=col).alignment = Alignment(horizontal="center")

        # Widths
        widths = {"A": 10, "B": 12, "C": 22, "D": 16, "E": 10, "F": 14, "G": 12, "H": 10, "I": 12, "J": 10, "K": 16,
                  "L": 18, "M": 18, "N": 18, "O": 12, "P": 16}
        for col, w in widths.items():
            ws.column_dimensions[col].width = w

        # Number formats (2 decimals for F,G,H,J,K,L,M,N; E as integer)
        for r in range(first_row, last_row + 1):
            ws[f"E{r}"].number_format = "0"
        two_dec_cols = ["F", "G", "H", "J", "K", "L", "M", "N"]
        for col in two_dec_cols:
            for r in range(first_row, last_row + 1):
                ws[f"{col}{r}"].number_format = "0.00"

        # Conditional formatting — SAME rules/colors
        data_range = f"A{first_row}:P{last_row}"
        grey = PatternFill("solid", fgColor="BFBFBF")
        green = PatternFill("solid", fgColor="C6EFCE")
        yellow = PatternFill("solid", fgColor="FFEB9C")
        red = PatternFill("solid", fgColor="FFC7CE")

        THR2 = 'IF($D2="Габон",59,39)'

        # Grey: E = 0
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=["$E2=0"], fill=grey, stopIfTrue=True)
        )

        # Green: INT(H) <= INT(I) AND L > 39
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=[f"AND($E2>0,INT($H2)<=INT($I2),$L2>{THR2})"], fill=green, stopIfTrue=True)
        )

        # Yellow: (INT(H) <= INT(I)) OR (L > 39 AND H < I*1.31)
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=[f"AND($E2>0,OR(INT($H2)<=INT($I2),AND($L2>{THR2},$H2<$I2*1.31)))"], fill=yellow,
                        stopIfTrue=True)
        )

        # Red: (E > 0 AND H > I*1.3 AND L > 39) OR (E > 0 AND INT(H) > INT(I) AND L < 39)
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(
                formula=[f"OR(AND($E2>0,$H2>$I2*1.3,$L2>{THR2}),AND($E2>0,INT($H2)>INT($I2),$L2<{THR2}))"],
                fill=red,
                stopIfTrue=True
            )
        )


# ===================== BOT HANDLERS =====================

@bot.message_handler(commands=["start", "help"])
@require_access
def start(message: types.Message):
    chat_id = message.chat.id
    user_states[chat_id] = UserState()
    bot.reply_to(
        message,
        (
            "Привіт! 👋\n\n"
            "1) Надішли <b>головну таблицю</b> (CSV/XLSX) — аркуш <b>BUDG</b> з колонками: <b>Назва Офферу</b>, <b>ГЕО</b>, <b>Загальні витрати</b>.\n"
            "2) Бот підсумує витрати по унікальних парах <b>Offer ID+ГЕО</b> і визначить список унікальних <b>Назв Офферу</b>.\n"
            "3) Потім НА КОЖНУ <b>Назву Офферу</b> надішли одну додаткову таблицю (в ній є всі країни для цього офера) з колонками: <b>Країна</b>, <b>Сума депозитів</b>.\n"
            "4) Фінал: Excel з колонками: Назва Офферу, ГЕО, Total Spend, Total Dep Sum, Total Dep Amount.\n"
            "Надішли зараз головну таблицю."
        ),
    )


@bot.message_handler(content_types=["document"])
@require_access
def on_document(message: types.Message):
    chat_id = message.chat.id
    state = user_states.setdefault(chat_id, UserState())

    try:
        file_info = bot.get_file(message.document.file_id)
        file_bytes = bot.download_file(file_info.file_path)
        filename = message.document.file_name or "uploaded"
    except Exception as e:
        bot.reply_to(message, f"Не вдалося отримати файл: <code>{e}</code>", parse_mode="HTML")
        return

    try:
        if state.phase == "WAIT_MAIN":
            df = load_main_budg_table(file_bytes, filename=filename)
            bot.reply_to(message, "✅ Головна таблиця завантажена! Тепер надішліть додаткові таблиці.")
            handle_main_table(message, state, df)

        elif state.phase == "WAIT_ADDITIONAL":
            df = read_additional_table(file_bytes, filename)
            handle_additional_table(message, state, df)

        elif state.phase == "WAIT_ALLOC_RESULT":
            # читаємо result.xlsx
            bio = io.BytesIO(file_bytes)
            try:
                df_res = pd.read_excel(bio, sheet_name="Result", engine="openpyxl")
            except Exception:
                bio.seek(0)
                df_res = pd.read_excel(bio, engine="openpyxl")

            # нормалізуємо назви
            df_res.columns = [str(c).replace("\xa0", " ").replace("\u00A0", " ").strip() for c in df_res.columns]

            # мінімальний набір, від якого рахуємо
            required_cols = ["FTD qty", "Total spend", "Total Dep Amount"]
            missing = [c for c in required_cols if c not in df_res.columns]
            if missing:
                raise ValueError("У result.xlsx бракує колонок: " + ", ".join(missing))

            # числа
            for num_col in ["FTD qty", "Total spend", "Total Dep Amount"]:
                df_res[num_col] = pd.to_numeric(df_res[num_col], errors="coerce").fillna(0)

            # --- ГІЛКА ДЛЯ OpenAI: без бюджету ---
            if getattr(state, "alloc_mode", None) == "openai":
                try:
                    base_rules = globals().get("OPENAI_RULES", """
                        1) Обчисли колонку 'New Spend' для кожного рядка за правилами алокації:
                           - Якщо FTD=0 — New Spend=0.
                           - Жовтий поріг: FTD * CPA_target * 1.3 (якщо є відповідні поля).
                           - Якщо доступні MyDeposit і Total+% — додаткове обмеження: MyDeposit * 100 / (Total+%).
                           - Якщо після роздачі по жовтому лишається бюджет — дозалий до червоного порогу: FTD * CPA_target * 1.8 у порядку зростання Total+%.
                        2) Не змінюй інші колонки. Поверни той самий набір колонок + 'New Spend'.
                        3) Відповідь — ТІЛЬКИ CSV без пояснень. Десятковий роздільник — крапка.
                    """).strip()

                    # якщо хочеш, можеш передавати TOTAL_BUDGET тут теж — або взагалі не передавати
                    rules_text = base_rules

                    out_df = allocate_with_openai(df_res, rules_text)

                    out = io.BytesIO()
                    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
                        out_df.to_excel(writer, index=False, sheet_name="Result")
                    out.seek(0)

                    bot.send_document(
                        chat_id,
                        out,
                        visible_file_name="allocation_openai.xlsx",
                        caption="Готово: алокація через OpenAI"
                    )
                except Exception as e:
                    bot.send_message(chat_id, f"⚠️ Помилка алокації через OpenAI: <code>{e}</code>", parse_mode="HTML")
                finally:
                    state.phase = "WAIT_MAIN"
                return

            # --- ЛОКАЛЬНІ режими: просимо бюджет ---
            if state.alloc_mode == "alternative":
                out_df = allocate_total_spend_alternative(
                    df_res,
                    col_total_spend="Total spend",
                    col_ftd_qty="FTD qty",
                    col_cpa_target="CPA Target",
                    col_my_deposit="My deposit amount",
                    col_total_dep_amount="Total Dep Amount",
                    excel_path="Result.xlsx",  # файл з формулами + CF
                    sheet_name="Result",
                    header_row=1,
                )

                # ⬇️ Надсилаємо саме файл, збережений вище
                try:
                    with open("Result.xlsx", "rb") as f:
                        bot.send_document(
                            chat_id,
                            f,
                            visible_file_name="allocation_alternative.xlsx",  # назву можеш лишити як хочеш
                            caption="Готово: алокація за альтернативним режимом (з формулами та підсвіткою)"
                        )
                except Exception as e:
                    bot.send_message(chat_id, f"⚠️ Не вдалося відправити Excel: <code>{e}</code>", parse_mode="HTML")

                state.phase = "WAIT_MAIN"
                return
            else:
                state.alloc_df = df_res
                state.phase = "WAIT_ALLOC_BUDGET"
                bot.reply_to(message, "✅ Файл Result прийнято. Введіть, будь ласка, бюджет (наприклад: 200).")

        else:
            bot.reply_to(message, "⚠️ Несподівана фаза. Спробуйте ще раз із головної таблиці.")

    except ValueError as ve:
        bot.reply_to(
            message,
            f"❌ Помилка у файлі <b>{filename}</b>:\n<code>{ve}</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        bot.reply_to(message, f"⚠️ Непередбачена помилка: <code>{e}</code>", parse_mode="HTML")


@bot.message_handler(commands=["allocate"])
@require_access
def cmd_allocate(message: types.Message):
    chat_id = message.chat.id
    state = user_states.setdefault(chat_id, UserState())

    # скинемо проміжний стан алокації
    state.alloc_df = None
    state.alloc_budget = None
    state.alloc_mode = None
    state.phase = "WAIT_ALLOC_MODE"

    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🔹 Оптимальна (локальна)", callback_data="alloc_mode:optimal"),
        types.InlineKeyboardButton("🔹 Альтернативна (локальна без бюджету)", callback_data="alloc_mode:alternative"),
    )
    kb.add(
        types.InlineKeyboardButton("🤖 OpenAI (без бюджету)", callback_data="alloc_mode:openai"),
    )

    bot.reply_to(
        message,
        "Оберіть режим алокації:",
        reply_markup=kb
    )


@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("alloc_mode:"))
@require_access_cb
def on_alloc_mode(call: types.CallbackQuery):
    chat_id = call.message.chat.id
    state = user_states.setdefault(chat_id, UserState())
    mode = call.data.split(":", 1)[1]

    if mode not in {"optimal", "alternative", "openai"}:
        bot.answer_callback_query(call.id, "Невідомий режим.")
        return

    state.alloc_mode = mode
    # Після вибору режиму просимо result.xlsx (для будь-якого режиму)
    state.phase = "WAIT_ALLOC_RESULT"
    bot.answer_callback_query(call.id, "Режим обрано.")
    bot.send_message(
        chat_id,
        (
            "Надішліть файл <b>result.xlsx</b> (той, що бот згенерував раніше).\n\n"
            "• У режимі <b>OpenAI</b> бюджет не потрібен — оброблю одразу після отримання файлу.\n"
            "• У локальних режимах попрошу бюджет після завантаження файлу."
        ),
        parse_mode="HTML"
    )


@bot.message_handler(content_types=["text"], func=lambda m: not (m.text or "").startswith("/"))
@require_access
def on_text(message: types.Message):
    chat_id = message.chat.id
    state = user_states.setdefault(chat_id, UserState())

    # перехоплюємо тільки у фазі алокації
    if state.phase != "WAIT_ALLOC_BUDGET":
        return

    # ===== Локальні режими: нижче все як було (потрібен бюджет) =====
    # Parse budget
    txt = (message.text or "").strip().replace(",", ".")
    try:
        budget = float(txt)
        if budget < 0:
            raise ValueError("negative")
    except Exception:
        bot.reply_to(message, "Введи, будь ласка, коректне додатнє число (наприклад: 200).")
        return

    if state.alloc_df is None or len(state.alloc_df) == 0:
        bot.reply_to(message, "Немає завантаженої таблиці Result. Використай /allocate ще раз.")
        state.phase = "WAIT_MAIN"
        return

    # Локальна алокація
    alloc_df, summary, alloc_vec = compute_optimal_allocation(state.alloc_df, budget)

    # Формуємо файл із новими витратами
    bio = io.BytesIO()
    write_result_like_excel_with_new_spend(bio, state.alloc_df, new_total_spend=alloc_vec)

    bio.seek(0)
    bot.send_document(
        chat_id,
        bio,
        visible_file_name="allocation.xlsx",
        caption=summary  # короткий підсумок
    )

    # Детальне пояснення
    explanation = build_allocation_explanation(state.alloc_df, alloc_vec, budget, max_lines=20)
    bot.send_message(chat_id, explanation)

    state.phase = "WAIT_MAIN"


@bot.callback_query_handler(func=lambda c: c.data == "skip_offer")
@require_access_cb
def on_skip_offer(call: types.CallbackQuery):
    chat_id = call.message.chat.id
    state = user_states.setdefault(chat_id, UserState())

    # Якщо вже поза межами — просто ігноруємо
    if state.current_offer_index >= len(state.offers):
        bot.answer_callback_query(call.id, "Немає активного оферу.")
        return

    offer = state.offers[state.current_offer_index]

    # ВАЖЛИВО: при пропуску — НЕ додаємо цей офер у результат,
    # тож просто прибираємо його з проміжних структур (якщо ти зберігаєш агрегати)
    if hasattr(state, "main_agg_df") and state.main_agg_df is not None:
        # повністю забираємо рядки цього оферу, щоб не потрапили у фінальний Excel
        state.main_agg_df = state.main_agg_df[state.main_agg_df["Назва Офферу"] != offer]

    # перехід до наступного оферу
    state.current_offer_index += 1
    bot.answer_callback_query(call.id, "Офер пропущено.")

    # якщо ще є офери — попросимо наступну додаткову таблицю
    if state.current_offer_index < len(state.offers):
        ask_additional_table_with_skip(call.message, state)
    else:
        # якщо оферів більше немає — генеруємо фінальний файл
        try:
            final_df = build_final_output(state)
            send_final_table(call.message, final_df)
        except Exception as e:
            bot.send_message(chat_id, f"⚠️ Помилка під час формування файлу: <code>{e}</code>")


@bot.message_handler(commands=["whoami"])
def whoami(message: types.Message):
    bot.reply_to(message, f"Ваш Telegram ID: <code>{message.from_user.id}</code>")


# ===================== MAIN TABLE LOGIC =====================

def handle_main_table(message: types.Message, state: UserState, df: pd.DataFrame):
    # Clean & coerce
    work = df.copy()
    work["Назва Офферу"] = work["Назва Офферу"].astype(str).str.strip()
    work["ГЕО"] = work["ГЕО"].astype(str).str.strip()
    work["Загальні витрати"] = pd.to_numeric(work["Загальні витрати"], errors="coerce").fillna(0.0)

    # Drop empty/placeholder Offer IDs - handle string values properly
    work = work[
        work["Назва Офферу"].ne("") &
        work["Назва Офферу"].ne("nan") &
        work["Назва Офферу"].ne("None") &
        work["Назва Офферу"].notna()
        ]

    # Also filter out rows where ГЕО is empty
    work = work[
        work["ГЕО"].ne("") &
        work["ГЕО"].ne("nan") &
        work["ГЕО"].ne("None") &
        work["ГЕО"].notna()
        ]

    if len(work) == 0:
        bot.reply_to(message, "Не знайдено валідних записів у BUDG таблиці після очищення.")
        return

    # Aggregate by Offer ID + GEO
    agg = (
        work.groupby(["Назва Офферу", "ГЕО"])["Загальні витрати"]
        .sum().reset_index()
    )

    state.main_agg_df = agg

    # Unique Offer IDs (from cleaned data)
    state.offers = sorted(agg["Назва Офферу"].unique().tolist())
    state.phase = "WAIT_ADDITIONAL"
    state.current_offer_index = 0
    ask_additional_table_with_skip(message, state)

    if not state.offers:
        bot.reply_to(message, "Не знайдено жодного валідного Offer ID у аркуші BUDG після очищення.")
        return


# ===================== ADDITIONAL TABLE LOGIC =====================

def handle_additional_table(message: types.Message, state: UserState, df: pd.DataFrame):
    # 1) Clean & normalize
    work = df.copy()
    work["Країна"] = work["Країна"].astype(str).str.strip()
    work["Сума депозитів"] = pd.to_numeric(work["Сума депозитів"], errors="coerce").fillna(0.0)

    # Фільтруємо порожні/некоректні країни
    work = work[
        work["Країна"].ne("") &
        work["Країна"].ne("nan") &
        work["Країна"].ne("None") &
        work["Країна"].notna()
        ]

    # 2) Визначаємо поточний офер
    try:
        current_offer = state.offers[state.current_offer_index]
    except IndexError:
        bot.reply_to(message, "Помилка: немає активного Offer ID. Напиши /start для початку.")
        return

    # 3) Якщо після очищення немає жодного валідного рядка — проставляємо нулі
    if len(work) == 0:
        # Зберігаємо «нульові» депозити для логіки фінального мерджу
        # (порожній словник означає, що по країнах нічого не додавати;
        # нижче ще й гарантуємо нулі у проміжній таблиці, якщо вона вже є)
        state.offer_deposits[current_offer] = {}

        # Якщо в пам’яті вже є агрегована головна таблиця — гарантуємо нулі для поточного оферу
        if hasattr(state, "main_agg_df") and state.main_agg_df is not None:
            mask = state.main_agg_df["Назва Офферу"] == current_offer
            # створимо колонки, якщо їх ще немає
            if "Total Dep Sum" not in state.main_agg_df.columns:
                state.main_agg_df["Total Dep Sum"] = 0.0
            if "Total Dep Amount" not in state.main_agg_df.columns:
                state.main_agg_df["Total Dep Amount"] = 0
            # нулі для всіх рядків цього оферу
            state.main_agg_df.loc[mask, "Total Dep Sum"] = 0.0
            state.main_agg_df.loc[mask, "Total Dep Amount"] = 0

        # Повідомлення користувачу
        bot.reply_to(
            message,
            (
                f"ℹ️ У додатковій таблиці для <b>{current_offer}</b> немає даних після очищення.\n"
                f"Для цього оферу проставлено <b>0</b> у колонках депозитів."
            ),
        )

        # Перехід до наступного оферу / фінал
        state.current_offer_index += 1
        if state.current_offer_index >= len(state.offers):
            final_df = build_final_output(state)
            send_final_table(message, final_df)
            user_states[message.chat.id] = UserState()  # reset
            return

        next_offer = state.offers[state.current_offer_index]
        bot.reply_to(
            message,
            (
                f"Надішліть додаткову таблицю для <b>{next_offer}</b> "
                f"({state.current_offer_index + 1}/{len(state.offers)})."
            ),
        )
        return

    # 4) Якщо дані є — канонікалізуємо країни та агрегуємо
    work["canon_en"] = work["Країна"].apply(
        lambda x: to_canonical_en(x, state.country_map_uk_to_en, state.country_canon)
    )

    dep_by_country = (
        work.groupby("canon_en")["Сума депозитів"]
        .agg(["sum", "count"]).reset_index()
        .rename(columns={"sum": "total", "count": "count"})
    )

    # Зберігаємо агрегати в пам'ять для цього оферу
    state.offer_deposits[current_offer] = {
        row["canon_en"]: {"total": float(row["total"]), "count": int(row["count"])}
        for _, row in dep_by_country.iterrows()
    }

    # 5) Підсумкова інфо для користувача
    countries_found = list(dep_by_country["canon_en"].unique())
    total_deposits = float(dep_by_country["total"].sum())

    # 6) Перехід до наступного оферу / фінал
    state.current_offer_index += 1
    if state.current_offer_index >= len(state.offers):
        final_df = build_final_output(state)
        send_final_table(message, final_df)
        user_states[message.chat.id] = UserState()  # reset
        return

    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Пропустити цей офер", callback_data="skip_offer"))

    next_offer = state.offers[state.current_offer_index]
    summary = f"""
✅ Прийнято дані для <b>{current_offer}</b>
📊 Знайдено {len(countries_found)} країн, загальна сума депозитів: {total_deposits:,.2f}

Країни: {', '.join(countries_found[:5])}{' ...' if len(countries_found) > 5 else ''}

Надішли наступну додаткову таблицю для <b>{next_offer}</b> ({state.current_offer_index + 1}/{len(state.offers)})
    """.strip()

    bot.send_message(message.from_user.id, summary, reply_markup=kb)


# ===================== BUILD FINAL =====================

def geo_to_canonical(geo: str, uk_to_en: Dict[str, str], canonical: Dict[str, str]) -> str:
    return to_canonical_en(geo, uk_to_en, canonical)


def build_final_output(state: UserState) -> pd.DataFrame:
    agg = state.main_agg_df.copy()
    # Canonical GEO for matching
    agg["ГЕО_canon"] = agg["ГЕО"].apply(lambda g: geo_to_canonical(g, state.country_map_uk_to_en, state.country_canon))

    rows: List[Dict[str, object]] = []
    for _, row in agg.iterrows():
        offer_name = str(row["Назва Офферу"])
        # If you have a real Offer ID elsewhere — put it here. Fallback to offer_name.
        offer_id = offer_name

        geo_display = str(row["ГЕО"])
        geo_canon = str(row["ГЕО_canon"])
        spend_total = float(row["Загальні витрати"])

        dep_sum = 0.0  # Total Dep Sum ($)
        dep_cnt = 0  # FTD qty
        offer_map = state.offer_deposits.get(offer_name, {})
        if geo_canon in offer_map:
            dep_sum = float(offer_map[geo_canon]["total"])
            dep_cnt = int(offer_map[geo_canon]["count"])
        else:
            import difflib
            candidates = list(offer_map.keys())
            close = difflib.get_close_matches(geo_canon, candidates, n=1, cutoff=0.6)
            if close:
                dep_sum = float(offer_map[close[0]]["total"])
                dep_cnt = int(offer_map[close[0]]["count"])

        rows.append({
            "Subid": "",  # empty
            "Offer ID": offer_id,  # from main table (fallback = name)
            "Назва Офферу": offer_name,  # Offer Name
            "ГЕО": geo_display,  # Country
            "FTD qty": dep_cnt,  # count
            "Total Spend": spend_total,  # $
            "Total Dep Amount": dep_sum,  # $ (your naming; this is the sum)
            # rest computed later in Excel
        })

    # Order primary columns; computed ones will be added in send_final_table
    df = pd.DataFrame(rows, columns=[
        "Subid", "Offer ID", "Назва Офферу", "ГЕО", "FTD qty", "Total Spend", "Total Dep Amount"
    ])
    return df


# ===================== SENDER =====================

def send_final_table(message: types.Message, df: pd.DataFrame):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        # Rebuild to requested order
        base_cols = [
            "Subid", "Offer ID", "Назва Офферу", "ГЕО",
            "FTD qty", "Total Spend", "Total Dep Amount"
        ]
        df = df[base_cols].copy()

        # ---- NEW: round numeric inputs before write ----
        df["Total Spend"] = pd.to_numeric(df["Total Spend"], errors="coerce").round(2)
        df["Total Dep Amount"] = pd.to_numeric(df["Total Dep Amount"], errors="coerce").round(2)
        df["FTD qty"] = pd.to_numeric(df["FTD qty"], errors="coerce").fillna(0).astype(int)
        # -------------------------------------------------

        final_cols = [
            "Subid", "Offer ID", "Назва Офферу", "ГЕО",
            "FTD qty", "Total spend", "Total+%", "CPA", "CPA Target", "СP/Ч",
            "Total Dep Amount", "My deposit amount", "C. profit Target 40%", "C. profit Target 50%",
            "CAP", "Остаток CAP"
        ]

        df.rename(columns={"Total Spend": "Total spend"}, inplace=True)

        # Placeholders
        df["Total+%"] = None
        df["CPA"] = None
        df["CPA Target"] = None
        df["СP/Ч"] = None
        df["My deposit amount"] = None
        df["C. profit Target 40%"] = None
        df["C. profit Target 50%"] = None
        df["CAP"] = ""
        df["Остаток CAP"] = ""

        df = df[final_cols]

        df.to_excel(writer, index=False, sheet_name="Result")

        wb = writer.book
        ws = writer.sheets["Result"]

        from openpyxl.styles import PatternFill, Alignment, Font
        from openpyxl.formatting.rule import FormulaRule

        first_row = 2
        last_row = ws.max_row

        # Formulas (letters for new layout)
        for r in range(first_row, last_row + 1):
            ws[f"G{r}"].value = f"=F{r}*1.3"  # Total+%
            ws[f"H{r}"].value = f"=IFERROR(G{r}/E{r},\"\")"  # CPA
            ws[f"I{r}"].value = cpa_target_for_geo(ws[f"D{r}"].value)  # CPA Target
            ws[f"J{r}"].value = f"=IFERROR(K{r}/E{r},\"\")"  # СP/Ч
            ws[f"L{r}"].value = f"=IFERROR(K{r}/G{r}*100,0)"  # My deposit amount
            ws[f"M{r}"].value = f"=G{r}*0.4"  # Profit 40%
            ws[f"N{r}"].value = f"=G{r}*0.5"  # Profit 50%

        # ---- NEW: number formats to 2 decimals everywhere needed ----
        # Integers: E (FTD qty)
        for r in range(first_row, last_row + 1):
            ws[f"E{r}"].number_format = "0"

        # Two decimals: F..N except I (integer target), but leave I as integer 8
        two_dec_cols = ["F", "G", "H", "J", "K", "L", "M", "N"]
        for col in two_dec_cols:
            for r in range(first_row, last_row + 1):
                ws[f"{col}{r}"].number_format = "0.00"

        # Якщо хочеш дві коми саме в українському форматі з комою як розділювачем,
        # Excel підхопить локаль користувача автоматично; шаблон "0.00" відобразиться з комою у UA-локалі.
        # --------------------------------------------------------------

        # Header styling
        for col in range(1, 17):  # A..P
            ws.cell(row=1, column=col).font = Font(bold=True)
            ws.cell(row=1, column=col).alignment = Alignment(horizontal="center")

        widths = {
            "A": 10, "B": 12, "C": 22, "D": 16, "E": 10, "F": 14, "G": 12, "H": 10, "I": 12,
            "J": 10, "K": 16, "L": 18, "M": 18, "N": 18, "O": 12, "P": 16
        }
        for col, w in widths.items():
            ws.column_dimensions[col].width = w

        # Conditional formatting (unchanged logic, new letters)
        data_range = f"A{first_row}:P{last_row}"
        grey = PatternFill("solid", fgColor="BFBFBF")
        green = PatternFill("solid", fgColor="C6EFCE")
        yellow = PatternFill("solid", fgColor="FFEB9C")
        red = PatternFill("solid", fgColor="FFC7CE")

        # Grey: E = 0
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=["$E2=0"], fill=grey, stopIfTrue=True)
        )

        # Green: INT(H) <= INT(I) AND L > 39
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=["AND($E2>0,INT($H2)<=INT($I2),$L2>39)"], fill=green, stopIfTrue=True)
        )

        # Yellow: (INT(H) <= INT(I)) OR (L > 39 AND H < I*1.31)
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=["AND($E2>0,OR(INT($H2)<=INT($I2),AND($L2>39,$H2<$I2*1.31)))"], fill=yellow,
                        stopIfTrue=True)
        )

        # Red: (E > 0 AND H > I*1.3 AND L > 39) OR (E > 0 AND INT(H) > INT(I) AND L < 39)
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(
                formula=["OR(AND($E2>0,$H2>$I2*1.3,$L2>39),AND($E2>0,INT($H2)>INT($I2),$L2<39))"],
                fill=red,
                stopIfTrue=True
            )
        )

    bio.seek(0)
    bot.send_document(
        message.chat.id,
        bio,
        visible_file_name="result.xlsx",
        caption="Фінальна таблиця (2 знаки після коми, новий порядок колонок)"
    )


# ===================== MAIN =====================
if __name__ == "__main__":
    print("Bot is running...")
    bot.infinity_polling(skip_pending=True)
