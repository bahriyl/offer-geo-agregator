import os
import io
import re
import html
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv

import pandas as pd
from telebot import TeleBot, types
import numpy as np
from datetime import datetime

load_dotenv()

# ===================== CONFIG =====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
MAIN_SHEET_NAME = "BUDG"  # read this sheet from the main file
ALLOWED_MAIN_COLUMNS = ["Назва Офферу", "ГЕО", "Загальні витрати"]
ADDITIONAL_REQUIRED_COLS = ["Країна", "Сума депозитів"]

bot = TeleBot(BOT_TOKEN, parse_mode="HTML")

CPA_TARGET_DEFAULT = 8.0
CPA_TARGET_INT = int(CPA_TARGET_DEFAULT)
YELLOW_MULT = 1.31
RED_MULT = 1.8
DEPOSIT_GREEN_MIN = 39.0
EPS = 1e-12
EPS_YEL = 1e-6
CPA_TOL = 1e-9
DEPOSIT_TOL = 1e-9


# ===================== FORMULA HELPERS =====================


def _build_yellow_formula(row: int = 2) -> str:
    """Return the Excel conditional formatting formula for the yellow status."""

    row_ref = str(row)
    return (
        f"AND($E{row_ref}>0,"
        f"OR("
        f"AND($L{row_ref}>{DEPOSIT_GREEN_MIN:.0f},$H{row_ref}<$I{row_ref}*{YELLOW_MULT:.2f}),"
        f"AND($L{row_ref}<={DEPOSIT_GREEN_MIN:.0f},$H{row_ref}<=INT($I{row_ref})+1)"
        f")"
        f")"
    )


# ===================== STATE =====================
class UserState:
    def __init__(self):
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
        self.alloc_mode: Optional[str] = None  # "budget" or "max_yellow"


user_states: Dict[int, UserState] = {}

# ===== ACCESS CONTROL =====
# Заповни своїми Telegram ID (int). Можна зберігати у .env і парсити з ENV.
ALLOWED_USER_IDS = {
    155840708,
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


# ===================== NORMALIZATION (countries) =====================

def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip().lower()


def _normalize_money(series: pd.Series) -> pd.Series:
    """Normalise money-like strings into numeric values."""
    if series is None:
        return pd.Series(dtype=float)

    if isinstance(series, pd.Series):
        ser = series.copy()
    else:
        ser = pd.Series(series)
    if ser.empty:
        return pd.to_numeric(ser, errors="coerce")

    ser = ser.astype("string")
    ser = ser.str.replace("\u00a0", "", regex=False)
    ser = ser.str.replace("\u202f", "", regex=False)
    ser = ser.str.strip()
    ser = ser.str.replace(r"\s+", "", regex=True)
    ser = ser.str.replace(r"[^0-9,\.\-]", "", regex=True)

    def _harmonise_decimal(value):
        if value is pd.NA:
            return value
        if value is None:
            return pd.NA
        text = str(value)
        if text == "" or text in {"-", ".", ",", "-.", "-,"}:
            return pd.NA
        if "," in text and "." in text:
            text = text.replace(",", "")
        elif "," in text:
            text = text.replace(",", ".")
        return text

    ser = ser.map(_harmonise_decimal)
    return pd.to_numeric(ser, errors="coerce")


def build_country_map_uk_to_en() -> Dict[str, str]:
    m = {
        "Бенін": "Benin",
        "Буркіна-Фасо": "Burkina Faso",
        "Габон": "Gabon",
        "Гаїті": "Haiti",
        "Гана": "Ghana",
        "Гвінея": "Guinea",
        # ---- FIX: зводимо все до "Congo (Kinshasa)" ----
        "Демократична Республіка Конґо": "Congo (Kinshasa)",
        "ДР Конго": "Congo (Kinshasa)",
        "Конго (Кіншаса)": "Congo (Kinshasa)",
        # -----------------------------------------------
        "Республіка Конго": "Congo (Brazzaville)",
        "Конго-Браззавіль": "Congo (Brazzaville)",
        "Конго": "Congo (Kinshasa)",  # якщо пишуть просто "Конго" — приймаємо як DRC
        "Камерун": "Cameroon",
        "Кот-д'Івуар": "Cote d'Ivoire",
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
    }
    return {normalize_text(k): v for k, v in m.items()}


def build_country_canonical() -> Dict[str, str]:
    canon = {
        # EN canonical
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
        # ---- FIX: усі синоніми до Congo (Kinshasa) ----
        "DRC": "Congo (Kinshasa)",
        "DR Congo": "Congo (Kinshasa)",
        "Democratic Republic of the Congo": "Congo (Kinshasa)",
        # -----------------------------------------------
        "Ivory Coast": "Cote d'Ivoire",
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

        # UA → canonical EN
        "бенін": "Benin",
        "буркіна-фасо": "Burkina Faso",
        "габон": "Gabon",
        "гаїті": "Haiti",
        "гана": "Ghana",
        "гвінея": "Guinea",
        # ---- FIX UA-синоніми ДРК ----
        "демократична республіка конґо": "Congo (Kinshasa)",
        "др конго": "Congo (Kinshasa)",
        "конго (кіншаса)": "Congo (Kinshasa)",
        # --------------------------------
        "республіка конго": "Congo (Brazzaville)",
        "конго-браззавіль": "Congo (Brazzaville)",
        "конго": "Congo (Kinshasa)",  # дефолт у бік DRC
        "камерун": "Cameroon",
        "кот-д'івуар": "Cote d'Ivoire",
        "кенія": "Kenya",
        "сенегал": "Senegal",
        "сьєрра-леоне": "Sierra Leone",
        "танзанія": "Tanzania",
        "того": "Togo",
        "уганда": "Uganda",
        "замбія": "Zambia",
        "ефіопія": "Ethiopia",
        "нігер": "Niger",
        "нігерія": "Nigeria",
        "малі": "Mali",
        "казахстан": "Kazakhstan",
        "іспанія": "Spain",
        "франція": "France",
        "італія": "Italy",
        "португалія": "Portugal",
        "домініканська республіка": "Dominican Republic",
        "канада": "Canada",
        "філіппіни": "Philippines",
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
    Robust Excel reader with multiple fallback strategies
    and filter: keep only rows where column 'Дата' belongs to current month.
    """
    bio = io.BytesIO(file_bytes)
    errors = []

    # Helper: filter to current month
    def filter_current_month(df: pd.DataFrame) -> pd.DataFrame:
        from datetime import datetime
        try:
            from zoneinfo import ZoneInfo
            now_month = datetime.now(ZoneInfo("Europe/Kyiv")).month
        except Exception:
            # якщо zoneinfo недоступний
            now_month = datetime.now().month

        if "Місяць" not in df.columns:
            return df.iloc[0:0]  # або підніміть помилку, якщо так зручніше

        out = df.copy()
        # у стовпці можуть бути "", текст тощо — приводимо до числа
        out["Місяць"] = pd.to_numeric(out["Місяць"], errors="coerce")
        return out[out["Місяць"] == now_month]

    # Strategy 1: Try openpyxl with data_only=True
    try:
        bio.seek(0)
        df = pd.read_excel(bio, sheet_name=sheet_name, header=header, engine="openpyxl")
        return filter_current_month(df)
    except Exception as e:
        errors.append(f"openpyxl: {e}")
        print(f"openpyxl failed: {e}")

    # Strategy 2: Try reading without specifying header first, then set it manually
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

    # Strategy 3: Try calamine if available
    try:
        bio.seek(0)
        df = pd.read_excel(bio, sheet_name=sheet_name, header=header, engine="calamine")
        return filter_current_month(df)
    except Exception as e:
        errors.append(f"calamine: {e}")
        print(f"calamine failed: {e}")

    # Strategy 4: Try xlrd
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
            # (тут залишається твоя fallback-логіка для Excel)
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

        # detect header row
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

    df = df[[colmap["Назва Офферу"], colmap["ГЕО"], colmap["Загальні витрати"], *([c for c in df.columns if c == "Місяць"])]].copy()

    # rename тільки основні колонки
    rename_map = {
        colmap["Назва Офферу"]: "Назва Офферу",
        colmap["ГЕО"]: "ГЕО",
        colmap["Загальні витрати"]: "Загальні витрати"
    }
    df.rename(columns=rename_map, inplace=True)

    # --- фільтрація по поточному місяцю ---
    if "Місяць" in df.columns:
        from datetime import datetime
        current_month = datetime.now().month
        df["Місяць"] = pd.to_numeric(df["Місяць"], errors="coerce")
        df = df[df["Місяць"] == current_month]

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


def read_result_allocation_table(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Parse result.xlsx (sheet Result) and normalise required columns."""
    if not filename.lower().endswith((".xlsx", ".xls", ".xlsm")):
        raise ValueError("Очікую файл Excel (result.xlsx) з аркушем Result.")

    engines = ["openpyxl", "calamine", "xlrd"]
    errors = []
    df = None
    for engine in engines:
        try:
            bio = io.BytesIO(file_bytes)
            df = pd.read_excel(bio, sheet_name="Result", engine=engine)
            break
        except Exception as e:
            errors.append(f"{engine}: {e}")

    if df is None:
        raise ValueError(
            "Не вдалося прочитати аркуш Result у файлі. Спробуйте ще раз або перевірте, що файл — result.xlsx."
            + (f" Деталі: {'; '.join(errors)}" if errors else "")
        )

    df.columns = [str(c).replace("\xa0", " ").replace("\u00A0", " ").strip() for c in df.columns]

    def _ensure_column(label: str, required: bool) -> None:
        mapping = match_columns(df.columns, [label])
        if mapping:
            actual = mapping[label]
            if actual != label:
                df.rename(columns={actual: label}, inplace=True)
            return
        if required:
            raise ValueError(f"У файлі немає очікуваної колонки \"{label}\" на аркуші Result.")

    for optional_col in ["Subid", "Offer ID", "Назва Офферу", "ГЕО"]:
        _ensure_column(optional_col, required=False)

    for required_col in ["FTD qty", "Total spend", "Total Dep Amount", "Total+%"]:
        _ensure_column(required_col, required=True)

    df["FTD qty"] = pd.to_numeric(df.get("FTD qty", 0), errors="coerce").fillna(0).astype(int)
    for col in ["Total spend", "Total Dep Amount"]:
        series = df.get(col, pd.Series(0.0, index=df.index))
        df[col] = _normalize_money(series).fillna(0.0).round(2)

    total_plus_raw = df.get("Total+%", pd.Series(0.0, index=df.index))
    if not isinstance(total_plus_raw, pd.Series):
        total_plus_raw = pd.Series(total_plus_raw, index=df.index)

    total_plus_numeric = _normalize_money(total_plus_raw).astype(float)

    total_spend_series = df["Total spend"].astype(float)
    total_plus_str = total_plus_raw.astype("string")
    formula_mask = (
        total_plus_str.str.contains(r"=", regex=False, na=False)
        | total_plus_str.str.contains(r"[A-Za-z]", regex=True, na=False)
    )

    valid_ratio_mask = (
        ~formula_mask
        & np.isfinite(total_spend_series)
        & (total_spend_series > 0)
        & np.isfinite(total_plus_numeric)
        & (total_plus_numeric > 0)
    )

    if valid_ratio_mask.any():
        multiplier = float((total_plus_numeric[valid_ratio_mask] / total_spend_series[valid_ratio_mask]).median())
    else:
        multiplier = 1.3

    needs_recompute = (
        formula_mask
        | ~np.isfinite(total_plus_numeric)
        | (total_plus_numeric <= 0)
    )
    fallback_mask = needs_recompute & np.isfinite(total_spend_series) & (total_spend_series > 0)
    if fallback_mask.any():
        total_plus_numeric.loc[fallback_mask] = total_spend_series.loc[fallback_mask] * multiplier

    df["Total+%"] = total_plus_numeric.fillna(0.0)

    return df


# ===================== HELPERS =====================

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

def _resolve_target_value(raw: Optional[float]) -> Tuple[float, int]:
    value = float(raw) if raw is not None and np.isfinite(raw) and raw > 0 else CPA_TARGET_DEFAULT
    target_int = int(np.floor(value)) if value > 0 else CPA_TARGET_INT
    if target_int <= 0:
        target_int = CPA_TARGET_INT
    return value, target_int


def _calc_cpa(e: float, f: float) -> float:
    if e <= 0:
        return float("inf")
    return 1.3 * f / e


def _calc_deposit_pct(k: float, f: float) -> float:
    if f <= 0:
        return float("inf")
    return (100.0 * k) / (1.3 * f)


def _extract_targets(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    targets = pd.to_numeric(df.get("CPA Target", CPA_TARGET_DEFAULT), errors="coerce").fillna(CPA_TARGET_DEFAULT)
    targets = targets.where(targets > 0, CPA_TARGET_DEFAULT)
    target_ints = np.floor(targets.to_numpy())
    target_ints[~np.isfinite(target_ints) | (target_ints <= 0)] = CPA_TARGET_INT
    target_ints = target_ints.astype(int)
    return targets, pd.Series(target_ints, index=df.index)


def _build_threshold_table(E: pd.Series, K: pd.Series, targets: pd.Series, target_ints: pd.Series) -> pd.DataFrame:
    e = E.to_numpy(dtype=float)
    k = K.to_numpy(dtype=float)
    t = targets.to_numpy(dtype=float)
    tint = target_ints.to_numpy(dtype=float)

    with np.errstate(divide='ignore', invalid='ignore'):
        green_cpa_limit = np.where(e > 0, (tint * e) / 1.3, 0.0)
        deposit_break = np.where((k > 0) & (DEPOSIT_GREEN_MIN > 0), (100.0 * k) / (1.3 * DEPOSIT_GREEN_MIN), 0.0)
        yellow_soft_raw = np.where(e > 0, (t * YELLOW_MULT * e) / 1.3, 0.0)
        red_limit_raw = np.where(e > 0, (t * RED_MULT * e) / 1.3, 0.0)

    red_ceiling = np.maximum(red_limit_raw - EPS_YEL, 0.0)
    red_floor = np.minimum(yellow_soft_raw, red_ceiling)
    red_floor = np.maximum(red_floor - EPS_YEL, 0.0)
    yellow_soft = np.minimum(np.maximum(yellow_soft_raw - EPS_YEL, 0.0), red_floor)
    green_ceiling = np.minimum(green_cpa_limit, np.maximum(deposit_break - EPS_YEL, 0.0))
    green_ceiling = np.minimum(green_ceiling, red_floor)

    return pd.DataFrame({
        "target": t,
        "target_int": target_ints.astype(int),
        "green_cpa_limit": green_cpa_limit,
        "deposit_break": deposit_break,
        "green_ceiling": green_ceiling,
        "red_floor": red_floor,
        "yellow_soft_ceiling": yellow_soft,
        "red_ceiling": red_ceiling,
    }, index=E.index)


def _compute_make_yellow_target(e: float, f_cur: float, k: float, thresholds_row: pd.Series) -> Optional[float]:
    if e <= 0:
        return None
    red_ceiling = float(thresholds_row.get("red_ceiling", 0.0))
    if red_ceiling <= f_cur + EPS:
        return None

    candidates: List[float] = []

    cpa_cross = float(thresholds_row.get("green_cpa_limit", 0.0))
    if np.isfinite(cpa_cross) and cpa_cross > f_cur + EPS:
        candidates.append(min(red_ceiling, cpa_cross + EPS_YEL))

    deposit_break = float(thresholds_row.get("deposit_break", 0.0))
    if np.isfinite(deposit_break) and deposit_break > f_cur + EPS:
        candidate_raw = deposit_break + EPS_YEL
        if _calc_cpa(e, candidate_raw) < float(thresholds_row.get("target_int", CPA_TARGET_INT)) + EPS:
            candidates.append(min(red_ceiling, candidate_raw))

    if not candidates:
        return None

    yellow_soft = float(thresholds_row.get("yellow_soft_ceiling", 0.0))
    if yellow_soft > 0:
        candidates = [min(c, yellow_soft) for c in candidates]

    red_floor = float(thresholds_row.get("red_floor", 0.0))
    if red_floor > 0:
        candidates = [min(c, red_floor) for c in candidates]

    target_value = min(candidates)
    return target_value if target_value > f_cur + EPS else None


def _compute_yellow_limit(e: float, f_cur: float, k: float, thresholds_row: pd.Series) -> float:
    red_ceiling = float(thresholds_row.get("red_ceiling", 0.0))
    if red_ceiling <= 0:
        return 0.0
    deposit_now = _calc_deposit_pct(k, f_cur)
    if deposit_now > DEPOSIT_GREEN_MIN + EPS:
        limit = float(thresholds_row.get("yellow_soft_ceiling", 0.0))
    else:
        limit = max(0.0, float(thresholds_row.get("green_cpa_limit", 0.0)) - EPS_YEL)

    red_floor = float(thresholds_row.get("red_floor", 0.0))
    if red_floor > 0:
        limit = min(limit, red_floor)

    return min(max(limit, 0.0), red_ceiling)


def _classify_status(E: float, F: float, K: float, target: Optional[float] = None) -> str:
    if E <= 0:
        return "Grey"
    target_val, target_int = _resolve_target_value(target)
    cpa = _calc_cpa(E, F)
    deposit_pct = _calc_deposit_pct(K, F)

    deposit_green_cutoff = DEPOSIT_GREEN_MIN + DEPOSIT_TOL
    red_lower_bound = target_val * YELLOW_MULT
    red_upper_bound = target_val * RED_MULT

    if (deposit_pct > deposit_green_cutoff) and (cpa <= target_int + CPA_TOL):
        return "Green"

    if deposit_pct > deposit_green_cutoff:
        if (cpa >= target_int - CPA_TOL) and (cpa < red_lower_bound - CPA_TOL):
            return "Yellow"
    else:
        if cpa <= target_int - CPA_TOL:
            return "Yellow"

    if (cpa >= red_lower_bound - CPA_TOL) and (cpa <= red_upper_bound + CPA_TOL):
        return "Red"

    return "Grey"


def _fmt(v: float, suf: str = "", nan_text: str = "-") -> str:
    if not np.isfinite(v):
        return nan_text
    return f"{v:.2f}{suf}"


def build_allocation_explanation(df_source: pd.DataFrame,
                                 alloc_vec: pd.Series,
                                 budget: float,
                                 max_lines: int = 20,
                                 *,
                                 alloc_is_delta: bool = True) -> str:
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
    F = _normalize_money(df.get("Total spend", pd.Series(0.0, index=df.index))).fillna(0.0)
    K = _normalize_money(df.get("Total Dep Amount", pd.Series(0.0, index=df.index))).fillna(0.0)
    targets, _ = _extract_targets(df)

    alloc_input = pd.to_numeric(alloc_vec, errors="coerce").reindex(df.index).fillna(0.0)

    if alloc_is_delta:
        alloc_delta = alloc_input
        F_new = F + alloc_delta
        used = float(alloc_delta.sum())
    else:
        F_new = alloc_input
        alloc_delta = F_new - F
        used = float(F_new.sum())

    # Статуси ДО/ПІСЛЯ
    before = [
        _classify_status(float(E[i]), float(F[i]), float(K[i]), float(targets.at[i]))
        for i in df.index
    ]
    after = [
        _classify_status(float(E[i]), float(F_new[i]), float(K[i]), float(targets.at[i]))
        for i in df.index
    ]

    # Метрики
    total_budget = float(budget)
    left = max(0.0, total_budget - used)

    yellow_before = sum(1 for s in before if s == "Yellow")
    yellow_after = sum(1 for s in after if s == "Yellow")
    green_to_yellow = sum(1 for i in df.index if (before[i] == "Green" and after[i] == "Yellow"))

    # Побудова списку рядків з алокацією
    rows = []
    for i in alloc_delta.index:
        if alloc_delta[i] <= 0:
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
            f"+{alloc_delta[i]:.2f} → Total Spend {Fi:.2f}→{Fni:.2f}; "
            f"CPA {_fmt(H_before)}→{_fmt(H_after)}, "
            f"My deposit amount {_fmt(L_before, '%')}→{_fmt(L_after, '%')} | "
            f"{before[i]} → {after[i]}"
        )
        rows.append((alloc_delta[i], line))

    # Сортуємо за найбільшою алокацією і обрізаємо
    rows.sort(key=lambda x: (-x[0], x[1]))
    detail_lines = [ln for _, ln in rows[:max_lines]]
    escaped_detail_lines = [html.escape(ln) for ln in detail_lines]

    header = (
        f"Розподіл бюджету: {used:.2f} / {total_budget:.2f} використано; залишок {left:.2f}\n"
        f"Жовтих ДО/ПІСЛЯ: {yellow_before} → {yellow_after} (зел.→жовт.: {green_to_yellow})\n"
        f"Правила: green — CPA≤INT(target) і депозит>{DEPOSIT_GREEN_MIN:.0f}%, yellow — або депозит>{DEPOSIT_GREEN_MIN:.0f}% із CPA в діапазоні [INT(target); target×{YELLOW_MULT:.2f}),"
        f" або депозит≤{DEPOSIT_GREEN_MIN:.0f}% із CPA<INT(target); red — CPA в межах [target×{YELLOW_MULT:.2f}; target×{RED_MULT:.1f}]."
    )

    header = html.escape(header)

    if not detail_lines:
        return header + "\n\n(Алокації по рядках відсутні — бюджет не було куди розподілити за правилами.)"

    return header + "\n\nТоп розподілів:\n" + "\n".join(escaped_detail_lines) + \
        ("\n\n…Список обрізано." if len(rows) > max_lines else "")


def compute_allocation_max_yellow(df: pd.DataFrame) -> Tuple[pd.DataFrame, float, pd.Series]:
    """
    Режим «максимум жовтих» з автоматичним бюджетом:
      - кожен рядок має цільовий spend у колонці "Total+%" (верхня межа);
      - доступний глобальний бюджет = вся поточна сума в колонці "Total spend";
      - розподіл іде за зростанням Target: спершу переводимо green у yellow,
        потім (якщо лишилися кошти) насичуємо жовті в межах CPA < target×YELLOW_MULT
        та не виходячи за межі [target×YELLOW_MULT; target×RED_MULT], а за наявності
        залишку доводимо рядки до червоного порогу (red_ceiling), де CPA в діапазоні
        [target×YELLOW_MULT; target×RED_MULT] відповідає червоній зоні.
    Повертає оновлену таблицю, фактично розподілений бюджет та фінальні значення spend по рядках.
    """

    dfw = df.copy()
    dfw.columns = [str(c).replace("\xa0", " ").replace("\u00A0", " ").strip() for c in dfw.columns]

    E = pd.to_numeric(dfw.get("FTD qty", 0.0), errors="coerce").fillna(0.0)
    F = _normalize_money(dfw.get("Total spend", pd.Series(0.0, index=dfw.index))).fillna(0.0)
    K = _normalize_money(dfw.get("Total Dep Amount", pd.Series(0.0, index=dfw.index))).fillna(0.0)
    T = pd.to_numeric(dfw.get("Total+%", 0.0), errors="coerce").fillna(0.0)
    targets, target_ints = _extract_targets(dfw)
    thresholds = _build_threshold_table(E, K, targets, target_ints)

    stop_before_red = thresholds["red_ceiling"].fillna(0.0)

    row_allowance = pd.Series(
        np.minimum(T.to_numpy(), stop_before_red.to_numpy()),
        index=dfw.index,
    )
    row_allowance = pd.to_numeric(row_allowance, errors="coerce").clip(lower=0.0).fillna(0.0)

    available_budget = float(F.sum())
    order = T.sort_values(ascending=True).index.tolist()
    order_by_total_spend = F.sort_values(ascending=True).index.tolist()
    spend_order = order_by_total_spend
    alloc = pd.Series(0.0, index=dfw.index, dtype=float)
    rem = available_budget

    # Крок 1: переводимо green у yellow
    for idx in spend_order:
        if rem <= 1e-9:
            break
        allowance_left = float(row_allowance.at[idx] - alloc.at[idx])
        if allowance_left <= 1e-9:
            continue
        ei = float(E.at[idx])
        if ei <= 0:
            continue
        ki = float(K.at[idx])
        f_cur = float(alloc.at[idx])
        status_now = _classify_status(ei, f_cur, ki, float(targets.at[idx]))
        if status_now != "Green":
            continue
        target_yellow = _compute_make_yellow_target(ei, f_cur, ki, thresholds.loc[idx])
        if target_yellow is None:
            continue
        max_target = min(target_yellow, float(row_allowance.at[idx]))
        need = max_target - f_cur
        if need <= 1e-9:
            continue
        give = min(rem, need, allowance_left)
        if give <= 1e-9:
            continue
        alloc.at[idx] += give
        rem -= give

    # Крок 2: насичуємо yellow в межах нових правил
    if rem > 1e-9:
        F_mid = alloc.copy()
        status_mid = pd.Series([
            _classify_status(float(E.at[i]), float(F_mid.at[i]), float(K.at[i]), float(targets.at[i]))
            for i in dfw.index
        ], index=dfw.index)
        is_yellow_mid = status_mid == "Yellow"

        yellow_limit = pd.Series(0.0, index=dfw.index, dtype=float)
        for idx in dfw.index:
            if not is_yellow_mid.at[idx]:
                continue
            limit_val = _compute_yellow_limit(float(E.at[idx]), float(F_mid.at[idx]), float(K.at[idx]), thresholds.loc[idx])
            limit_val = min(limit_val, float(row_allowance.at[idx]))
            yellow_limit.at[idx] = max(limit_val, float(F_mid.at[idx]))

        headroom = (yellow_limit - F_mid).clip(lower=0.0)

        for idx in spend_order:
            if rem <= 1e-9:
                break
            if not is_yellow_mid.at[idx]:
                continue
            head = float(headroom.at[idx])
            if head <= 1e-9:
                continue
            allowance_left = float(row_allowance.at[idx] - alloc.at[idx])
            if allowance_left <= 1e-9:
                continue
            give = min(rem, head, allowance_left)
            if give <= 1e-9:
                continue
            alloc.at[idx] += give
            rem -= give

    # Крок 3: доводимо до червоного стелі, якщо бюджет ще лишився
    if rem > 1e-9:
        red_caps = thresholds["red_ceiling"].fillna(0.0)
        for idx in order_by_total_spend:
            if rem <= 1e-9:
                break
            if float(E.at[idx]) <= 0:
                continue
            allowance_left = float(row_allowance.at[idx] - alloc.at[idx])
            if allowance_left <= 1e-9:
                continue
            cap = min(float(red_caps.at[idx]), float(row_allowance.at[idx]))
            current = float(alloc.at[idx])
            need = cap - current
            if need <= 1e-9:
                continue
            give = min(rem, need, allowance_left)
            if give <= 1e-9:
                continue
            alloc.at[idx] += give
            rem -= give

    F_final = alloc
    statuses_final = pd.Series([
        _classify_status(float(E.at[i]), float(F_final.at[i]), float(K.at[i]), float(targets.at[i]))
        for i in dfw.index
    ], index=dfw.index)

    dfw["Allocated extra"] = F_final - F
    dfw["New Total spend"] = F_final
    dfw["Will be yellow"] = ["Yes" if statuses_final.at[i] == "Yellow" else "No" for i in dfw.index]

    used = float(F_final.sum())

    return dfw, used, alloc


def compute_optimal_allocation(df: pd.DataFrame, budget: float) -> Tuple[pd.DataFrame, str, pd.Series]:
    """
    Алгоритм нового розподілу:
      A) Мінімально переводимо GREEN → YELLOW (рухаємо CPA до INT(target) або депозит до 39%, не перетинаючи червону межу).
      B) Якщо залишився бюджет — насичуємо жовті, але тримаємося в межах CPA < target×YELLOW_MULT та не заходимо у червону зону [target×YELLOW_MULT; target×RED_MULT] (ця зона → Red).

    Позначення:
      E = FTD qty,
      F = Total spend,
      K = Total Dep Amount.
    """
    dfw = df.copy()

    # Числові колонки
    E = pd.to_numeric(dfw["FTD qty"], errors="coerce").fillna(0.0)
    F = _normalize_money(dfw.get("Total spend", pd.Series(0.0, index=dfw.index))).fillna(0.0)
    K = _normalize_money(dfw.get("Total Dep Amount", pd.Series(0.0, index=dfw.index))).fillna(0.0)
    targets, target_ints = _extract_targets(dfw)
    thresholds = _build_threshold_table(E, K, targets, target_ints)

    statuses_now = pd.Series([
        _classify_status(float(E.at[i]), float(F.at[i]), float(K.at[i]), float(targets.at[i]))
        for i in dfw.index
    ], index=dfw.index)

    green_mask = statuses_now == "Green"

    alloc = pd.Series(0.0, index=dfw.index, dtype=float)
    rem = float(budget) if budget and budget > 0 else 0.0

    # -------------------------------
    # A) GREEN -> YELLOW (мінімальний spend)
    # -------------------------------
    make_yellow_targets = pd.Series(np.nan, index=dfw.index)
    for idx in dfw.index:
        if not green_mask.at[idx]:
            continue
        goal = _compute_make_yellow_target(float(E.at[idx]), float(F.at[idx]), float(K.at[idx]), thresholds.loc[idx])
        if goal is not None:
            make_yellow_targets.at[idx] = goal

    need_delta = (make_yellow_targets - F).clip(lower=0.0)

    for idx in need_delta[green_mask].sort_values(ascending=True).index:
        if rem <= 1e-9:
            break
        need = float(need_delta.at[idx])
        if need <= 0:
            continue
        take = min(rem, need)
        if take <= 0:
            continue
        alloc.at[idx] += take
        rem -= take

    # -------------------------------
    # B) Насичення YELLOW в межах правил (залишитись жовтими)
    # -------------------------------
    if rem > 1e-9:
        F_mid = F + alloc
        status_mid = pd.Series([
            _classify_status(float(E.at[i]), float(F_mid.at[i]), float(K.at[i]), float(targets.at[i]))
            for i in dfw.index
        ], index=dfw.index)

        yellow_limit = pd.Series(0.0, index=dfw.index, dtype=float)
        for idx in dfw.index:
            if status_mid.at[idx] != "Yellow":
                continue
            limit_val = _compute_yellow_limit(float(E.at[idx]), float(F_mid.at[idx]), float(K.at[idx]), thresholds.loc[idx])
            yellow_limit.at[idx] = max(limit_val, float(F_mid.at[idx]))

        headroom = (yellow_limit - F_mid).clip(lower=0.0)

        for idx in headroom.sort_values(ascending=False).index:
            if rem <= 1e-9:
                break
            head = float(headroom.at[idx])
            if head <= 1e-9:
                continue
            give = min(rem, head)
            if give <= 1e-9:
                continue
            alloc.at[idx] += give
            rem -= give

    # ПІДСУМОК
    F_final = F + alloc
    statuses_final = pd.Series([
        _classify_status(float(E.at[i]), float(F_final.at[i]), float(K.at[i]), float(targets.at[i]))
        for i in dfw.index
    ], index=dfw.index)

    kept_yellow = int((statuses_final == "Yellow").sum())
    total_posE = int((E > 0).sum())

    dfw["Allocated extra"] = alloc
    dfw["New Total spend"] = F_final
    dfw["Will be yellow"] = ["Yes" if statuses_final.at[i] == "Yellow" else "No" for i in dfw.index]

    summary = html.escape(
        f"Бюджет: {budget:.2f}\n"
        f"Жовтих після розподілу: {kept_yellow}/{total_posE}\n"
        f"Правила: green — CPA≤INT(target) і депозит>{DEPOSIT_GREEN_MIN:.0f}%, yellow — тримаємо CPA нижче target×{YELLOW_MULT:.2f} "
        f"(або депозит≤{DEPOSIT_GREEN_MIN:.0f}% із CPA<INT(target)), red — CPA в межах [target×{YELLOW_MULT:.2f}; target×{RED_MULT:.1f}]."
    )
    return dfw, summary, alloc


def write_result_like_excel_with_new_spend(bio: io.BytesIO,
                                           df_source: pd.DataFrame,
                                           new_total_spend: pd.Series,
                                           *,
                                           overwrite_total_spend: bool = False):
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
    df_out["Total spend"] = _normalize_money(
        df_out.get("Total spend", pd.Series(0.0, index=df_out.index))
    ).fillna(0.0)
    df_out["Total Dep Amount"] = _normalize_money(
        df_out.get("Total Dep Amount", pd.Series(0.0, index=df_out.index))
    ).fillna(0.0)

    # Apply new spend
    # align by index; if shapes don't match, reindex new_total_spend to df_out
    new_total_spend = pd.to_numeric(new_total_spend, errors="coerce").fillna(0.0)
    new_total_spend = new_total_spend.reindex(df_out.index).fillna(0.0)
    if overwrite_total_spend:
        df_out["Total spend"] = new_total_spend.round(2)
    else:
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
        df_out[col] = _normalize_money(df_out[col]).round(2)

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
            ws[f"I{r}"].value = CPA_TARGET_DEFAULT  # CPA Target
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

        # Conditional formatting за новими правилами
        data_range = f"A{first_row}:P{last_row}"
        grey = PatternFill("solid", fgColor="BFBFBF")
        green = PatternFill("solid", fgColor="C6EFCE")
        yellow = PatternFill("solid", fgColor="FFEB9C")
        red = PatternFill("solid", fgColor="FFC7CE")

        ws.conditional_formatting.add(data_range, FormulaRule(formula=["$E2=0"], fill=grey, stopIfTrue=True))
        ws.conditional_formatting.add(data_range,
                                      FormulaRule(formula=[f"AND($E2>0,$H2<=INT($I2),$L2>{DEPOSIT_GREEN_MIN:.0f})"], fill=green, stopIfTrue=True))
        yellow_formula = _build_yellow_formula()
        ws.conditional_formatting.add(data_range, FormulaRule(formula=[yellow_formula], fill=yellow,
                                                              stopIfTrue=True))
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(
                formula=[
                    f"AND($E2>0,$H2>=$I2*{YELLOW_MULT:.2f},$H2<=$I2*{RED_MULT:.2f})"
                ],
                fill=red,
                stopIfTrue=True,
            ),
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
        bot.reply_to(message, f"Не вдалося отримати файл: <code>{e}</code>")
        return

    try:
        if state.phase == "WAIT_MAIN":
            df = load_main_budg_table(file_bytes, filename=filename)
            bot.reply_to(message, "✅ Головна таблиця завантажена! Тепер надішліть додаткові таблиці.")
            handle_main_table(message, state, df)
        elif state.phase == "WAIT_ADDITIONAL":
            df = read_additional_table(file_bytes, filename)
            handle_additional_table(message, state, df)
        elif state.phase == "WAIT_ALLOC_CHOICE":
            bot.reply_to(message, "Спочатку оберіть режим розподілу за допомогою кнопок під повідомленням.")
            return
        elif state.phase == "WAIT_ALLOC_RESULT":
            if not state.alloc_mode:
                bot.reply_to(
                    message,
                    "Режим розподілу не вибрано. Використай команду /allocate та обери потрібний режим.",
                )
                state.phase = "WAIT_MAIN"
                state.alloc_df = None
                state.alloc_mode = None
                state.alloc_budget = None
                return
            try:
                df = read_result_allocation_table(file_bytes, filename)
            except ValueError as ve:
                bot.reply_to(
                    message,
                    (
                        f"❌ Не вдалося опрацювати <b>{filename}</b>:\n"
                        f"<code>{ve}</code>\n\n"
                        "Будь ласка, переконайтеся, що надсилаєте згенерований ботом result.xlsx."
                    ),
                )
                state.alloc_df = None
                state.alloc_mode = None
                state.alloc_budget = None
                state.phase = "WAIT_MAIN"
                return

            state.alloc_df = df
            if state.alloc_mode == "budget":
                state.phase = "WAIT_ALLOC_BUDGET"
                bot.reply_to(message, "✅ Файл result.xlsx отримано. Введіть бюджет (наприклад: 200 або 200.5).")
            elif state.alloc_mode == "max_yellow":
                _alloc_df, used_budget, alloc_vec = compute_allocation_max_yellow(state.alloc_df)

                total_spend = _normalize_money(
                    state.alloc_df.get("Total spend", pd.Series(0.0, index=state.alloc_df.index))
                ).fillna(0.0)
                starting_budget = float(total_spend.sum())
                unused_budget = max(0.0, starting_budget - used_budget)

                df_norm = state.alloc_df.copy()
                df_norm.columns = [str(c).replace("\xa0", " ").replace("\u00A0", " ").strip() for c in df_norm.columns]
                E = pd.to_numeric(df_norm.get("FTD qty", 0.0), errors="coerce").fillna(0.0)
                K = pd.to_numeric(df_norm.get("Total Dep Amount", 0.0), errors="coerce").fillna(0.0)
                F_before = total_spend
                F_after = pd.to_numeric(alloc_vec, errors="coerce").reindex(df_norm.index).fillna(0.0)

                before_status = [_classify_status(float(E[i]), float(F_before[i]), float(K[i])) for i in df_norm.index]
                after_status = [_classify_status(float(E[i]), float(F_after[i]), float(K[i])) for i in df_norm.index]

                total_posE = int((E > 0).sum())
                yellow_after = sum(1 for s in after_status if s == "Yellow")
                green_to_yellow = sum(
                    1 for i in range(len(before_status)) if before_status[i] == "Green" and after_status[i] == "Yellow"
                )

                summary = (
                    "Режим: максимум жовтих (Total+% цілі) з доведенням до red-ceiling.\n"
                    f"Початковий бюджет (сума Total spend): {starting_budget:.2f}; розподілено: {used_budget:.2f}; невикористано: {unused_budget:.2f}\n"
                    f"Жовтих після розподілу: {yellow_after}/{total_posE} (зел.→жовт.: {green_to_yellow})"
                )

                bio = io.BytesIO()
                write_result_like_excel_with_new_spend(
                    bio,
                    state.alloc_df,
                    new_total_spend=alloc_vec,
                    overwrite_total_spend=True,
                )

                bio.seek(0)
                bot.send_document(
                    chat_id,
                    bio,
                    visible_file_name="allocation.xlsx",
                    caption=summary,
                )

                explanation = build_allocation_explanation(
                    state.alloc_df,
                    alloc_vec,
                    starting_budget,
                    max_lines=20,
                    alloc_is_delta=False,
                )
                bot.send_message(chat_id, explanation)

                state.phase = "WAIT_MAIN"
                state.alloc_df = None
                state.alloc_mode = None
                state.alloc_budget = None
            else:
                bot.reply_to(
                    message,
                    "Невідомий режим розподілу. Використай /allocate, щоб почати заново.",
                )
                state.phase = "WAIT_MAIN"
                state.alloc_df = None
                state.alloc_mode = None
                state.alloc_budget = None
        else:
            bot.reply_to(message, "⚠️ Несподівана фаза. Спробуйте ще раз із головної таблиці.")
    except ValueError as ve:
        # Catch wrong structure/columns
        bot.reply_to(
            message,
            (
                f"❌ Помилка у файлі <b>{filename}</b>:\n\n"
                f"<code>{ve}</code>\n\n"
                "Будь ласка, перевірте структуру таблиці та надішліть файл ще раз. "
                "Очікувані колонки:\n"
                "- Для головної таблиці: Назва Офферу, ГЕО, Загальні витрати\n"
                "- Для додаткових таблиць: Країна, Сума депозитів"
            ),
        )
        if state.phase in {"WAIT_ALLOC_RESULT", "WAIT_ALLOC_BUDGET", "WAIT_ALLOC_CHOICE"}:
            state.phase = "WAIT_MAIN"
            state.alloc_mode = None
            state.alloc_df = None
            state.alloc_budget = None
    except Exception as e:
        bot.reply_to(message, f"⚠️ Непередбачена помилка: <code>{e}</code>")
        if state.phase in {"WAIT_ALLOC_RESULT", "WAIT_ALLOC_BUDGET", "WAIT_ALLOC_CHOICE"}:
            state.phase = "WAIT_MAIN"
            state.alloc_mode = None
            state.alloc_df = None
            state.alloc_budget = None


@bot.message_handler(commands=["allocate"])
@require_access
def cmd_allocate(message: types.Message):
    chat_id = message.chat.id
    state = user_states.setdefault(chat_id, UserState())
    state.phase = "WAIT_ALLOC_CHOICE"
    state.alloc_df = None
    state.alloc_budget = None
    state.alloc_mode = None

    keyboard = types.InlineKeyboardMarkup()
    keyboard.row(
        types.InlineKeyboardButton("📊 За бюджетом", callback_data="alloc_mode_budget"),
        types.InlineKeyboardButton("💛 Максимум жовтих (Total+%)", callback_data="alloc_mode_max_yellow"),
    )

    bot.reply_to(
        message,
        "Оберіть режим розподілу бюджету:",
        reply_markup=keyboard,
    )


@bot.callback_query_handler(func=lambda c: c.data in {"alloc_mode_budget", "alloc_mode_max_yellow"})
@require_access_cb
def on_allocate_mode(call: types.CallbackQuery):
    chat_id = call.message.chat.id
    state = user_states.setdefault(chat_id, UserState())

    if call.data == "alloc_mode_budget":
        state.alloc_mode = "budget"
        prompt = (
            "Режим <b>за бюджетом</b> обрано. Надішліть файл <b>result.xlsx</b>. "
            "Після завантаження попрошу вказати бюджет (Spend)."
        )
    else:
        state.alloc_mode = "max_yellow"
        prompt = (
            "Режим <b>максимум жовтих</b> (цілі з <code>Total+%</code>) обрано. Надішліть файл <b>result.xlsx</b>. "
            "Бюджет береться з колонок <code>Total+%</code>, тож нічого вводити вручну не потрібно."
        )

    state.phase = "WAIT_ALLOC_RESULT"
    state.alloc_df = None
    state.alloc_budget = None

    try:
        bot.answer_callback_query(call.id, "Режим застосовано.")
    except Exception:
        pass

    bot.send_message(chat_id, prompt)


@bot.message_handler(content_types=["text"], func=lambda m: not (m.text or "").startswith("/"))
@require_access
def on_text(message: types.Message):
    chat_id = message.chat.id
    state = user_states.setdefault(chat_id, UserState())

    # Only intercept when waiting for budget
    if state.phase != "WAIT_ALLOC_BUDGET":
        return

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
        state.alloc_mode = None
        state.alloc_budget = None
        return

    # Compute allocation
    alloc_df, summary, alloc_vec = compute_optimal_allocation(state.alloc_df, budget)

    # Build allocation.xlsx with the SAME structure as result.xlsx,
    # applying the new Total spend (= old F + allocated extra)
    bio = io.BytesIO()
    write_result_like_excel_with_new_spend(bio, state.alloc_df, new_total_spend=alloc_vec)

    bio.seek(0)
    bot.send_document(
        chat_id,
        bio,
        visible_file_name="allocation.xlsx",
        caption=summary  # короткий підсумок
    )

    # ДЕТАЛЬНЕ ПОЯСНЕННЯ: куди пішов бюджет, статуси ДО/ПІСЛЯ, нові H і L
    explanation = build_allocation_explanation(state.alloc_df, alloc_vec, budget, max_lines=20)
    bot.send_message(chat_id, explanation)

    # reset phase (or keep?)
    state.phase = "WAIT_MAIN"
    state.alloc_mode = None
    state.alloc_df = None
    state.alloc_budget = None


@bot.callback_query_handler(func=lambda c: c.data == "skip_offer")
@require_access
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
            if final_df.empty:
                bot.send_message(
                    chat_id,
                    "ℹ️ Після пропуску всіх оферів не залишилось даних для експорту.",
                )
            else:
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
            ws[f"I{r}"].value = CPA_TARGET_DEFAULT  # CPA Target
            ws[f"J{r}"].value = f"=IFERROR(K{r}/E{r},\"\")"  # СP/Ч
            ws[f"L{r}"].value = f"=IFERROR(K{r}/G{r}*100,0)"  # My deposit amount
            ws[f"M{r}"].value = f"=G{r}*0.4"  # Profit 40%
            ws[f"N{r}"].value = f"=G{r}*0.5"  # Profit 50%

        # ---- NEW: number formats to 2 decimals everywhere needed ----
        # Integers: E (FTD qty)
        for r in range(first_row, last_row + 1):
            ws[f"E{r}"].number_format = "0"

        # Two decimals: F..N except I (integer target column)
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

        # Conditional formatting за новими правилами
        data_range = f"A{first_row}:P{last_row}"
        grey = PatternFill("solid", fgColor="BFBFBF")
        green = PatternFill("solid", fgColor="C6EFCE")
        yellow = PatternFill("solid", fgColor="FFEB9C")
        red = PatternFill("solid", fgColor="FFC7CE")

        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=["$E2=0"], fill=grey, stopIfTrue=True),
        )
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=[f"AND($E2>0,$H2<=INT($I2),$L2>{DEPOSIT_GREEN_MIN:.0f})"], fill=green, stopIfTrue=True),
        )
        yellow_formula = _build_yellow_formula()
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=[yellow_formula], fill=yellow, stopIfTrue=True),
        )
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(
                formula=[
                    f"AND($E2>0,$H2>=$I2*{YELLOW_MULT:.2f},$H2<=$I2*{RED_MULT:.2f})"
                ],
                fill=red,
                stopIfTrue=True,
            ),
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
