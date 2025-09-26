import os
import io
import re
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv

import pandas as pd
from telebot import TeleBot, types
import numpy as np

load_dotenv()

# ===================== CONFIG =====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
MAIN_SHEET_NAME = "BUDG"  # read this sheet from the main file
ALLOWED_MAIN_COLUMNS = ["Назва Офферу", "ГЕО", "Загальні витрати"]
ADDITIONAL_REQUIRED_COLS = ["Країна", "Сума депозитів"]

bot = TeleBot(BOT_TOKEN, parse_mode="HTML")

H_THRESH = 9.0  # H <= 9 is "good"
L_THRESH = 39.99  # L > 39.99 is "good" (strict >)
CPA_CAP = 11.0
EPS = 1e-12
EPS_YEL = 1e-6


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
        if "Дата" in df.columns:
            df["Дата"] = pd.to_datetime(df["Дата"], format="%d/%m/%Y", errors="coerce")
            now = datetime.now()
            df = df[(df["Дата"].dt.month == now.month) & (df["Дата"].dt.year == now.year)]
        return df

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

    for required_col in ["FTD qty", "Total spend", "Total Dep Amount"]:
        _ensure_column(required_col, required=True)

    df["FTD qty"] = pd.to_numeric(df.get("FTD qty", 0), errors="coerce").fillna(0).astype(int)
    for col in ["Total spend", "Total Dep Amount"]:
        df[col] = pd.to_numeric(df.get(col, 0.0), errors="coerce").fillna(0.0).round(2)

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


def compute_allocation_max_yellow(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, pd.Series]:
    """
    Розрахунок режиму «максимум жовтих»:
      - шукаємо мінімальний додатковий spend, щоб перевести всі дозволені зелені рядки в жовті;
      - не насичуємо жовті понад це (тобто відсутній крок B з compute_optimal_allocation).
    """
    dfw = df.copy()

    E = pd.to_numeric(dfw["FTD qty"], errors="coerce").fillna(0.0)
    F = pd.to_numeric(dfw["Total spend"], errors="coerce").fillna(0.0)
    K = pd.to_numeric(dfw["Total Dep Amount"], errors="coerce").fillna(0.0)

    with np.errstate(divide='ignore', invalid='ignore'):
        H = 1.3 * F / E.replace(0, np.nan)
        L = 100.0 * K / (1.3 * F.replace(0, np.nan))

    F_at_H = H_THRESH * E / 1.3
    F_at_L = (100.0 * K) / (1.3 * L_THRESH)
    F_cap = CPA_CAP * E / 1.3

    grey_mask = (E <= 0)
    green_mask = (~grey_mask) & (H <= H_THRESH + EPS) & (L > L_THRESH + EPS)

    F_cross_H = F_at_H + EPS_YEL
    F_cross_L = F_at_L + EPS_YEL

    candidates = pd.DataFrame({
        "F_now": F,
        "F_cap": F_cap,
        "F_cross_H": F_cross_H,
        "F_cross_L": F_cross_L,
        "E": E,
        "K": K,
    })

    F_target = F.copy()

    for i in candidates[green_mask].index:
        Fi = float(candidates.at[i, "F_now"])
        Fcap = float(candidates.at[i, "F_cap"])
        Fh = float(candidates.at[i, "F_cross_H"])
        Fl = float(candidates.at[i, "F_cross_L"])
        Ei = float(E.at[i])
        Ki = float(K.at[i])

        options = []
        for Ft in (Fh, Fl):
            if np.isfinite(Ft) and Ft > Fi + EPS and Ft <= Fcap + EPS:
                Ht = 1.3 * Ft / Ei if Ei > 0 else float("inf")
                Lt = (100.0 * Ki) / (1.3 * Ft) if Ft > 0 else float("inf")
                is_red = (Ht > H_THRESH + EPS) and (Lt <= L_THRESH + EPS)
                if not is_red:
                    options.append(Ft)

        if options:
            F_target.at[i] = min(options)
        else:
            F_target.at[i] = Fi

    need_delta = (F_target - F).clip(lower=0.0)
    alloc = need_delta.where(green_mask, 0.0)

    F_final = F + alloc
    with np.errstate(divide='ignore', invalid='ignore'):
        H_final = 1.3 * F_final / E.replace(0, np.nan)
        L_final = 100.0 * K / (1.3 * F_final.replace(0, np.nan))

    still_green = (E > 0) & (H_final <= H_THRESH + EPS) & (L_final > L_THRESH + EPS)
    still_yellow = (E > 0) & (((H_final <= H_THRESH + EPS) | (L_final > L_THRESH + EPS)) & (~still_green))

    dfw["Allocated extra"] = alloc
    dfw["New Total spend"] = F_final
    dfw["Will be yellow"] = ["Yes" if x else "No" for x in still_yellow]

    total_posE = int((E > 0).sum())
    kept_yellow = int(still_yellow.sum())

    before_status = [_classify_status(float(E[i]), float(F[i]), float(K[i])) for i in dfw.index]
    after_status = [_classify_status(float(E[i]), float(F_final[i]), float(K[i])) for i in dfw.index]
    green_to_yellow = sum(
        1 for i in range(len(before_status)) if before_status[i] == "Green" and after_status[i] == "Yellow"
    )

    used = float(alloc.sum())

    summary = (
        "Режим: максимум жовтих\n"
        f"Додатковий spend для переведення зелених: {used:.2f}\n"
        f"Жовтих після розподілу: {kept_yellow}/{total_posE} (зел.→жовт.: {green_to_yellow})\n"
        f"Правила: додаємо мінімальний spend (CPA≤{CPA_CAP:g}), щоб перевести всі дозволені зелені в жовті."
    )

    return dfw, summary, alloc


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
            ws[f"I{r}"].value = 8  # CPA Target
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

        ws.conditional_formatting.add(data_range, FormulaRule(formula=["$E2=0"], fill=grey, stopIfTrue=True))
        ws.conditional_formatting.add(data_range,
                                      FormulaRule(formula=["AND($E2>0,$H2<=9,$L2>39.99)"], fill=green, stopIfTrue=True))
        ws.conditional_formatting.add(data_range, FormulaRule(formula=["AND($E2>0,OR($H2<=9,$L2>39.99))"], fill=yellow,
                                                              stopIfTrue=True))
        ws.conditional_formatting.add(data_range,
                                      FormulaRule(formula=["AND($E2>0,$H2>9,$L2<39.99)"], fill=red, stopIfTrue=True))


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
                _alloc_df, summary, alloc_vec = compute_allocation_max_yellow(state.alloc_df)

                bio = io.BytesIO()
                write_result_like_excel_with_new_spend(bio, state.alloc_df, new_total_spend=alloc_vec)

                bio.seek(0)
                bot.send_document(
                    chat_id,
                    bio,
                    visible_file_name="allocation.xlsx",
                    caption=summary,
                )

                used_budget = float(pd.to_numeric(alloc_vec, errors="coerce").fillna(0.0).sum())
                explanation = build_allocation_explanation(state.alloc_df, alloc_vec, used_budget, max_lines=20)
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
        types.InlineKeyboardButton("💛 Максимум жовтих", callback_data="alloc_mode_max_yellow"),
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
            "Режим <b>максимум жовтих</b> обрано. Надішліть файл <b>result.xlsx</b>. "
            "Цей режим не питатиме про бюджет — одразу перерахує мінімальний spend."
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
            ws[f"I{r}"].value = 8  # CPA Target
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

        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=["$E2=0"], fill=grey, stopIfTrue=True),
        )
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=["AND($E2>0,$H2<=9,$L2>39.99)"], fill=green, stopIfTrue=True),
        )
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=["AND($E2>0,OR($H2<=9,$L2>39.99))"], fill=yellow, stopIfTrue=True),
        )
        ws.conditional_formatting.add(
            data_range,
            FormulaRule(formula=["AND($E2>0,$H2>9,$L2<39.99)"], fill=red, stopIfTrue=True),
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
