import os
import io
import re
import unicodedata
from typing import Optional, Dict, List, Set
from dotenv import load_dotenv

from telebot import TeleBot, types
from openai import OpenAI  # залишив, якщо десь у проєкті ще використовуєте
import pandas as pd
from openpyxl.utils import get_column_letter

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# admins by username (from .env: ADMIN_USERNAMES=nick1,nick2)
ADMIN_USERNAMES = {
    u.strip().lstrip("@")
    for u in (os.getenv("ADMIN_USERNAMES") or "").split(",")
    if u.strip()
}

# allowed user ids — тільки з .env
# .env: ALLOWED_USER_IDS=155840708,123456789
_raw_allowed = os.getenv("ALLOWED_USER_IDS", "")
ALLOWED_USER_IDS: Set[int] = set()
for part in _raw_allowed.split(","):
    part = part.strip()
    if not part:
        continue
    try:
        ALLOWED_USER_IDS.add(int(part))
    except ValueError:
        # якщо раптом там щось не число — пропускаємо
        pass

bot = TeleBot(BOT_TOKEN, parse_mode="HTML")
client = OpenAI(api_key=OPENAI_API_KEY)


# ===================== NORMALIZATION (countries) =====================
def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip().lower()


def build_country_map_uk_to_en() -> Dict[str, str]:
    m = {
        "Аргентина": "Argentina",
        "Бенін": "Benin",
        "Буркіна-Фасо": "Burkina Faso",
        "Венесуела": "Venezuela",
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
        "Конго": "Congo (Brazzaville)",

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
        "Пакистан": "Pakistan",
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


def build_country_map_ru_to_en() -> Dict[str, str]:
    m = {
        "Аргентина": "Argentina",
        "Бенин": "Benin",
        "Буркина-Фасо": "Burkina Faso",
        "Венесуэла": "Venezuela",
        "Габон": "Gabon",
        "Гаити": "Haiti",
        "Гана": "Ghana",

        # Как и в UA-карте: "Гвинея" → "Guinea-Conakry" (далее канонизируем в "Guinea")
        "Гвинея": "Guinea-Conakry",

        # ---- DRC/ROC ----
        "Демократическая Республика Конго": "Congo (Kinshasa)",
        "ДР Конго": "Congo (Kinshasa)",
        "Конго (Киншаса)": "Congo (Kinshasa)",
        "Республика Конго": "Congo (Brazzaville)",
        "Конго-Браззавиль": "Congo (Brazzaville)",
        # если просто "Конго" — как в UA-карте используем Brazzaville
        "Конго": "Congo (Brazzaville)",

        "Камерун": "Cameroon",

        # Кот-д’Ивуар — разные апострофы/пробелы
        "Кот-д'Ивуар": "Cote d'Ivoire",
        "Кот-д’Ивуар": "Cote d'Ivoire",
        "Кот д’Ивуар": "Cote d'Ivoire",

        "Кения": "Kenya",
        "Сенегал": "Senegal",
        "Сьерра-Леоне": "Sierra Leone",
        "Танзания": "Tanzania",
        "Того": "Togo",
        "Уганда": "Uganda",
        "Замбия": "Zambia",
        "Эфиопия": "Ethiopia",
        "Нигер": "Niger",
        "Нигерия": "Nigeria",
        "Мали": "Mali",
        "Пакистан": "Pakistan",
        "Казахстан": "Kazakhstan",
        "Испания": "Spain",
        "Франция": "France",
        "Италия": "Italy",
        "Португалия": "Portugal",
        "Доминиканская Республика": "Dominican Republic",
        "Канада": "Canada",
        "Филиппины": "Philippines",

        # Латам (как в UA-карте)
        "Боливия": "Bolivia",
        "Эквадор": "Ecuador",
        "Колумбия": "Colombia",
        "Парагвай": "Paraguay",
        "Перу": "Peru",
    }
    return {normalize_text(k): v for k, v in m.items()}


def build_country_canonical() -> Dict[str, str]:
    canon = {
        # самоканонічні EN
        "Argentina": "Argentina",
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
        "Venezuela": "Venezuela",
        "Zambia": "Zambia",
        "Ethiopia": "Ethiopia",
        "Niger": "Niger",
        "Nigeria": "Nigeria",
        "Mali": "Mali",
        "Pakistan": "Pakistan",
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


def to_canonical_en(
        country: str,
        uk_to_en: Dict[str, str],
        canonical: Dict[str, str],
        ru_to_en: Optional[Dict[str, str]] = None,
) -> str:
    key = normalize_text(country)

    # 1) UA → EN
    if key in uk_to_en:
        mapped = uk_to_en[key]
        return canonical.get(normalize_text(mapped), mapped)

    # 2) RU → EN (резерв, якщо не знайшли в UA)
    if ru_to_en and key in ru_to_en:
        mapped = ru_to_en[key]
        return canonical.get(normalize_text(mapped), mapped)

    # 3) Уже EN/варіант — канонізуємо
    if key in canonical:
        return canonical[key]

    # 4) Спецвипадки
    if key in {"конго", "congo"}:
        return "Congo (Kinshasa)"

    # 5) Як є (не впізнали)
    return country


# ---- країни (для unite_geo) ----
COUNTRY_EQUIV_UK_EN = {
    "гана": "Ghana", "ефіопія": "Ethiopia", "кенія": "Kenya", "колумбія": "Colombia",
    "кот-д’івуар": "Côte d'Ivoire", "кот-д'івуар": "Côte d'Ivoire", "кот д’івуар": "Côte d'Ivoire",
    "кот д'івуар": "Côte d'Ivoire",
    "парагвай": "Paraguay", "танзанія": "Tanzania", "конго": "Congo",
    "демократична республіка конго": "Democratic Republic of the Congo",
    "республіка конго": "Congo", "венесуела": "Venezuela", "греція": "Greece", "іспанія": "Spain", "італія": "Italy",
    "казахстан": "Kazakhstan", "канада": "Canada", "малайзія": "Malaysia", "малі": "Mali", "португалія": "Portugal",
    "сенегал": "Senegal", "таджикистан": "Tajikistan", "таїланд": "Thailand", "туреччина": "Turkey",
    "узбекистан": "Uzbekistan",
    "франція": "France", "шрі-ланка": "Sri Lanka", "в'єтнам": "Vietnam",
}
COUNTRY_EQUIV_RU_EN = {
    "гана": "Ghana", "эфиопия": "Ethiopia", "кения": "Kenya", "колумбия": "Colombia",
    "кот-д'ивуар": "Côte d'Ivoire", "кот д'ивуар": "Côte d'Ivoire", "кот-д’ивуар": "Côte d'Ivoire",
    "парагвай": "Paraguay", "танзания": "Tanzania", "конго": "Congo",
    "демократическая республика конго": "Democratic Republic of the Congo",
    "республика конго": "Congo", "венесуэла": "Venezuela", "греция": "Greece", "испания": "Spain", "италия": "Italy",
    "казахстан": "Kazakhstan", "канада": "Canada", "малайзия": "Malaysia", "мали": "Mali", "португалия": "Portugal",
    "сенегал": "Senegal", "таджикистан": "Tajikistan", "таиланд": "Thailand", "турция": "Turkey",
    "узбекистан": "Uzbekistan",
    "франция": "France", "шри-ланка": "Sri Lanka", "вьетнам": "Vietnam",
}
COUNTRY_CANON = {
    "ghana": "Ghana", "ethiopia": "Ethiopia", "kenya": "Kenya", "colombia": "Colombia",
    "cote d'ivoire": "Côte d'Ivoire", "côte d'ivoire": "Côte d'Ivoire", "paraguay": "Paraguay", "tanzania": "Tanzania",
    "democratic republic of the congo": "Democratic Republic of the Congo", "republic of the congo": "Congo",
    "congo": "Congo",
    "venezuela": "Venezuela", "greece": "Greece", "spain": "Spain", "italy": "Italy", "kazakhstan": "Kazakhstan",
    "canada": "Canada", "malaysia": "Malaysia", "mali": "Mali", "portugal": "Portugal", "senegal": "Senegal",
    "tajikistan": "Tajikistan", "thailand": "Thailand", "turkey": "Turkey", "uzbekistan": "Uzbekistan",
    "france": "France", "sri lanka": "Sri Lanka", "viet nam": "Vietnam", "vietnam": "Vietnam",
}


def build_country_map_uk_to_en(): return COUNTRY_EQUIV_UK_EN.copy()


def build_country_map_ru_to_en(): return COUNTRY_EQUIV_RU_EN.copy()


def build_country_canonical():    return COUNTRY_CANON.copy()


def _normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def normalize_text(s: str) -> str:
    if not s: return ""
    s = s.lower().strip()
    s = unicodedata.normalize("NFKD", s)
    s = re.sub(r"[^a-zа-яіїєґ0-9\s\-'’]", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def is_user_allowed(user_id: int, username: Optional[str]) -> bool:
    # 1) явно дозволені ID з .env
    if user_id in ALLOWED_USER_IDS:
        return True
    # 2) адміни по username
    if username and username in ADMIN_USERNAMES:
        return True
    return False


def _deny_access_message():
    return "⛔ <b>Доступ заборонено.</b>\nЯкщо вам потрібен доступ — зверніться до адміністратора бота."


def require_access(func):
    def wrapper(message, *args, **kwargs):
        user_id = message.from_user.id
        username = (message.from_user.username or "")
        if not is_user_allowed(user_id, username):
            bot.reply_to(message, _deny_access_message())
            return
        return func(message, *args, **kwargs)

    return wrapper


def require_access_cb(func):
    def wrapper(call, *args, **kwargs):
        user_id = call.from_user.id
        username = (call.from_user.username or "")
        if not is_user_allowed(user_id, username):
            bot.answer_callback_query(call.id, _deny_access_message(), show_alert=True)
            return
        return func(call, *args, **kwargs)

    return wrapper


class UserState:
    def __init__(self):
        # покроковий флоу під /start
        self.phase: str = "IDLE"  # IDLE -> WAIT_CURRENT -> WAIT_SUBID -> WAIT_ADDITIONAL -> DONE
        self.flow_active: bool = False

        self.main_agg_df: Optional[pd.DataFrame] = None
        self.offers: List[str] = []
        self.current_offer_index: int = 0
        self.offer_deposits: Dict[str, Dict[str, Dict[str, float]]] = {}

        # SubID фільтр: None -> всі, set() -> використаємо всі (технічно None),
        # непорожня множина -> фільтруємо по множині
        self.subid_filters: Optional[Set[str]] = None

        # unite_geo
        self.country_map_uk_to_en = build_country_map_uk_to_en()
        self.country_map_ru_to_en = build_country_map_ru_to_en()
        self.country_canon = build_country_canonical()

        self.unite_country_col = "ГЕО"
        self.unite_spend_col = "Total spend"

    def reset_for_flow(self):
        self.phase = "WAIT_CURRENT"
        self.flow_active = True
        self.main_agg_df = None
        self.offers = []
        self.current_offer_index = 0
        self.offer_deposits = {}
        self.subid_filters = None

    def finish_flow(self):
        self.phase = "IDLE"
        self.flow_active = False


user_states: Dict[int, UserState] = {}


# ---------- Команди ----------

@bot.message_handler(commands=["start"])
def cmd_start(message: types.Message):
    if not is_user_allowed(message.from_user.id, message.from_user.username or ""):
        bot.reply_to(message, _deny_access_message())
        return

    st = user_states.setdefault(message.chat.id, UserState())
    st.reset_for_flow()
    bot.reply_to(
        message,
        "👋 Стартуємо покроково:\n"
        "1) Надішліть <b>основну</b> таблицю (.xlsx/.xls/.csv) з колонками: офер, гео, сума депозитів."
    )


@bot.message_handler(commands=["unite_geo"])
@require_access
def cmd_unite_geo(message: types.Message):
    st = user_states.setdefault(message.chat.id, UserState())
    st.phase = "WAIT_UNITE_TABLE"
    bot.reply_to(
        message,
        "Надішліть таблицю (.xlsx/.xls/.csv), де треба уніфікувати назви країн (UA/RU → EN → канон)."
    )


@bot.message_handler(commands=["help"])
@require_access
def cmd_help(message: types.Message):
    bot.reply_to(
        message,
        "<b>Доступні сценарії:</b>\n"
        "• <b>/start</b> — покроково: основна таблиця → SubID (або всі) → додаткові таблиці → результат.\n"
        "• <b>/unite_geo</b> — уніфікація назв країн у будь-якій таблиці."
    )


# ---------- Утиліти читання ----------
def _looks_like_header(cols: list[str]) -> bool:
    wanted = {"гео", "geo", "country", "країна", "страна"}
    normed = {str(c).strip().lower() for c in cols}
    return bool(normed & wanted)


def _read_excel_auto_header(bio: io.BytesIO) -> pd.DataFrame:
    """
    Шукаємо рядок із заголовками в перших 15 рядках.
    Це потрібно для звітів, де колонки починаються з 8-го рядка і нижче.
    """
    for header_row in range(15):  # 👈 було 5, стало 15
        bio.seek(0)
        df_try = pd.read_excel(bio, header=header_row)
        if _looks_like_header(list(df_try.columns)):
            return df_try

    # якщо так і не знайшли — беремо як є
    bio.seek(0)
    return pd.read_excel(bio)


def _read_csv_auto_header(bio: io.BytesIO) -> pd.DataFrame:
    for header_row in range(15):  # 👈 теж до 15
        bio.seek(0)
        df_try = pd.read_csv(bio, header=header_row)
        if _looks_like_header(list(df_try.columns)):
            return df_try
    bio.seek(0)
    return pd.read_csv(bio)


def _ensure_series(df: pd.DataFrame, col: str) -> Optional[pd.Series]:
    """Повертає Series навіть якщо назва колонки дублювалась і df[col] — DataFrame."""
    if col not in df.columns:
        return None
    obj = df[col]
    if isinstance(obj, pd.DataFrame):
        # взяти перший стовпець з таким ім’ям
        return obj.iloc[:, 0]
    return obj


def extract_month_series(df: pd.DataFrame) -> pd.Series:
    """
    Повертає Series з місяцями (1..12), використовуючи, у такому порядку:
    1) готову колонку 'Місяць', якщо є;
    2) дату в 'Дата';
    3) дату в 'Дата_2' (часто формат dd.mm.yyyy).
    """
    # 1) 'Місяць'
    s = _ensure_series(df, "Місяць")
    if s is not None:
        # часто це float типу 7.0 → 7
        return pd.to_numeric(s, errors="coerce").round().astype("Int64")

    # 2) 'Дата' (часто dd/mm/yyyy)
    s = _ensure_series(df, "Дата")
    if s is not None:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if dt.notna().any():
            return dt.dt.month.astype("Int64")

    # 3) 'Дата_2' (часто dd.mm.yyyy)
    s = _ensure_series(df, "Дата_2")
    if s is not None:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if dt.notna().any():
            return dt.dt.month.astype("Int64")

    # якщо нічого не вийшло — повертаємо все NaN
    return pd.Series([pd.NA] * len(df), index=df.index, dtype="Int64")


def filter_main_by_month(df: pd.DataFrame, month_num: int) -> pd.DataFrame:
    m = extract_month_series(df)
    keep = (m == int(month_num))
    out = df.loc[keep.fillna(False)].copy()
    return out


def read_main_table(file_bytes: bytes, filename: str) -> pd.DataFrame:
    bio = io.BytesIO(file_bytes)
    fname = (filename or "").lower()

    if fname.endswith((".xlsx", ".xls", ".xlsm")):
        df = _read_excel_auto_header(bio)
    elif fname.endswith(".csv"):
        df = _read_csv_auto_header(bio)
    else:
        raise ValueError("Невідомий формат файлу. Використайте .xlsx/.xls/.xlsm/.csv")

    print("=== MAIN RAW COLUMNS ===")
    print(list(df.columns))
    print("=== MAIN RAW HEAD ===")
    try:
        print(df.head(10))
    except Exception:
        pass

    possible_geo    = ["ГЕО", "Гео", "geo", "Geo", "Country", "Країна", "Страна"]
    possible_offer  = ["Назва Офферу", "Офер", "Offer", "Оффер", "Оферр", "Назва оферу"]
    possible_spend  = ["Total spend", "Total Spend", "Загальні витрати", "Загальні витрати ", "Загальні витрати, $"]
    partner_cols    = ["Партнер", "Партнер ", "Partner"]
    possible_date   = ["Дата", "дата", "Date", "date"]
    possible_month  = ["Місяць", "Месяц", "month", "Month"]

    if not any(col in df.columns for col in possible_geo):
        raise ValueError(f"Не знайдено колонку з гео (ГЕО/Гео/Country/Країна/Страна). Знайшов: {list(df.columns)}")
    if not any(col in df.columns for col in possible_offer):
        raise ValueError("Не знайдено колонку з назвою оферу")

    # --- акуратне перейменування, щоб не створити два 'Дата'
    rename_map = {}
    date_hits = []  # зберемо всі колонки, схожі на дату
    for c in df.columns:
        c_norm = str(c).strip()
        if c_norm in possible_geo:
            rename_map[c] = "ГЕО"
        elif c_norm in possible_offer:
            rename_map[c] = "Назва Офферу"
        elif c_norm in possible_spend:
            rename_map[c] = "Total spend"
        elif c_norm in partner_cols:
            rename_map[c] = "Партнер"
        elif c_norm in possible_date:
            date_hits.append(c)
        elif c_norm in possible_month:
            rename_map[c] = "Місяць"

    # першу дату назвемо "Дата", другу — "Дата_2"
    if date_hits:
        rename_map[date_hits[0]] = "Дата"
        if len(date_hits) > 1:
            rename_map[date_hits[1]] = "Дата_2"

    df = df.rename(columns=rename_map)

    # усунути точні дублікати назв колонок (залишаємо перше входження)
    df = df.loc[:, ~df.columns.duplicated()]

    # формуємо список колонок, які хочемо зберегти
    keep_cols = ["Назва Офферу", "ГЕО"]
    if "Total spend" in df.columns: keep_cols.append("Total spend")
    if "Партнер" in df.columns:     keep_cols.append("Партнер")
    if "Дата" in df.columns:         keep_cols.append("Дата")
    if "Дата_2" in df.columns:       keep_cols.append("Дата_2")
    if "Місяць" in df.columns:       keep_cols.append("Місяць")

    df = df[keep_cols]

    print("=== MAIN AFTER RENAME/KEEP ===")
    print(list(df.columns))
    try:
        print(df.head(10))
    except Exception:
        pass

    return df


def read_additional_table(file_bytes: bytes, filename: str) -> pd.DataFrame:
    bio = io.BytesIO(file_bytes)
    fname = (filename or "").lower()

    if fname.endswith((".xlsx", ".xls", ".xlsm")):
        df = _read_excel_auto_header(bio)
    elif fname.endswith(".csv"):
        df = _read_csv_auto_header(bio)
    else:
        raise ValueError("Невідомий формат файлу. Використайте .xlsx/.xls/.xlsm/.csv")

    # тепер нормалізуємо назви
    rename_map = {}
    for c in df.columns:
        c_str = str(c).strip()  # 👈 важливо: обрізаємо пробіли
        low = c_str.lower()
        if c_str in ("Country", "Країна", "Страна", "ГЕО", "Гео") or low in (
        "country", "країна", "страна", "гео", "geo"):
            rename_map[c] = "ГЕО"
        elif c_str in ("Сума депозитів", "Сумма депозитов") or low in ("total dep amount", "dep amount", "deposits"):
            rename_map[c] = "Сума депозитів"
        elif low in ("subid", "sub_id", "sub id"):
            rename_map[c] = "SubID"

    df = df.rename(columns=rename_map)
    return df


# ---------- Флоу-питання ----------

def ask_subids(message: types.Message):
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Взяти всі SubID", callback_data="subid_all"))
    bot.send_message(
        message.chat.id,
        "Вкажіть, будь ласка, <b>SubID</b> (один або кілька) для врахування у <b>додаткових</b> таблицях.\n"
        "• Можна через кому/крапку з комою/пробіли: <code>123,456; 789</code>\n"
        "• Або натисніть «Взяти всі SubID».",
        reply_markup=kb,
    )


def ask_additional_table_with_skip(chat_id: int, state: UserState):
    offer = state.offers[state.current_offer_index]
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("Пропустити цей офер", callback_data="skip_offer"))

    subid_info = ""
    if state.subid_filters:
        subid_info = "\n(фільтр SubID: <b>" + ", ".join(sorted(state.subid_filters)) + "</b>)"

    bot.send_message(
        chat_id,
        f"Надішліть <b>додаткову</b> таблицю для оферу: <b>{offer}</b>{subid_info}\n\n"
        "Очікувані колонки: <b>ГЕО</b>, <b>Сума депозитів</b>"
        + (" і <b>SubID</b>." if state.subid_filters else ".")
        + "\nАбо натисніть «Пропустити цей офер».",
        reply_markup=kb,
    )


# ---------- Побудова результату ----------

def build_final_output(state: UserState) -> pd.DataFrame:
    uk_map = state.country_map_uk_to_en
    ru_map = state.country_map_ru_to_en
    canon_map = state.country_canon

    def canon_geo(val: str) -> str:
        return to_canonical_en(val, uk_map, canon_map, ru_map)

    # 1) додаткові (те, що зібрали з файлів)
    extra_rows = []
    for offer, geo_map in state.offer_deposits.items():
        for geo_raw, data in geo_map.items():
            extra_rows.append({
                "Назва Офферу": offer,
                "ГЕО": geo_raw,
                "Total Dep Amount": float(data.get("sum_dep", 0.0) or 0.0),
                "FTD qty": int(data.get("ftd_qty", 0) or 0),
            })

    if not extra_rows:
        template_cols = [
            "Subid","Offer ID","Назва Офферу","ГЕО","FTD qty","Total spend","Total+%",
            "CPA","CPA Target","СP/Ч","Total Dep Amount","My deposit amount",
            "C. profit Target 40%","C. profit Target 50%","CAP","Остаток CAP","Current",
        ]
        return pd.DataFrame(columns=template_cols)

    df_extra = pd.DataFrame(extra_rows)
    print("=== EXTRA BEFORE CANON ===")
    print(df_extra)

    # канонізуємо ГЕО у додаткових
    df_extra["ГЕО"] = df_extra["ГЕО"].astype(str).map(canon_geo)

    # якщо прийшло по кілька разів той самий офер+гео — згортаємо
    df_extra = (
        df_extra.groupby(["Назва Офферу", "ГЕО"], as_index=False)
                .agg({"Total Dep Amount": "sum", "FTD qty": "sum"})
    )
    print("=== EXTRA AFTER GROUP+CANON ===")
    print(df_extra)

    # 2) main (таблиця бюджету)
    df_main = state.main_agg_df.copy()
    if "Total spend" not in df_main.columns:
        df_main["Total spend"] = 0.0
    if "Партнер" not in df_main.columns:
        df_main["Партнер"] = ""

    print("=== MAIN RAW IN build_final_output ===")
    print(df_main.head(30))

    # канонізуємо ГЕО
    df_main["ГЕО"] = df_main["ГЕО"].astype(str).map(canon_geo)

    # 🔧 нормалізація Total spend — в тебе воно типу "11,69"
    df_main["Total spend"] = (
        df_main["Total spend"]
        .astype(str)
        .str.replace(" ", "", regex=False)
        .str.replace("\u00a0", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df_main["Total spend"] = pd.to_numeric(df_main["Total spend"], errors="coerce").fillna(0.0)

    # ❗ головне: агрегуємо по ГЕО (а не по оферу), бо в main у тебе офери NaN
    df_main_geo = (
        df_main.groupby("ГЕО", as_index=False)
               .agg({"Total spend": "sum", "Партнер": "first"})
    )
    print("=== MAIN GEO-AGG ===")
    print(df_main_geo)

    # 3) merge: тільки по ГЕО
    df = pd.merge(
        df_extra,
        df_main_geo,
        on="ГЕО",
        how="left",
    )
    print("=== MERGED BY GEO ONLY ===")
    print(df)

    # 4) бізнес-логіка
    partner = df["Партнер"].fillna("")
    total_spend = df["Total spend"].fillna(0.0)

    # покажемо ті, що не підвантажились
    zero_spend = df[total_spend == 0]
    if not zero_spend.empty:
        print("=== DEBUG: rows with Total spend == 0 AFTER GEO-MERGE ===")
        print(zero_spend[["Назва Офферу", "ГЕО", "Total spend"]])

    # Total+%: Melbet → +35%, інакше +30%
    # 1.30 = +30%, 1.35 = +35%; зробимо явніше
    is_melbet = partner.str.lower().str.strip().eq("melbet")
    total_plus = total_spend * (1.35 * is_melbet + 1.30 * (~is_melbet))

    ftd_qty = pd.to_numeric(df["FTD qty"].fillna(0), errors="coerce").fillna(0).astype(int)
    total_dep = pd.to_numeric(df["Total Dep Amount"].fillna(0), errors="coerce").fillna(0.0)

    # CPA
    cpa = (total_plus / ftd_qty.replace(0, pd.NA)).fillna(0.0)

    # CPA TARGET
    CPA_DEFAULT_TARGET = 8
    CPA_OVERRIDES: Dict[str, float] = {
        "Argentina": 20,
        "Bolivia": 15,
        "Venezuela": 5,
        "Gabon": 7,
        "Ghana": 5,
        "Ecuador": 15,
        "Jordan": 40,
        "Iraq": 40,
        "Kazakhstan": 30,
        "Colombia": 11,
        "Malaysia": 40,
        "Paraguay": 15,
        "Pakistan": 15,
        "Peru": 12,
        "Thailand": 22,
        "Uruguay": 12,
        "Philippines": 10,
    }
    cpa_target = df["ГЕО"].map(CPA_OVERRIDES).fillna(CPA_DEFAULT_TARGET)

    # СP/Ч = total_dep / ftd_qty
    cp_per = (total_dep / ftd_qty.replace(0, pd.NA)).fillna(0.0)

    # My deposit amount = Total Dep Amount / Total+% * 100
    my_dep_amount = (total_dep / total_plus.replace(0, pd.NA) * 100.0).fillna(0.0)

    # C. profit ...
    c_profit_40 = total_plus * 0.4
    c_profit_50 = total_plus * 0.5

    # 5) формуємо у потрібному порядку
    template_cols = [
        "Subid","Offer ID","Назва Офферу","ГЕО","FTD qty","Total spend","Total+%",
        "CPA","CPA Target","СP/Ч","Total Dep Amount","My deposit amount",
        "C. profit Target 40%","C. profit Target 50%","CAP","Остаток CAP","Current",
    ]
    out = pd.DataFrame(columns=template_cols)

    if state.subid_filters:
        out["Subid"] = ", ".join(sorted(state.subid_filters))
    else:
        out["Subid"] = ""

    out["Offer ID"] = df["Назва Офферу"]
    out["Назва Офферу"] = df["Назва Офферу"]
    out["ГЕО"] = df["ГЕО"]
    out["FTD qty"] = ftd_qty
    out["Total spend"] = total_spend
    out["Total+%"] = total_plus
    out["CPA"] = cpa
    out["CPA Target"] = cpa_target
    out["СP/Ч"] = cp_per
    out["Total Dep Amount"] = total_dep
    out["My deposit amount"] = my_dep_amount
    out["C. profit Target 40%"] = c_profit_40
    out["C. profit Target 50%"] = c_profit_50
    out["CAP"] = ""
    out["Остаток CAP"] = ""
    out["Current"] = ""

    return out[template_cols]


def send_final_table(chat_id: int, df: pd.DataFrame):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Result")
        ws = writer.sheets["Result"]
        for col_idx, col_name in enumerate(df.columns, start=1):
            max_len = max([len(str(col_name))] + [len(str(x)) for x in df[col_name]])
            ws.column_dimensions[get_column_letter(col_idx)].width = max_len + 2
    bio.seek(0)
    bot.send_document(chat_id, bio, visible_file_name="result.xlsx", caption="Готово ✅")


# ---------- Обробники ----------

@bot.message_handler(content_types=["document"])
@require_access
def on_document(message: types.Message):
    chat_id = message.chat.id
    st = user_states.setdefault(chat_id, UserState())

    filename = message.document.file_name or "file"
    file_info = bot.get_file(message.document.file_id)
    file_bytes = bot.download_file(file_info.file_path)

    try:
        if st.phase == "WAIT_CURRENT" and st.flow_active:
            df = read_main_table(file_bytes, filename)

            # 👇 тут ЗАПАМ’ЯТОВУЄМО сирі колонки дати
            st.main_agg_df = df  # як і було
            st.phase = "WAIT_MONTH"  # 👈 нова фаза!
            bot.reply_to(
                message,
                "✅ Основну таблицю зчитано. Вкажіть, будь ласка, номер місяця (1-12), наприклад 10."
            )
            return

        if st.phase == "WAIT_ADDITIONAL" and st.flow_active:
            # читаємо додаткову, фільтруємо за SubID (якщо задані), додаємо
            df_add = read_additional_table(file_bytes, filename)

            offer = st.offers[st.current_offer_index]
            geo_col, dep_col = "ГЕО", "Сума депозитів"

            if geo_col not in df_add.columns or dep_col not in df_add.columns:
                bot.reply_to(message,
                             "⚠️ У таблиці немає потрібних колонок (очікується ГЕО та Сума депозитів). Пропущено.")
            else:
                df_work = df_add.copy()
                # фільтр за SubID (якщо задані)
                if st.subid_filters:
                    if "SubID" not in df_work.columns:
                        bot.reply_to(message,
                                     "⚠️ Ви вказали SubID, але в цій додатковій таблиці немає колонки SubID. Таблицю пропущено.")
                        df_work = df_work.iloc[0:0]  # порожня
                    else:
                        df_work["__SubID__"] = df_work["SubID"].astype(str).str.strip()
                        df_work = df_work[df_work["__SubID__"].isin(st.subid_filters)]

                # sums + counts per GEO
                pair_sum: Dict[str, float] = {}
                pair_cnt: Dict[str, int] = {}

                for _, row in df_work.iterrows():
                    geo_raw = str(row.get(geo_col, "")).strip()
                    if not geo_raw:
                        continue

                    try:
                        dep_val = float(row.get(dep_col, 0))
                    except:
                        dep_val = 0.0

                    pair_sum[geo_raw] = pair_sum.get(geo_raw, 0.0) + dep_val
                    pair_cnt[geo_raw] = pair_cnt.get(geo_raw, 0) + 1

                if offer not in st.offer_deposits:
                    st.offer_deposits[offer] = {}

                for geo_raw in set(list(pair_sum.keys()) + list(pair_cnt.keys())):
                    prev = st.offer_deposits[offer].get(geo_raw, {"sum_dep": 0.0, "ftd_qty": 0})
                    prev["sum_dep"] = prev.get("sum_dep", 0.0) + pair_sum.get(geo_raw, 0.0)
                    prev["ftd_qty"] = prev.get("ftd_qty", 0) + pair_cnt.get(geo_raw, 0)
                    st.offer_deposits[offer][geo_raw] = prev

            # наступний офер або фінал
            st.current_offer_index += 1
            if st.current_offer_index < len(st.offers):
                ask_additional_table_with_skip(chat_id, st)
            else:
                try:
                    final_df = build_final_output(st)
                    send_final_table(chat_id, final_df)
                finally:
                    st.finish_flow()
            return

        if st.phase == "WAIT_UNITE_TABLE":
            # окремий режим /unite_geo
            df_in = read_additional_table(file_bytes, filename)  # читаємо як універсальну таблицю
            country_col = "ГЕО"
            if country_col not in df_in.columns:
                # підбираємо альтернативу
                for alt in ("Country", "Країна", "Страна", "Гео"):
                    if alt in df_in.columns:
                        df_in = df_in.rename(columns={alt: "ГЕО"})
                        country_col = "ГЕО"
                        break
            if country_col not in df_in.columns:
                bot.reply_to(message, "⚠️ Не знайдено колонку з країнами (ГЕО / Country / Країна / Страна).")
                return

            # без складної нормалізації — залишаємо мінімал
            bio_out = io.BytesIO()
            with pd.ExcelWriter(bio_out, engine="openpyxl") as writer:
                df_in.to_excel(writer, index=False, sheet_name="Unified")
            bio_out.seek(0)
            bot.send_document(chat_id, bio_out, visible_file_name="united_geo.xlsx", caption="Готово ✅")
            st.phase = "IDLE"
            return

        # якщо файл прийшов поза сценарієм
        bot.reply_to(message, "Я очікую файл у межах активного кроку. Натисніть /start, щоб почати сценарій спочатку.")
    except Exception as e:
        bot.reply_to(message, f"❌ Помилка читання файлу: <code>{e}</code>")


@bot.message_handler(func=lambda m: True, content_types=["text"])
@require_access
def on_text(message: types.Message):
    st = user_states.setdefault(message.chat.id, UserState())

    # 👇 новий крок — вибір місяця
    if st.flow_active and st.phase == "WAIT_MONTH":
        txt = (message.text or "").strip()
        try:
            month = int(txt)
            if not 1 <= month <= 12:
                raise ValueError
        except ValueError:
            bot.reply_to(message, "Введіть, будь ласка, число від 1 до 12, напр. 10.")
            return

        df = st.main_agg_df.copy()

        # локальний хелпер: гарантуємо Series навіть якщо назва колонки дублювалась
        def _ensure_series(dff, col):
            if col not in dff.columns:
                return None
            obj = dff[col]
            if isinstance(obj, pd.DataFrame):
                return obj.iloc[:, 0]
            return obj

        # Пробуємо в такому порядку: готова "Місяць" → "Дата" → "Дата_2" → "Column 1" → "Дата виклику"
        month_ser = None

        mon = _ensure_series(df, "Місяць")
        if mon is not None:
            month_ser = pd.to_numeric(mon, errors="coerce").round().astype("Int64")

        if month_ser is None or month_ser.isna().all():
            mask_total = pd.Series(False, index=df.index)
            for cand in ["Дата", "Дата_2", "Column 1", "Дата виклику"]:
                s = _ensure_series(df, cand)
                if s is None:
                    continue
                # парсимо dd/mm/yyyy, dd.mm.yyyy, dd-mm-yyyy; dayfirst=True
                s_str = s.astype(str).str.strip()  # тут уже точно Series
                parsed = pd.to_datetime(
                    s_str
                    .str.replace(".", "/", regex=False)
                    .str.replace("-", "/", regex=False),
                    format="%d/%m/%Y",
                    errors="coerce",
                )
                mask_total |= parsed.dt.month.eq(month)

            # Якщо знайшли дати через парсинг — просто відфільтровуємо.
            df = df[mask_total.fillna(False)].copy()
        else:
            # маємо числовий стовпець "Місяць"
            df = df[month_ser.eq(month).fillna(False)].copy()

        st.main_agg_df = df
        st.phase = "WAIT_SUBID"
        bot.reply_to(message, f"✅ Візьму тільки місяць {month}. Рядків лишилось: {len(df)}.")
        ask_subids(message)
        return

    # 👇 очікуємо введення SubID у флоу
    if st.flow_active and st.phase == "WAIT_SUBID":
        raw = (message.text or "").strip()
        if raw:
            # ділимо за комою/крапкою з комою/пробілами
            parts = re.split(r"[,\;\s]+", raw)
            parts = [p.strip() for p in parts if p.strip()]
            if parts:
                st.subid_filters = set(parts)
                st.phase = "WAIT_ADDITIONAL"

                # підготуємо список оферів із main
                offers_col = st.main_agg_df.get("Назва Офферу")
                if offers_col is not None:
                    st.offers = offers_col.dropna().astype(str).unique().tolist()
                else:
                    st.offers = []

                st.current_offer_index = 0
                bot.reply_to(message, "✅ Прийнято SubID: <b>" + ", ".join(parts) + "</b>")
                ask_additional_table_with_skip(message.chat.id, st)
                return

        # якщо пусто — візьмемо всі
        st.subid_filters = None
        st.phase = "WAIT_ADDITIONAL"

        offers_col = st.main_agg_df.get("Назва Офферу")
        if offers_col is not None:
            st.offers = offers_col.dropna().astype(str).unique().tolist()
        else:
            st.offers = []

        st.current_offer_index = 0
        bot.reply_to(message, "Візьму <b>всі</b> SubID.")
        ask_additional_table_with_skip(message.chat.id, st)
        return

    # інші тексти
    if not st.flow_active:
        bot.reply_to(message, "Спробуйте /start для повного сценарію або /unite_geo для уніфікації країн.")
    else:
        bot.reply_to(message, "Зараз триває сценарій. Дотримуйтесь підказок у чаті.")


@bot.callback_query_handler(func=lambda c: c.data == "skip_offer")
@require_access_cb
def on_skip_offer(call: types.CallbackQuery):
    st = user_states.setdefault(call.message.chat.id, UserState())
    if not (st.flow_active and st.phase == "WAIT_ADDITIONAL"):
        bot.answer_callback_query(call.id, "Немає активного кроку.")
        return

    # пропустити поточний офер
    st.current_offer_index += 1
    bot.answer_callback_query(call.id, "Офер пропущено.")
    if st.current_offer_index < len(st.offers):
        ask_additional_table_with_skip(call.message.chat.id, st)
    else:
        try:
            final_df = build_final_output(st)
            send_final_table(call.message.chat.id, final_df)
        finally:
            st.finish_flow()


@bot.callback_query_handler(func=lambda c: c.data == "subid_all")
@require_access_cb
def on_subid_all(call: types.CallbackQuery):
    st = user_states.setdefault(call.message.chat.id, UserState())
    if not (st.flow_active and st.phase == "WAIT_SUBID"):
        bot.answer_callback_query(call.id, "Немає активного кроку.")
        return
    st.subid_filters = None
    st.phase = "WAIT_ADDITIONAL"
    st.offers = st.main_agg_df["Назва Офферу"].dropna().astype(str).unique().tolist()
    st.current_offer_index = 0
    bot.answer_callback_query(call.id, "Беру всі SubID")
    bot.edit_message_text(
        "✅ Буду враховувати <b>всі</b> SubID. Тепер надішліть додаткові таблиці по оферах.",
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
    )
    ask_additional_table_with_skip(call.message.chat.id, st)


# ---------- main ----------

def main():
    print("Bot is polling...")
    bot.infinity_polling()


if __name__ == "__main__":
    main()
