import telebot
from telebot import types
import threading
import json
import os
from datetime import datetime, timedelta
import time
import traceback
import gspread
import csv
import io
from oauth2client.service_account import ServiceAccountCredentials

TOKEN = os.environ.get("BOT_TOKEN") or os.environ.get("TOKEN", "8579096962:AAHLE-OEdiNbmc7TydZ5uN5fM7kEJ1tecC4")
ADMINS = [8133757512, 522637522]
DATA_FILE = "data.json"

bot = telebot.TeleBot(TOKEN)
data_lock = threading.Lock()

PRESET_TIMES = [f"{h:02d}:00" for h in range(8, 21)]
RU_WEEKDAYS = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

WELCOME_TEXT = (
    "👋 Привет! Я — автоматический помощник для записи на приём к специалистам. Работаю 24/7.\n\n"
    "Я умею:\n"
    "- 📅 Записывать вас на удобное время;\n"
    "- 🔔 Напоминать за час до приёма;\n"
    "- 📨 Принимать запросы и предложения для админа;\n"
    "- ✉️ Пересылать сообщения специалистам — админ ответит вам напрямую;\n"
    "Нажмите «🔘 Начать» или отправьте /start, чтобы открыть меню.\n"
    "Если нужно — напишите «Запрос» или «Предложение», либо выберите специалиста из списка.\n\n"
    "✨ Я работаю круглосуточно, чтобы сделать запись проще и удобнее для вас!"
)

# Centralized message templates / UI texts
MSG = {
    "choose_specialist": "👥 Выберите специалиста\nНажмите на имя, чтобы открыть карточку, расписание и доступные услуги.",
    "welcome_short": "🔘 Главное меню — выберите действие или специалиста.",
    "no_slots": "— нет свободных слотов",
}

if not os.path.exists(DATA_FILE):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump({
                "specialists": ["Иванов Иван Иванович", "Петров Пётр Петрович", "Сидорова Анна Сергеевна"],
                "schedule": {},
                "records": {},
                "messages": [],
                "next_message_id": 1,
                "actions": [],
                "specialists_info": {},
                "users_settings": {}
        }, f, ensure_ascii=False, indent=2)

def load_data():
    with data_lock:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

def save_data(data):
    with data_lock:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    # Web-панель удалена: не выполняем экспорт admin_data.json

# Google Sheets integration
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID') or '1AYY_vvVCtqJvaQqjHtikAX5u-32_FIyPCs7dYvexFrs'
CREDENTIALS_FILE = os.environ.get('GOOGLE_CREDENTIALS_JSON') or 'credentials.json'

_gs_client = None
_sheet = None

def get_worksheet():
    global _gs_client, _sheet
    if _sheet is not None:
        return _sheet
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        _gs_client = gspread.authorize(creds)
        sh = _gs_client.open_by_key(SPREADSHEET_ID)
        _sheet = sh.sheet1
        try:
            ensure_headers(_sheet)
        except Exception:
            pass
        return _sheet
    except Exception as e:
        print("Failed to init Google Sheets:", e)
        return None

def ensure_headers(sheet):
    try:
        headers = sheet.row_values(1)
        if not headers or len(headers) < 6:
            header_row = ["ts", "type", "user_id", "username", "spec", "date", "time", "text", "msg_id"]
            sheet.insert_row(header_row, 1)
    except Exception as e:
        print("ensure_headers failed:", e)

def write_message_to_sheet(msg_obj):
    sheet = get_worksheet()
    if sheet is None:
        return
    try:
        ts = datetime.now().isoformat()
        row = [
            ts,
            msg_obj.get("tag", ""),
            msg_obj.get("from_id", ""),
            msg_obj.get("from_username", ""),
            msg_obj.get("spec", ""),
            "",
            "",
            msg_obj.get("text", ""),
            msg_obj.get("id", "")
        ]
        sheet.append_row(row, value_input_option='USER_ENTERED')
    except Exception as e:
        print("write_message_to_sheet failed:", e)

def write_record_to_sheet(rec):
    sheet = get_worksheet()
    if sheet is None:
        return
    try:
        ts = datetime.now().isoformat()
        row = [
            ts,
            "record",
            rec.get("user_id", ""),
            rec.get("username", ""),
            rec.get("spec", ""),
            rec.get("date", ""),
            rec.get("time", ""),
            "",
            ""
        ]
        sheet.append_row(row, value_input_option='USER_ENTERED')
    except Exception as e:
        print("write_record_to_sheet failed:", e)


def add_action_log(actor, text, details=None, data=None):
    """Добавляем запись в лог действий (видно админам)."""
    if data is None:
        data = load_data()
    actions = data.setdefault("actions", [])
    entry = {"ts": datetime.now().isoformat(), "actor": actor, "text": text}
    if details:
        entry["details"] = details
    actions.append(entry)
    save_data(data)
    try:
        write_message_to_sheet({"tag": "action", "from_id": actor or "system", "from_username": "action_log", "text": text})
    except Exception:
        pass


def show_action_log_admin(chat_id, last_n=30):
    data = load_data()
    actions = data.get("actions", [])[-last_n:]
    if not actions:
        bot.send_message(chat_id, "Журнал пуст.")
        return
    out_lines = ["📝 История действий:"]
    for a in reversed(actions):
        ts = a.get("ts", "?")
        actor = a.get("actor", "?")
        text = a.get("text", "")
        out_lines.append(f"— {text} ({actor} | {ts.split('T')[0]})")
    bot.send_message(chat_id, "\n".join(out_lines))


def show_weekly_free_slots(chat_id):
    data = load_data()
    today = datetime.today()
    week_slots = {}
    for i in range(7):
        d = today + timedelta(days=i)
        key = d.date().isoformat()
        slots_set = set()
        for spec, sched in data.get("schedule", {}).items():
            for t in sched.get(key, []):
                slots_set.add(t)
        slots = sorted(list(slots_set))
        weekday = RU_WEEKDAYS[d.weekday()]
        week_slots[weekday + f' {d.day:02d}.{d.month:02d}'] = slots

    out = "📅 Свободное время\n\n"
    for day, slots in week_slots.items():
        out += f"{day}:\n"
        if not slots:
            out += "— нет свободных слотов\n\n"
            continue
        for s in slots:
            out += f"— {s}\n"
        out += "\n"
    bot.send_message(chat_id, out)


def show_stats_admin(chat_id):
    data = load_data()
    today_iso = datetime.today().date().isoformat()
    recs = []
    for uid, arr in data.get("records", {}).items():
        for r in arr:
            recs.append((uid, r))
    total_records = len(recs)
    today_count = sum(1 for uid, r in recs if r.get("date") == today_iso)
    unique_clients = len(set(uid for uid, r in recs))
    # free slots this week
    now = datetime.today()
    free_slots = 0
    for i in range(7):
        d = now + timedelta(days=i)
        key = d.date().isoformat()
        for spec, sched in data.get("schedule", {}).items():
            free_slots += len(sched.get(key, []))
    unread_msgs = sum(1 for m in data.get("messages", []) if not m.get("answered") and m.get("tag") != "регистрация")
    out = (
        f"📊 Статистика\n\n"
        f"Записей за сегодня: {today_count}\n"
        f"Всего записей: {total_records}\n"
        f"Уникальных клиентов: {unique_clients}\n"
        f"Свободных слотов на этой неделе: {free_slots}\n"
        f"Непрочитанных сообщений: {unread_msgs}"
    )
    bot.send_message(chat_id, out)


def auto_clean_loop(days=30):
    """Удаляем записи старше `days` дней один раз в сутки."""
    while True:
        try:
            data = load_data()
            cutoff = (datetime.now() - timedelta(days=days)).date()
            removed = 0
            for uid in list(data.get("records", {}).keys()):
                recs = data.get("records", {}).get(uid, [])
                new_recs = []
                for r in recs:
                    try:
                        rdate = datetime.fromisoformat(r.get("date") + "T00:00").date()
                    except Exception:
                        # если не распарсилось — оставляем
                        new_recs.append(r)
                        continue
                    if rdate < cutoff:
                        removed += 1
                    else:
                        new_recs.append(r)
                if new_recs:
                    data["records"][uid] = new_recs
                else:
                    data["records"].pop(uid, None)
            if removed > 0:
                save_data(data)
                # уведомляем админов
                for adm in ADMINS:
                    try:
                        bot.send_message(adm, f"🧹 Автоочистка завершена.\nУдалено устаревших записей: {removed}")
                    except Exception:
                        pass
                add_action_log("system", f"Auto-clean removed {removed} records")
        except Exception:
            print("auto_clean_loop error:", traceback.format_exc())
        # Спим 24 часа
        time.sleep(24 * 3600)

def safe_edit_message(chat_id, message_id, text, reply_markup=None):
    try:
        bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=reply_markup)
    except Exception as e:
        print("safe_edit_message failed:", e)
        try:
            bot.send_message(chat_id, text, reply_markup=reply_markup)
        except Exception as e2:
            print("safe_edit fallback failed:", e2)
def get_username(user):
    if not user:
        return "unknown"
    parts = []
    if getattr(user, 'first_name', None):
        parts.append(user.first_name)
    if getattr(user, 'last_name', None):
        parts.append(user.last_name)
    name = " ".join(parts).strip()
    if getattr(user, 'username', None):
        uname = user.username
        if name:
            return f"{name} (@{uname})"
        return f"@{uname}"
    if name:
        return name
    return f"id{getattr(user, 'id', '?')}"
def remove_reply_kb():
    return types.ReplyKeyboardRemove()

def cancel_reply_kb():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("Отмена")
    return kb

def _extract_spec_name(button_text):
    """
    Возвращает имя специалиста, даже если на кнопке есть префикс-эмодзи.
    Пример: "👩‍⚕️ Иванов" -> "Иванов". Если текст не имеет префикса, возвращает как есть.
    """
    if not button_text:
        return button_text
    prefixes = ["👩‍⚕️ ", "👨‍⚕️ ", "👩‍⚕ ", "👨‍⚕ "]
    for p in prefixes:
        if button_text.startswith(p):
            return button_text[len(p):].strip()
    return button_text

def _build_admin_notify_kb():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(types.InlineKeyboardButton("📨 Посмотреть", callback_data="admin_view_messages"),
           types.InlineKeyboardButton("🗑 Удалить", callback_data="del_notify"))
    return kb


def ensure_user_settings(uid, data=None):
    if data is None:
        data = load_data()
    users = data.setdefault("users_settings", {})
    key = str(uid)
    if key not in users:
        users[key] = {"24h": True, "1h": True, "10m": True}
        save_data(data)
    return users[key]


def show_reminder_settings(chat_id, user_id, edit_message=False, message_id=None):
    data = load_data()
    settings = data.get("users_settings", {}).get(str(user_id))
    if not settings:
        settings = ensure_user_settings(user_id, data)
    kb = types.InlineKeyboardMarkup()
    def lbl(k):
        return ("✅ " if settings.get(k) else "❌ ") + ("За 24 часа" if k=="24h" else ("За 1 час" if k=="1h" else "За 10 минут"))
    kb.add(types.InlineKeyboardButton(lbl("24h"), callback_data=f"rem_toggle|24h"),
           types.InlineKeyboardButton(lbl("1h"), callback_data=f"rem_toggle|1h"))
    kb.add(types.InlineKeyboardButton(lbl("10m"), callback_data=f"rem_toggle|10m"))
    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_main"))
    text = "🔔 Настройки напоминаний\nВыберите, какие напоминания вы хотите получать:" 
    if edit_message and message_id:
        safe_edit_message(chat_id, message_id, text, reply_markup=kb)
    else:
        bot.send_message(chat_id, text, reply_markup=kb)


def show_specialist_card(chat_id, spec_idx):
    data = load_data()
    try:
        spec = data["specialists"][int(spec_idx)]
    except Exception:
        bot.send_message(chat_id, "Специалист не найден.")
        return
    info = data.get("specialists_info", {}).get(spec, {})
    title = f"👩‍⚕️ {spec}"
    out_lines = [title]
    # Optional fields — показываем только если они заданы
    if info.get("profession"):
        out_lines.append(info.get("profession"))
    if info.get("experience"):
        out_lines.append(f"Опыт: {info.get('experience')}")
    if info.get("works"):
        out_lines.append(f"График: {info.get('works')}")
    # Services — list
    services = info.get("services")
    if services:
        out_lines.append("\nУслуги:")
        for s in services:
            out_lines.append(f"• {s}")
    # Contacts
    contacts = info.get("contacts", {})
    if contacts:
        out_lines.append("\nКонтакты:")
        if contacts.get("tg"):
            out_lines.append(f"• Telegram: {contacts.get('tg')}")
        if contacts.get("phone"):
            out_lines.append(f"• Телефон: {contacts.get('phone')}")

    out = "\n".join(out_lines)

    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📅 Записаться", callback_data=f"choose|{spec_idx}"),
           types.InlineKeyboardButton("💬 Написать специалисту", callback_data=f"msg_to_spec|{spec_idx}"))
    # Если админ смотрит — дадим кнопку редактирования характеристик
    try:
        if chat_id in ADMINS:
            kb.add(types.InlineKeyboardButton("⚙️ Добавить/изменить характеристики", callback_data=f"spec_editinfo|{spec_idx}"))
    except Exception:
        pass
    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_main"))
    bot.send_message(chat_id, out, reply_markup=kb)

def set_admin_notification_count(admin_id, count, data=None):
    if data is None:
        data = load_data()
    admin_notifications = data.setdefault("admin_notifications", {})
    key = str(admin_id)
    entry = admin_notifications.get(key)
    text = f"У вас {count} новых сообщений." if count > 0 else "Нет новых сообщений."
    kb = _build_admin_notify_kb()
    try:
        previous_count = 0
        if entry:
            previous_count = entry.get("count", 0)
            # Удаляем старое уведомление, если оно есть
            try:
                if entry.get("msg_id"):
                    bot.delete_message(admin_id, entry["msg_id"])
            except Exception:
                pass
        if count > 0:
            # корректное склонение для русского
            def russian_count_text(n):
                n = int(n)
                if 11 <= (n % 100) <= 14:
                    form = "новых сообщений"
                else:
                    last = n % 10
                    if last == 1:
                        form = "новое сообщение"
                    elif 2 <= last <= 4:
                        form = "новых сообщения"
                    else:
                        form = "новых сообщений"
                return f"У вас {n} {form}."

            try:
                msg = bot.send_message(admin_id, russian_count_text(count), reply_markup=kb)
                admin_notifications[key] = {"msg_id": msg.message_id, "count": count}
            except Exception:
                print("failed to send admin notify new", admin_id, traceback.format_exc())
        else:
            admin_notifications.pop(key, None)
    except Exception:
        print("set_admin_notification_count failed for", admin_id, traceback.format_exc())
    save_data(data)

def increment_admin_notifications_for_all(data=None):
    if data is None:
        data = load_data()
    msgs_count = len(data.get("messages", []))
    for adm in ADMINS:
        set_admin_notification_count(adm, msgs_count, data)

def main_keyboard(user_id=None):
    data = load_data()
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(types.KeyboardButton("📅 Свободные слоты на неделю"))
    kb.add(types.KeyboardButton("🔔 Напоминания"))
    for idx, spec in enumerate(data["specialists"]):
        kb.add(types.KeyboardButton(f"👩‍⚕️ {spec}"))
    kb.add(types.KeyboardButton("📋 Мои записи"))
    kb.add(types.KeyboardButton("📨 Мои сообщения"))
    if user_id in ADMINS:
        kb.add(types.KeyboardButton("⚙️ Админ панель"))
    kb.row(types.KeyboardButton("🔘 Начать"), types.KeyboardButton("❓ Запрос"), types.KeyboardButton("💡 Предложение"))
    return kb

def admin_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    # Row 1
    kb.row(types.KeyboardButton("📋 Все записи"), types.KeyboardButton("📨 Запросы пользователей"))
    # Row 2
    kb.row(types.KeyboardButton("💡 Предложения пользователей"), types.KeyboardButton("📥 Выгрузить пользователей"))
    # Row 3
    kb.row(types.KeyboardButton("📊 Статистика"), types.KeyboardButton("⏰ Управление временем"))
    # Row 4
    kb.row(types.KeyboardButton("👥 Управление специалистов"), types.KeyboardButton("🔙 На главную"))
    return kb

def specialists_manage_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("Добавить специалиста", "Удалить специалиста", "Переименовать специалиста")
    kb.add("Добавить характеристики")
    kb.add("🔙 На главную")
    return kb

pending_action = {}
# Anti-flood был удалён: функция check_rate_limit всегда разрешает действия.
def check_rate_limit(uid:int):
    return True, 0

@bot.message_handler(commands=["start"])
def cmd_start(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    pending_action.pop(chat_id, None)
    bot.send_message(chat_id, WELCOME_TEXT, reply_markup=main_keyboard(user_id))

@bot.message_handler(func=lambda m: True)
def all_text_handler(message):
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        # anti-flood: проверяем лимит для пользователя
        allowed, retry = check_rate_limit(user_id)
        if not allowed:
            bot.send_message(chat_id, f"⏳ Слишком часто — повторите через {retry} сек.")
            return
        text = (message.text or "").strip()
        # Поддержка отмены операций
        if text == "Отмена":
            pending_action.pop(chat_id, None)
            bot.send_message(chat_id, "Действие отменено.", reply_markup=main_keyboard(user_id))
            return
        data = load_data()

        if chat_id in pending_action:
            info = pending_action[chat_id]
            action = info.get("action")

            if action == "spec_add" and user_id in ADMINS:
                name = text
                if name and name not in data["specialists"]:
                    data["specialists"].append(name)
                    save_data(data)
                    # show updated specialists inline panel
                    bot.send_message(chat_id, f"✅ Добавлен специалист: {name}")
                    show_specialists_admin(chat_id)
                else:
                    bot.send_message(chat_id, "Имя пустое или уже существует.")
                pending_action.pop(chat_id, None)
                return

            if action == "spec_remove" and user_id in ADMINS:
                name = text
                if name in data["specialists"]:
                    data["specialists"].remove(name)
                    data.get("schedule", {}).pop(name, None)
                    recs_all = data.get("records", {})
                    for uid in list(recs_all.keys()):
                        recs = recs_all[uid]
                        new_recs = [r for r in recs if r.get("spec") != name]
                        if new_recs:
                            data["records"][uid] = new_recs
                        else:
                            data["records"].pop(uid, None)
                    save_data(data)
                    bot.send_message(chat_id, f"❌ Специалист '{name}' удалён, связанные записи и расписание удалены.")
                    show_specialists_admin(chat_id)
                else:
                    bot.send_message(chat_id, "Специалист с таким именем не найден.", reply_markup=specialists_manage_keyboard())
                pending_action.pop(chat_id, None)
                return

            if action == "spec_rename" and user_id in ADMINS:
                old_name = info.get("old_name")
                new_name = text
                if old_name and old_name in data["specialists"] and new_name:
                    idx = data["specialists"].index(old_name)
                    data["specialists"][idx] = new_name
                    if old_name in data.get("schedule", {}):
                        data["schedule"][new_name] = data["schedule"].pop(old_name)
                    for uid, recs in data.get("records", {}).items():
                        for r in recs:
                            if r.get("spec") == old_name:
                                r["spec"] = new_name
                    save_data(data)
                    bot.send_message(chat_id, f"✏️ '{old_name}' переименован в '{new_name}'")
                    show_specialists_admin(chat_id)
                else:
                    bot.send_message(chat_id, "Ошибка: старое имя не найдено или новое имя пустое.", reply_markup=specialists_manage_keyboard())
                pending_action.pop(chat_id, None)
                return

            if action in ("user_request", "user_suggest"):
                tag = "запрос" if action == "user_request" else "предложение"
                mid = data.get("next_message_id", 1)
                username = get_username(message.from_user)
                msg_obj = {"id": mid, "from_id": user_id, "from_username": username, "tag": tag, "text": text, "ts": datetime.now().isoformat()}
                data.setdefault("messages", []).append(msg_obj)
                data["next_message_id"] = mid + 1
                save_data(data)
                try:
                    write_message_to_sheet(msg_obj)
                except Exception:
                    print("Failed to write user_request message to Google Sheet", traceback.format_exc())
                # Подтверждение пользователю, что сообщение доставлено администраторам
                bot.send_message(chat_id, "✉️ Ваше сообщение отправлено администраторам.\n\nАдмин постарается ответить как можно скорее.", reply_markup=main_keyboard(user_id))
                for adm in ADMINS:
                    try:
                        data = load_data()
                        set_admin_notification_count(adm, len(data.get("messages", [])), data)
                    except Exception:
                        print("notify admin failed", adm, traceback.format_exc())
                pending_action.pop(chat_id, None)
                return

            if action == "writing_message":
                spec = info.get("spec")
                username = get_username(message.from_user)
                mid = data.get("next_message_id", 1)
                msg_obj = {
                    "id": mid,
                    "from_id": user_id,
                    "from_username": username,
                    "tag": "сообщение_специалисту",
                    "spec": spec,
                    "text": text,
                    "ts": datetime.now().isoformat()
                }
                data.setdefault("messages", []).append(msg_obj)
                data["next_message_id"] = mid + 1
                save_data(data)
                try:
                    write_message_to_sheet(msg_obj)
                except Exception:
                    print("Failed to write message_to_spec to Google Sheet", traceback.format_exc())
                bot.send_message(chat_id, f"✉️ Ваше сообщение специалисту '{spec}' отправлено администраторам. Админ постарается ответить как можно скорее.", reply_markup=main_keyboard(user_id))
                for adm in ADMINS:
                    try:
                        data = load_data()
                        set_admin_notification_count(adm, len(data.get("messages", [])), data)
                    except Exception:
                        print("send to admin failed", adm, traceback.format_exc())
                pending_action.pop(chat_id, None)
                return

            if action == "user_edit_message":
                edit_mid = info.get("edit_mid")
                msgs = data.get("messages", [])
                updated = False
                for m in msgs:
                    try:
                        if int(m.get("id", -1)) == int(edit_mid) and int(m.get("from_id", -1)) == int(user_id):
                            m["text"] = text
                            m["edited_ts"] = datetime.now().isoformat()
                            updated = True
                            break
                    except Exception:
                        continue
                if updated:
                    save_data(data)
                    bot.send_message(chat_id, "✅ Ваше сообщение обновлено.", reply_markup=main_keyboard(user_id))
                    try:
                        increment_admin_notifications_for_all(data)
                    except Exception:
                        pass
                else:
                    bot.send_message(chat_id, "Не удалось найти или редактировать сообщение.", reply_markup=main_keyboard(user_id))
                pending_action.pop(chat_id, None)
                return

            if action == "admin_reply" and user_id in ADMINS:
                target_uid = info.get("target_user_id")
                reply_mid = info.get("reply_mid")
                reply_text = text
                if not target_uid:
                    bot.send_message(chat_id, "Не удалось определить пользователя для ответа.", reply_markup=main_keyboard(user_id))
                    pending_action.pop(chat_id, None)
                    return
                try:
                    # Найдём оригинальный текст по ID сообщения (если есть), чтобы добавить его ниже
                    original_text = None
                    for mm in data.get("messages", []):
                        try:
                            if int(mm.get("id", -1)) == int(reply_mid):
                                original_text = mm.get("text", "")
                                break
                        except Exception:
                            continue
                    # Отправляем пользователю только ответ администратора и оригинальное сообщение (без упоминания имени)
                    msg_to_user = f"✉️ Ответ администратора:\n\n{reply_text}"
                    if original_text:
                        msg_to_user += f"\n\nИсходное сообщение:\n{original_text}"
                    bot.send_message(int(target_uid), msg_to_user)
                except Exception:
                    bot.send_message(chat_id, "Не удалось отправить сообщение пользователю.", reply_markup=main_keyboard(user_id))
                    pending_action.pop(chat_id, None)
                    return
                msgs = data.get("messages", [])
                for m in msgs:
                    if int(m.get("id", -1)) == int(reply_mid):
                        m["answered"] = True
                        m["answered_by"] = user_id
                        m["answered_ts"] = datetime.now().isoformat()
                        break
                save_data(data)
                bot.send_message(chat_id, "✅ Ответ отправлен.")
                # обновим панель сообщений для администратора
                try:
                    show_messages_admin(chat_id)
                except Exception:
                    pass
                pending_action.pop(chat_id, None)
                return

            if action == "admin_edit_spec_field" and user_id in ADMINS:
                spec = info.get("spec")
                field = info.get("field")
                spec_idx = info.get("spec_idx")
                val = text.strip()
                if not spec or not field:
                    bot.send_message(chat_id, "Ошибка: не указаны спец или поле.", reply_markup=admin_keyboard())
                    pending_action.pop(chat_id, None)
                    return
                data = load_data()
                s_info_all = data.setdefault("specialists_info", {})
                s_info = s_info_all.setdefault(spec, {})
                try:
                    if field == 'services':
                        # multiline services
                        if not val:
                            s_info.pop('services', None)
                        else:
                            services = [ln.strip() for ln in val.splitlines() if ln.strip()]
                            s_info['services'] = services
                    elif field == 'contacts':
                        if not val:
                            s_info.pop('contacts', None)
                        else:
                            contacts = {}
                            parts = [p.strip() for p in val.replace('\n',';').split(';') if p.strip()]
                            for p in parts:
                                if ':' in p:
                                    k,v = p.split(':',1)
                                elif '=' in p:
                                    k,v = p.split('=',1)
                                else:
                                    continue
                                contacts[k.strip()] = v.strip()
                            if contacts:
                                s_info['contacts'] = contacts
                    else:
                        if not val:
                            s_info.pop(field, None)
                        else:
                            s_info[field] = val
                    # save or clean empty dict
                    if not s_info:
                        s_info_all.pop(spec, None)
                    else:
                        s_info_all[spec] = s_info
                    save_data(data)
                    bot.send_message(chat_id, f"✅ Поле '{field}' обновлено для {spec}.", reply_markup=admin_keyboard())
                    try:
                        add_action_log(user_id, f"Admin updated spec {spec} field {field}")
                    except Exception:
                        pass
                except Exception:
                    print('admin_edit_spec_field failed', traceback.format_exc())
                    bot.send_message(chat_id, "Ошибка при сохранении характеристики.", reply_markup=admin_keyboard())
                pending_action.pop(chat_id, None)
                return

            if action == "user_cancel_record":
                target = text
                uid_str = str(user_id)
                recs = data.get("records", {}).get(uid_str, [])
                removed = False
                for r in list(recs):
                    display = f"{r['spec']} {r['date']} {r['time']}"
                    if display == target:
                        slots = data.setdefault("schedule", {}).setdefault(r['spec'], {}).setdefault(r['date'], [])
                        if r['time'] not in slots:
                            slots.append(r['time'])
                        recs.remove(r)
                        removed = True
                        # Явное сообщение о том, что запись отменена
                        bot.send_message(chat_id, f"❌ Ваша запись отменена:\n👩‍⚕️ {r['spec']}\n📅 {r['date']}\n⏰ {r['time']}", reply_markup=main_keyboard(user_id))
                if removed:
                    data["records"][uid_str] = recs
                    save_data(data)
                    try:
                        add_action_log(user_id, f"Пользователь отменил запись: {target}")
                    except Exception:
                        pass
                else:
                    bot.send_message(chat_id, "Не найдена запись с таким описанием.", reply_markup=main_keyboard(user_id))
                pending_action.pop(chat_id, None)
                return

        if text in ["Запрос", "❓ Запрос"]:
            pending_action[chat_id] = {"action": "user_request"}
            bot.send_message(chat_id, "✍️ Напишите ваш запрос. После отправки он поступит администратору с тегом #запрос:", reply_markup=cancel_reply_kb())
            return
        if text in ["Предложение", "💡 Предложение"]:
            pending_action[chat_id] = {"action": "user_suggest"}
            bot.send_message(chat_id, "💡 Напишите ваше предложение. После отправки оно поступит администратору с тегом #предложение:", reply_markup=cancel_reply_kb())
            return
        if text in ["🔘 Начать", "/start"]:
            bot.send_message(chat_id, WELCOME_TEXT, reply_markup=main_keyboard(user_id))
            pending_action.pop(chat_id, None)
            return

        if text == "📋 Мои записи":
            uid_str = str(user_id)
            recs = data.get("records", {}).get(uid_str, [])
            if not recs:
                bot.send_message(chat_id, "У вас нет записей.", reply_markup=main_keyboard(user_id))
                return
            out = "🗓 Ваши записи:\n\n"
            kb = types.InlineKeyboardMarkup()
            for r in recs:
                out += f"👩‍⚕️ {r['spec']} — {r['date']} {r['time']}\n"
                try:
                    spec_idx = data["specialists"].index(r['spec'])
                except ValueError:
                    spec_idx = 0
                cb = f"cancel_my|{spec_idx}|{r['date']}|{r['time']}"
                edit_cb = f"reschedule|{spec_idx}|{r['date']}|{r['time']}"
                # короткая подпись кнопки, чтобы не обрезалась
                kb.add(types.InlineKeyboardButton("✏️ Изменить время", callback_data=edit_cb),
                       types.InlineKeyboardButton("❌ Отменить", callback_data=cb))
            bot.send_message(chat_id, out, reply_markup=kb)
            return

        if text == "📅 Свободные слоты на неделю":
            show_weekly_free_slots(chat_id)
            return

        if text == "🔔 Напоминания":
            show_reminder_settings(chat_id, user_id)
            return

        if text == "📨 Мои сообщения":
            send_my_messages(chat_id, user_id)
            return

        if user_id in ADMINS:
            if text == "⚙️ Админ панель":
                bot.send_message(chat_id, "⚙️ Админ-панель:", reply_markup=admin_keyboard())
                return
            if text == "📋 Все записи" or text == "Все записи":
                show_all_records_admin(chat_id)
                return
            if text == "📨 Запросы пользователей" or text == "Запросы пользователей":
                show_messages_admin(chat_id, filter_tag="запрос")
                return
            if text == "💡 Предложения пользователей" or text == "Предложения пользователей":
                show_messages_admin(chat_id, filter_tag="предложение")
                return
            if text == "📊 Статистика":
                show_stats_admin(chat_id)
                return
            # журнал действий удалён
            # кнопка отмены записи убрана из админки — удаление выполняется через 'Все записи'
            if text == "🔙 На главную":
                bot.send_message(chat_id, "Главное меню:", reply_markup=main_keyboard(user_id))
                return
            
            if text == "⏰ Управление временем":
                data = load_data()
                kb = types.InlineKeyboardMarkup()
                for idx, spec in enumerate(data["specialists"]):
                    kb.add(types.InlineKeyboardButton(spec, callback_data=f"time_manage|{idx}"))
                kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_menu_back"))
                bot.send_message(chat_id, "Выберите специалиста для управления временем:", reply_markup=kb)
                return
            if text == "👥 Управление специалистов":
                show_specialists_admin(chat_id)
                return
            if text == "Добавить характеристики":
                data = load_data()
                kb = types.InlineKeyboardMarkup()
                for idx, spec in enumerate(data.get("specialists", [])):
                    kb.add(types.InlineKeyboardButton(spec, callback_data=f"spec_editinfo|{idx}"))
                kb.add(types.InlineKeyboardButton("🔙 На главную", callback_data="back_to_main"))
                bot.send_message(chat_id, "Выберите специалиста для добавления характеристик (поля опциональны):", reply_markup=kb)
                return
            if text == "📥 Выгрузить пользователей":
                # вызовем существующую команду экспорта
                try:
                    cmd_get_users(message)
                except Exception:
                    err = traceback.format_exc()
                    print('export button error:', err)
                    try:
                        bot.send_message(chat_id, f"Ошибка при запуске экспорта пользователей:\n{err}")
                    except Exception:
                        pass
                return
            if text == "Добавить специалиста":
                pending_action[chat_id] = {"action": "spec_add"}
                bot.send_message(chat_id, "Введите имя нового специалиста:", reply_markup=cancel_reply_kb())
                return
            if text == "Удалить специалиста":
                pending_action[chat_id] = {"action": "spec_remove"}
                bot.send_message(chat_id, "Введите точное имя специалиста для удаления:", reply_markup=cancel_reply_kb())
                return
            if text == "Переименовать специалиста":
                kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
                for spec in data["specialists"]:
                    kb.add(types.KeyboardButton(spec))
                kb.add("🔙 На главную")
                pending_action[chat_id] = {"action": "spec_rename", "old_name": None}
                bot.send_message(chat_id, "Выберите специалиста для переименования:", reply_markup=kb)
                return
            if pending_action.get(chat_id, {}).get("action") == "spec_rename":
                spec_name = _extract_spec_name(text)
                if spec_name in data["specialists"]:
                    pending_action[chat_id]["old_name"] = spec_name
                    bot.send_message(chat_id, f"Введите новое имя для {spec_name}:", reply_markup=cancel_reply_kb())
                    return

        spec_text = _extract_spec_name(text)
        if spec_text in data["specialists"]:
            idx = data["specialists"].index(spec_text)
            # Show improved specialist card
            show_specialist_card(chat_id, idx)
            return

        bot.send_message(chat_id, "Выберите действие из меню:", reply_markup=main_keyboard(user_id))

    except Exception:
        print("Error in all_text_handler:", traceback.format_exc())


@bot.callback_query_handler(func=lambda cb: True)
def inline_callbacks(cb):
    try:
        data = load_data()
        chat_id = cb.message.chat.id
        user_id = cb.from_user.id
        # anti-flood for callback queries too
        allowed, retry = check_rate_limit(user_id)
        if not allowed:
            try:
                bot.answer_callback_query(cb.id, f"⏳ Слишком часто — повторите через {retry} сек.")
            except Exception:
                pass
            return
        payload = cb.data

        if payload == "back_to_main":
            safe_edit_message(chat_id, cb.message.message_id, "Главное меню:", reply_markup=None)
            bot.send_message(chat_id, "Выберите действие:", reply_markup=main_keyboard(user_id))
            return

        if payload.startswith("rem_toggle|"):
            try:
                _, key = payload.split("|",1)
                data = load_data()
                users = data.setdefault("users_settings", {})
                u = users.setdefault(str(user_id), {"24h": True, "1h": True, "10m": True})
                u[key] = not bool(u.get(key, True))
                save_data(data)
                # refresh settings message
                try:
                    show_reminder_settings(chat_id, user_id, edit_message=True, message_id=cb.message.message_id)
                except Exception:
                    pass
            except Exception:
                print('rem_toggle failed', traceback.format_exc())
            return

        if payload.startswith("msg_to_spec|"):
            _, spec_idx = payload.split("|", 1)
            spec = data["specialists"][int(spec_idx)]
            pending_action[chat_id] = {"action": "writing_message", "spec": spec}
            bot.send_message(chat_id, f"Напишите сообщение специалисту {spec}:", reply_markup=cancel_reply_kb())
            return

        if payload.startswith("choose|"):
            _, spec_idx = payload.split("|", 1)
            spec = data["specialists"][int(spec_idx)]
            today = datetime.today()
            kb = types.InlineKeyboardMarkup(row_width=2)
            for i in range(7):
                d = today + timedelta(days=i)
                weekday = RU_WEEKDAYS[d.weekday()]
                label = f"{weekday}\n{d.day:02d}.{d.month:02d}"
                kb.add(types.InlineKeyboardButton(label, callback_data=f"date|{spec_idx}|{d.date().isoformat()}"))
            kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_main"))
            safe_edit_message(chat_id, cb.message.message_id, f"Вы выбрали: {spec}\nВыберите дату:", reply_markup=kb)
            return

        if payload.startswith("date|"):
            _, spec_idx, date_iso = payload.split("|", 2)
            spec = data["specialists"][int(spec_idx)]
            slots = data.get("schedule", {}).get(spec, {}).get(date_iso, [])
            if not slots:
                safe_edit_message(chat_id, cb.message.message_id, "❌ Нет доступного времени для этой даты.")
                return
            kb = types.InlineKeyboardMarkup(row_width=3)
            for t in slots:
                kb.add(types.InlineKeyboardButton(t, callback_data=f"book|{spec_idx}|{date_iso}|{t}"))
            kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data=f"choose|{spec_idx}"))
            safe_edit_message(chat_id, cb.message.message_id, f"Выберите время для {spec} {date_iso}:", reply_markup=kb)
            return

        if payload.startswith("book|"):
            _, spec_idx, date_iso, t = payload.split("|", 3)
            spec = data["specialists"][int(spec_idx)]
            # Проверяем доступность слота
            if t not in data["schedule"][spec].get(date_iso, []):
                bot.answer_callback_query(cb.id, "Извините — этот слот уже занят или недоступен.")
                return
            # Сохраняем временно выбранные данные в pending_action
            pending_action[chat_id] = {
                "action": "confirm_booking",
                "spec": spec,
                "date": date_iso,
                "time": t,
                "spec_idx": spec_idx
            }
            # Формируем подтверждение
            dt = datetime.strptime(date_iso, "%Y-%m-%d")
            date_str = dt.strftime("%d.%m")
            kb = types.InlineKeyboardMarkup(row_width=2)
            kb.add(
                types.InlineKeyboardButton("✅ Да", callback_data=f"confirm_book|yes"),
                types.InlineKeyboardButton("❌ Нет", callback_data=f"confirm_book|no")
            )
            safe_edit_message(
                chat_id, cb.message.message_id,
                f"Вы подтверждаете запись?\n👩‍⚕ {spec}\n📅 {date_str}\n⏰ {t}",
                reply_markup=kb
            )
            return

        if payload.startswith("confirm_book|"):
            _, answer = payload.split("|", 1)
            info = pending_action.get(chat_id)
            if not info or info.get("action") != "confirm_booking":
                bot.answer_callback_query(cb.id, "Нет данных для подтверждения.")
                return
            if answer == "no":
                pending_action.pop(chat_id, None)
                bot.send_message(chat_id, "Запись отменена.", reply_markup=main_keyboard(user_id))
                return
            # Подтверждение — создаём запись
            spec = info["spec"]
            date_iso = info["date"]
            t = info["time"]
            spec_idx = info["spec_idx"]
            data = load_data()
            if t not in data["schedule"][spec].get(date_iso, []):
                bot.send_message(chat_id, "Извините — этот слот уже занят или недоступен.", reply_markup=main_keyboard(user_id))
                pending_action.pop(chat_id, None)
                return
            uid_str = str(user_id)
            username = get_username(cb.from_user)
            rec = {"user_id": int(uid_str), "username": username, "spec": spec, "date": date_iso, "time": t, "ts": datetime.now().isoformat(), "notified": False, "reminders_sent": {"24h": False, "1h": False, "10m": False}}
            data.setdefault("records", {}).setdefault(uid_str, []).append(rec)
            data["schedule"][spec][date_iso].remove(t)
            # Добавляем запись о регистрации в messages
            msg_obj = {
                "id": data.get("next_message_id", 1),
                "from_id": user_id,
                "from_username": username,
                "tag": "регистрация",
                "spec": spec,
                "text": f"📅 {date_iso} ⏰ {t}",
                "ts": datetime.now().isoformat()
            }
            data.setdefault("messages", []).append(msg_obj)
            data["next_message_id"] = msg_obj["id"] + 1
            save_data(data)
            try:
                write_record_to_sheet(rec)
            except Exception:
                print("Failed to write record to Google Sheet", traceback.format_exc())
            try:
                write_message_to_sheet(msg_obj)
            except Exception:
                print("Failed to write registration message to Google Sheet", traceback.format_exc())
            safe_edit_message(chat_id, cb.message.message_id, f"✅ Вы записаны к {spec}\n📅 {date_iso}\n⏰ {t}")
            for adm in ADMINS:
                try:
                    data = load_data()
                    set_admin_notification_count(adm, len(data.get("messages", [])), data)
                except Exception:
                    print("notify admin failed", adm, traceback.format_exc())
            # log action
            try:
                add_action_log(user_id, f"Пользователь записался: {spec} {date_iso} {t}")
            except Exception:
                pass
            pending_action.pop(chat_id, None)
            return

        if payload.startswith("cancel_record|"):
            uid_str = str(user_id)
            recs = data.get("records", {}).get(uid_str, [])
            if not recs:
                bot.answer_callback_query(cb.id, "У вас нет записей.")
                return
            out = "Выберите запись для отмены:\n\n"
            kb = types.InlineKeyboardMarkup()
            for r in recs:
                try:
                    spec_idx = data["specialists"].index(r['spec'])
                except ValueError:
                    spec_idx = 0
                cbdata = f"cancel_my|{spec_idx}|{r['date']}|{r['time']}"
                kb.add(types.InlineKeyboardButton(f"{r['spec']} {r['date']} {r['time']}", callback_data=cbdata))
            safe_edit_message(chat_id, cb.message.message_id, out, reply_markup=kb)
            return

        if payload.startswith("cancel_my|"):
            _, spec_idx, date_iso, t = payload.split("|", 3)
            spec = data["specialists"][int(spec_idx)]
            uid_str = str(user_id)
            recs = data.get("records", {}).get(uid_str, [])
            removed = False
            for r in list(recs):
                if r.get("spec") == spec and r.get("date") == date_iso and r.get("time") == t:
                    recs.remove(r)
                    slots = data.setdefault("schedule", {}).setdefault(spec, {}).setdefault(date_iso, [])
                    if t not in slots:
                        slots.append(t)
                    removed = True
            if removed:
                if recs:
                    data["records"][uid_str] = recs
                else:
                    data["records"].pop(uid_str, None)
                save_data(data)
                safe_edit_message(chat_id, cb.message.message_id, f"✅ Запись отменена: {spec} {date_iso} {t}", reply_markup=None)
                bot.send_message(chat_id, "Запись успешно отменена.", reply_markup=main_keyboard(user_id))
                try:
                    add_action_log(user_id, f"Пользователь отменил запись: {spec} {date_iso} {t}")
                except Exception:
                    pass
            else:
                bot.answer_callback_query(cb.id, "Не удалось найти запись для отмены.")
            return

        if payload.startswith("reschedule|"):
            try:
                _, spec_idx, old_date, old_time = payload.split("|", 3)
                spec = data["specialists"][int(spec_idx)]
                today = datetime.today()
                kb = types.InlineKeyboardMarkup(row_width=2)
                for i in range(7):
                    d = today + timedelta(days=i)
                    weekday = RU_WEEKDAYS[d.weekday()]
                    label = f"{weekday}\n{d.day:02d}.{d.month:02d}"
                    cbdata = f"res_date|{spec_idx}|{old_date}|{old_time}|{d.date().isoformat()}"
                    kb.add(types.InlineKeyboardButton(label, callback_data=cbdata))
                kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="cancel_reschedule"))
                safe_edit_message(chat_id, cb.message.message_id, f"Изменение времени для {spec}. Выберите новую дату:", reply_markup=kb)
            except Exception:
                print('reschedule start failed', traceback.format_exc())
            return

        if payload.startswith("res_date|"):
            try:
                _, spec_idx, old_date, old_time, new_date = payload.split("|", 4)
                spec = data["specialists"][int(spec_idx)]
                slots = data.get("schedule", {}).get(spec, {}).get(new_date, [])
                if not slots:
                    safe_edit_message(chat_id, cb.message.message_id, "❌ Нет доступного времени для этой даты.")
                    return
                kb = types.InlineKeyboardMarkup(row_width=3)
                for t in slots:
                    kb.add(types.InlineKeyboardButton(t, callback_data=f"res_time|{spec_idx}|{old_date}|{old_time}|{new_date}|{t}"))
                kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data=f"reschedule|{spec_idx}|{old_date}|{old_time}"))
                safe_edit_message(chat_id, cb.message.message_id, f"Выберите новое время для {spec} {new_date}:", reply_markup=kb)
            except Exception:
                print('res_date handling failed', traceback.format_exc())
            return

        if payload.startswith("res_time|"):
            try:
                _, spec_idx, old_date, old_time, new_date, new_time = payload.split("|", 5)
                spec = data["specialists"][int(spec_idx)]
                kb = types.InlineKeyboardMarkup(row_width=2)
                kb.add(types.InlineKeyboardButton("✅ Подтвердить", callback_data=f"res_confirm|{spec_idx}|{old_date}|{old_time}|{new_date}|{new_time}"),
                       types.InlineKeyboardButton("❌ Отмена", callback_data=f"reschedule|{spec_idx}|{old_date}|{old_time}"))
                safe_edit_message(chat_id, cb.message.message_id, f"Подтвердите перенос:\n{spec}\nБыло: {old_date} {old_time}\nСтало: {new_date} {new_time}", reply_markup=kb)
            except Exception:
                print('res_time handling failed', traceback.format_exc())
            return

        if payload.startswith("res_confirm|"):
            try:
                _, spec_idx, old_date, old_time, new_date, new_time = payload.split("|", 5)
                spec = data["specialists"][int(spec_idx)]
                uid_str = str(user_id)
                recs = data.get("records", {}).get(uid_str, [])
                found = False
                for r in recs:
                    if r.get("spec") == spec and r.get("date") == old_date and r.get("time") == old_time:
                        # update schedule: return old slot
                        slots_old = data.setdefault("schedule", {}).setdefault(spec, {}).setdefault(old_date, [])
                        if old_time not in slots_old:
                            slots_old.append(old_time)
                            slots_old.sort()
                        # remove new slot
                        slots_new = data.setdefault("schedule", {}).setdefault(spec, {}).setdefault(new_date, [])
                        if new_time in slots_new:
                            slots_new.remove(new_time)
                        r["date"] = new_date
                        r["time"] = new_time
                        r["ts"] = datetime.now().isoformat()
                        # reset reminders_sent for that record
                        r["reminders_sent"] = {"24h": False, "1h": False, "10m": False}
                        found = True
                        break
                if found:
                    save_data(data)
                    safe_edit_message(chat_id, cb.message.message_id, f"✔ Ваша запись перенесена!\nБыло: {old_date} {old_time}\nСтало: {new_date} {new_time}")
                    bot.send_message(chat_id, f"✔ Ваша запись перенесена!\nБыло: {old_date} {old_time}\nСтало: {new_date} {new_time}", reply_markup=main_keyboard(user_id))
                    try:
                        add_action_log(user_id, f"Перенёс запись: {spec} {old_date} {old_time} -> {new_date} {new_time}")
                    except Exception:
                        pass
                else:
                    bot.answer_callback_query(cb.id, "Не удалось найти вашу запись для изменения.")
            except Exception:
                print('res_confirm failed', traceback.format_exc())
                bot.answer_callback_query(cb.id, "Ошибка при переносе записи.")
            return

        if payload.startswith("admin_msg_page|") and user_id in ADMINS:
            try:
                _, tag, pg = payload.split("|", 2)
                filter_tag = tag if tag else None
                page_num = int(pg or 0)
                show_messages_admin(chat_id, edit_message=True, message_id=cb.message.message_id, filter_tag=filter_tag, page=page_num)
            except Exception:
                print('admin_msg_page handling failed', traceback.format_exc())
            return

        if payload.startswith("user_msg_page|"):
            # навигация по сообщениям пользователя (страницы)
            try:
                _, pg = payload.split("|", 1)
                page_num = int(pg or 0)
                # заменяем текущее сообщение на страницу
                send_my_messages(chat_id, user_id, edit_message=True, message_id=cb.message.message_id, page=page_num)
            except Exception:
                print('user_msg_page handling failed', traceback.format_exc())
            return

        if payload.startswith("admin_rec_page|") and user_id in ADMINS:
            try:
                _, pg = payload.split("|", 1)
                page_num = int(pg or 0)
                show_all_records_admin(chat_id, edit_message=True, message_id=cb.message.message_id, page=page_num)
            except Exception:
                print('admin_rec_page handling failed', traceback.format_exc())
            return

        if payload.startswith("delete_record|") and user_id in ADMINS:
            try:
                _, uid, spec_idx, date_iso, t = payload.split("|", 4)
                spec = data["specialists"][int(spec_idx)]
                recs = data.get("records", {}).get(uid, [])
                removed = False
                user_name = None
                for r in list(recs):
                    if r.get("spec") == spec and r.get("date") == date_iso and r.get("time") == t:
                        user_name = r.get("username", "Пользователь")
                        recs.remove(r)
                        slots = data.setdefault("schedule", {}).setdefault(spec, {}).setdefault(date_iso, [])
                        if t not in slots:
                            slots.append(t)
                        removed = True
                if removed:
                    if recs:
                        data["records"][uid] = recs
                    else:
                        data["records"].pop(uid, None)
                    save_data(data)
                    bot.answer_callback_query(cb.id, "✅ Запись удалена.")
                    # Отправляем уведомление пользователю
                    try:
                        notification_text = (
                            f"❌ Ваша запись отменена\n\n"
                            f"Специалист: {spec}\n"
                            f"Дата: {date_iso}\n"
                            f"Время: {t}\n\n"
                            f"К сожалению, администратор отменил вашу запись. "
                            f"Пожалуйста, запишитесь на другое время."
                        )
                        bot.send_message(int(uid), notification_text, reply_markup=main_keyboard(int(uid)))
                    except Exception as e:
                        print("Failed to notify user about cancellation:", e)
                    # Обновляем панель записей
                    show_all_records_admin(chat_id, edit_message=True, message_id=cb.message.message_id)
                    try:
                        add_action_log(user_id, f"Админ удалил запись {user_name} : {spec} {date_iso} {t}")
                    except Exception:
                        pass
                else:
                    bot.answer_callback_query(cb.id, "Не удалось найти запись.")
            except Exception as e:
                print("delete_record error:", traceback.format_exc())
                bot.answer_callback_query(cb.id, "Ошибка при удалении записи.")
            return

        if payload.startswith("admin_reschedule|") and user_id in ADMINS:
            try:
                _, uid, spec_idx, old_date, old_time = payload.split("|", 4)
                spec = data["specialists"][int(spec_idx)]
                today = datetime.today()
                kb = types.InlineKeyboardMarkup(row_width=2)
                for i in range(7):
                    d = today + timedelta(days=i)
                    weekday = RU_WEEKDAYS[d.weekday()]
                    label = f"{weekday}\n{d.day:02d}.{d.month:02d}"
                    cbdata = f"admin_res_date|{uid}|{spec_idx}|{old_date}|{old_time}|{d.date().isoformat()}"
                    kb.add(types.InlineKeyboardButton(label, callback_data=cbdata))
                kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_show_records"))
                safe_edit_message(chat_id, cb.message.message_id, f"Перенос записи {spec} для user {uid}. Выберите новую дату:", reply_markup=kb)
            except Exception:
                print('admin_reschedule start failed', traceback.format_exc())
            return

        if payload.startswith("admin_res_date|") and user_id in ADMINS:
            try:
                _, uid, spec_idx, old_date, old_time, new_date = payload.split("|", 5)
                spec = data["specialists"][int(spec_idx)]
                slots = data.get("schedule", {}).get(spec, {}).get(new_date, [])
                if not slots:
                    safe_edit_message(chat_id, cb.message.message_id, "❌ Нет доступного времени для этой даты.")
                    return
                kb = types.InlineKeyboardMarkup(row_width=3)
                for t in slots:
                    kb.add(types.InlineKeyboardButton(t, callback_data=f"admin_res_time|{uid}|{spec_idx}|{old_date}|{old_time}|{new_date}|{t}"))
                kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data=f"admin_reschedule|{uid}|{spec_idx}|{old_date}|{old_time}"))
                safe_edit_message(chat_id, cb.message.message_id, f"Выберите новое время для {spec} {new_date}:", reply_markup=kb)
            except Exception:
                print('admin_res_date handling failed', traceback.format_exc())
            return

        if payload.startswith("admin_res_time|") and user_id in ADMINS:
            try:
                _, uid, spec_idx, old_date, old_time, new_date, new_time = payload.split("|", 6)
                spec = data["specialists"][int(spec_idx)]
                kb = types.InlineKeyboardMarkup(row_width=2)
                kb.add(types.InlineKeyboardButton("✅ Подтвердить", callback_data=f"admin_res_confirm|{uid}|{spec_idx}|{old_date}|{old_time}|{new_date}|{new_time}"),
                       types.InlineKeyboardButton("❌ Отмена", callback_data=f"admin_reschedule|{uid}|{spec_idx}|{old_date}|{old_time}"))
                safe_edit_message(chat_id, cb.message.message_id, f"Подтвердите перенос:\n{spec}\nБыло: {old_date} {old_time}\nСтало: {new_date} {new_time}", reply_markup=kb)
            except Exception:
                print('admin_res_time handling failed', traceback.format_exc())
            return

        if payload.startswith("admin_res_confirm|") and user_id in ADMINS:
            try:
                _, uid, spec_idx, old_date, old_time, new_date, new_time = payload.split("|", 6)
                spec = data["specialists"][int(spec_idx)]
                recs = data.get("records", {}).get(uid, [])
                found = False
                for r in recs:
                    if r.get("spec") == spec and r.get("date") == old_date and r.get("time") == old_time:
                        # update schedule: return old slot
                        slots_old = data.setdefault("schedule", {}).setdefault(spec, {}).setdefault(old_date, [])
                        if old_time not in slots_old:
                            slots_old.append(old_time)
                            slots_old.sort()
                        # remove new slot
                        slots_new = data.setdefault("schedule", {}).setdefault(spec, {}).setdefault(new_date, [])
                        if new_time in slots_new:
                            slots_new.remove(new_time)
                        r["date"] = new_date
                        r["time"] = new_time
                        r["ts"] = datetime.now().isoformat()
                        r["reminders_sent"] = {"24h": False, "1h": False, "10m": False}
                        found = True
                        break
                if found:
                    save_data(data)
                    # notify user
                    try:
                        bot.send_message(int(uid), f"✔ Ваша запись была перенесена администратором.\nСпециалист: {spec}\nБыло: {old_date} {old_time}\nСтало: {new_date} {new_time}", reply_markup=main_keyboard(int(uid)))
                    except Exception:
                        pass
                    bot.answer_callback_query(cb.id, "✅ Запись перенесена.")
                    show_all_records_admin(chat_id, edit_message=True, message_id=cb.message.message_id)
                    try:
                        add_action_log(user_id, f"Admin moved record {uid}: {spec} {old_date} {old_time} -> {new_date} {new_time}")
                    except Exception:
                        pass
                else:
                    bot.answer_callback_query(cb.id, "Не удалось найти запись для изменения.")
            except Exception:
                print('admin_res_confirm failed', traceback.format_exc())
                bot.answer_callback_query(cb.id, "Ошибка при переносе записи.")
            return

        if payload.startswith("time_manage|") and user_id in ADMINS:
            _, spec_idx = payload.split("|", 1)
            spec = data["specialists"][int(spec_idx)]
            today = datetime.today()
            kb = types.InlineKeyboardMarkup(row_width=2)
            for i in range(14):
                d = today + timedelta(days=i)
                kb.add(types.InlineKeyboardButton(f"{RU_WEEKDAYS[d.weekday()]} {d.day:02d}.{d.month:02d}", callback_data=f"time_date|{spec_idx}|{d.date().isoformat()}"))
            kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_menu_back"))
            safe_edit_message(chat_id, cb.message.message_id, f"Управление временем для: {spec}", reply_markup=kb)
            return

        if payload.startswith("time_date|") and user_id in ADMINS:
            _, spec_idx, date_iso = payload.split("|", 2)
            spec = data["specialists"][int(spec_idx)]
            slots = data.setdefault("schedule", {}).setdefault(spec, {}).setdefault(date_iso, [])
            kb = types.InlineKeyboardMarkup(row_width=4)
            for t in PRESET_TIMES:
                status = "✅" if t in slots else "❌"
                kb.add(types.InlineKeyboardButton(f"{t} {status}", callback_data=f"time_toggle|{spec_idx}|{date_iso}|{t}"))
            # debug: allow admin to dump schedule for this specialist
            kb.add(types.InlineKeyboardButton("📄 Показать расписание (debug)", callback_data=f"debug_sched|{spec_idx}|{date_iso}"))
            kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data=f"time_manage|{spec_idx}"))
            kb.add(types.InlineKeyboardButton("🔙 На админку", callback_data="admin_menu_back"))
            safe_edit_message(chat_id, cb.message.message_id, f"Редактирование времени для {spec}\nДата: {date_iso}", reply_markup=kb)
            return

        if payload.startswith("time_toggle|") and user_id in ADMINS:
            _, spec_idx, date_iso, t = payload.split("|", 3)
            spec = data["specialists"][int(spec_idx)]
            slots = data.setdefault("schedule", {}).setdefault(spec, {}).setdefault(date_iso, [])
            if t in slots:
                slots.remove(t)
                action_txt = 'removed'
            else:
                slots.append(t)
                action_txt = 'added'
            slots.sort()
            save_data(data)
            try:
                add_action_log(user_id, f"Admin time toggle {action_txt}: {spec} {date_iso} {t}")
            except Exception:
                pass
            kb = types.InlineKeyboardMarkup(row_width=4)
            for ts in PRESET_TIMES:
                status = "✅" if ts in slots else "❌"
                kb.add(types.InlineKeyboardButton(f"{ts} {status}", callback_data=f"time_toggle|{spec_idx}|{date_iso}|{ts}"))
            kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data=f"time_date|{spec_idx}|{date_iso}"))
            kb.add(types.InlineKeyboardButton("🔙 На админку", callback_data="admin_menu_back"))
            safe_edit_message(chat_id, cb.message.message_id, f"Редактирование времени для {spec}\nДата: {date_iso}", reply_markup=kb)
            return

        if payload == "admin_menu_back" and user_id in ADMINS:
            bot.send_message(chat_id, "⚙️ Админ-панель:", reply_markup=admin_keyboard())
            return

        if payload == "add_specs_info" and user_id in ADMINS:
            try:
                data = load_data()
                kb = types.InlineKeyboardMarkup()
                for idx, spec in enumerate(data.get("specialists", [])):
                    kb.add(types.InlineKeyboardButton(spec, callback_data=f"spec_editinfo|{idx}"))
                kb.add(types.InlineKeyboardButton("🔙 На админку", callback_data="admin_menu_back"))
                safe_edit_message(chat_id, cb.message.message_id, "Выберите специалиста для добавления/редактирования характеристик:", reply_markup=kb)
            except Exception:
                print('add_specs_info failed', traceback.format_exc())
            return

        if payload.startswith("debug_sched|") and user_id in ADMINS:
            try:
                _, spec_idx, date_iso = payload.split("|", 2)
                spec = data.get("specialists", [])[int(spec_idx)]
                sched = data.get("schedule", {}).get(spec, {})
                day_slots = sched.get(date_iso, [])
                bot.send_message(chat_id, f"Расписание для {spec} {date_iso}:\n{day_slots}")
            except Exception:
                print('debug_sched failed', traceback.format_exc())
                bot.answer_callback_query(cb.id, "Ошибка при показе расписания.")
            return

        if payload == "admin_show_records" and user_id in ADMINS:
            show_all_records_admin(chat_id, edit_message=True, message_id=cb.message.message_id)
            return

        if payload == "admin_view_messages" and user_id in ADMINS:
            show_messages_admin(chat_id, edit_message=False)
            return

        if payload == "del_notify" and user_id in ADMINS:
            try:
                data.get("admin_notifications", {}).pop(str(user_id), None)
                save_data(data)
                try:
                    bot.delete_message(chat_id, cb.message.message_id)
                except Exception:
                    safe_edit_message(chat_id, cb.message.message_id, "Уведомление удалено.")
            except Exception:
                print("del_notify failed", traceback.format_exc())
            return

        if payload.startswith("reply|") and user_id in ADMINS:
            try:
                _, mid_s = payload.split("|", 1)
                mid = int(mid_s)
                msgs = data.get("messages", [])
                target = next((m for m in msgs if int(m.get("id", -1)) == mid), None)
                if not target:
                    bot.answer_callback_query(cb.id, "Сообщение не найдено.")
                    return
                pending_action[chat_id] = {"action": "admin_reply", "reply_mid": mid, "target_user_id": target.get("from_id")}
                bot.send_message(chat_id, f"Напишите ответ пользователю {target.get('from_username')}:", reply_markup=cancel_reply_kb())
            except Exception:
                print("reply callback failed", traceback.format_exc())
            return

        if payload.startswith("admin_cancel_from_msg|") and user_id in ADMINS:
            try:
                _, mid_s = payload.split("|", 1)
                mid = int(mid_s)
                msgs = data.get("messages", [])
                target = next((m for m in msgs if int(m.get("id", -1)) == mid), None)
                if not target:
                    bot.answer_callback_query(cb.id, "Сообщение не найдено.")
                    return
                uid = str(target.get('from_id'))
                spec = target.get('spec')
                text = target.get('text','')
                # попытка извлечь дату/время
                date_iso = None
                time_str = None
                try:
                    if '📅' in text and '⏰' in text:
                        rest = text.split('📅', 1)[1].strip()
                        if '⏰' in rest:
                            date_part, time_part = rest.split('⏰', 1)
                            date_iso = date_part.strip()
                            time_str = time_part.strip()
                except Exception:
                    pass

                removed = False
                recs = data.get('records', {}).get(uid, [])
                for r in list(recs):
                    if spec and date_iso and time_str and r.get('spec') == spec and r.get('date') == date_iso and r.get('time') == time_str:
                        recs.remove(r)
                        slots = data.setdefault('schedule', {}).setdefault(spec, {}).setdefault(date_iso, [])
                        if time_str not in slots:
                            slots.append(time_str)
                        removed = True
                if removed:
                    if recs:
                        data['records'][uid] = recs
                    else:
                        data['records'].pop(uid, None)
                    # удаляем сообщение-уведомление регистрации
                    data['messages'] = [m for m in data.get('messages', []) if int(m.get('id', -1)) != mid]
                    save_data(data)
                    bot.answer_callback_query(cb.id, "✅ Запись пользователя отменена.")
                    try:
                        bot.send_message(int(uid), f"❌ Ваша запись была отменена администратором.\nСпециалист: {spec}\nДата: {date_iso}\nВремя: {time_str}", reply_markup=main_keyboard(int(uid)))
                    except Exception:
                        pass
                    try:
                        add_action_log(user_id, f"Admin cancelled record from msg {mid}: {spec} {date_iso} {time_str}")
                    except Exception:
                        pass
                    # обновляем панели админа
                    try:
                        show_messages_admin(chat_id, edit_message=True, message_id=cb.message.message_id)
                    except Exception:
                        pass
                    try:
                        show_all_records_admin(chat_id, edit_message=True, message_id=cb.message.message_id)
                    except Exception:
                        pass
                else:
                    bot.answer_callback_query(cb.id, "Не удалось найти запись для отмены.")
            except Exception:
                print('admin_cancel_from_msg failed', traceback.format_exc())
                bot.answer_callback_query(cb.id, "Ошибка при отмене записи.")
            return

        if payload.startswith("delmsg|") and user_id in ADMINS:
            _, target = payload.split("|", 1)
            msgs = data.get("messages", [])
            if target == "all":
                data["messages"] = []
                save_data(data)
                safe_edit_message(chat_id, cb.message.message_id, "✅ Все сообщения удалены.")
                try:
                    increment_admin_notifications_for_all(data)
                except Exception:
                    print("failed to update admin notifications after del all", traceback.format_exc())
                return
            else:
                # Удалить одно сообщение по ID
                try:
                    target_id = int(target)
                    msgs[:] = [m for m in msgs if int(m.get("id", -1)) != target_id]
                    save_data(data)
                    bot.answer_callback_query(cb.id, "✅ Сообщение удалено.")
                    # Обновляем панель сообщений
                    show_messages_admin(chat_id, edit_message=True, message_id=cb.message.message_id)
                except Exception:
                    print("delmsg single error:", traceback.format_exc())
                    bot.answer_callback_query(cb.id, "Ошибка при удалении сообщения.")
                return

        # User message edit/delete (для пользователей) — редактирование/удаление своих сообщений
        if payload.startswith("um_edit|"):
            try:
                _, mid_s = payload.split("|", 1)
                mid = int(mid_s)
                msgs = data.get("messages", [])
                target = next((m for m in msgs if int(m.get("id", -1)) == mid), None)
                if not target or int(target.get("from_id", -1)) != int(user_id):
                    bot.answer_callback_query(cb.id, "Сообщение не найдено или нет прав для редактирования.")
                    return
                pending_action[chat_id] = {"action": "user_edit_message", "edit_mid": mid}
                bot.send_message(chat_id, "✏️ Отправьте новый текст для вашего сообщения (или нажмите Отмена):", reply_markup=cancel_reply_kb())
            except Exception:
                print("um_edit callback failed", traceback.format_exc())
                bot.answer_callback_query(cb.id, "Ошибка при подготовке редактирования.")
            return

        if payload.startswith("um_del|"):
            try:
                _, mid_s = payload.split("|", 1)
                mid = int(mid_s)
                msgs = data.get("messages", [])
                orig_len = len(msgs)
                msgs[:] = [m for m in msgs if not (int(m.get("id", -1)) == mid and int(m.get("from_id", -1)) == int(user_id))]
                if len(msgs) == orig_len:
                    bot.answer_callback_query(cb.id, "Не найдено сообщение для удаления.")
                    return
                save_data(data)
                try:
                    bot.delete_message(chat_id, cb.message.message_id)
                except Exception:
                    pass
                bot.answer_callback_query(cb.id, "✅ Сообщение удалено.")
                try:
                    increment_admin_notifications_for_all(data)
                except Exception:
                    pass
            except Exception:
                print("um_del callback failed", traceback.format_exc())
                bot.answer_callback_query(cb.id, "Ошибка при удалении сообщения.")
            return

        if payload == "spec_add" and user_id in ADMINS:
            pending_action[chat_id] = {"action": "spec_add"}
            bot.send_message(chat_id, "Введите имя нового специалиста:", reply_markup=cancel_reply_kb())
            return

        if payload.startswith("spec_delete|") and user_id in ADMINS:
            # show confirmation before deleting
            try:
                _, idx_s = payload.split("|", 1)
                idx = int(idx_s)
                spec = data.get("specialists", [])[idx]
                kb = types.InlineKeyboardMarkup()
                kb.add(types.InlineKeyboardButton(f"Удалить {spec}", callback_data=f"confirm_delete|{idx}"))
                kb.add(types.InlineKeyboardButton("Отмена", callback_data="cancel_spec_action"))
                safe_edit_message(chat_id, cb.message.message_id, f"Вы уверены, что хотите удалить специалиста '{spec}'? Это удалит расписание и связанные записи.", reply_markup=kb)
            except Exception:
                safe_edit_message(chat_id, cb.message.message_id, "Ошибка при подготовке удаления специалиста.")
            return

        if payload.startswith("spec_rename|") and user_id in ADMINS:
            try:
                _, idx_s = payload.split("|", 1)
                idx = int(idx_s)
                spec = data.get("specialists", [])[idx]
                pending_action[chat_id] = {"action": "spec_rename", "old_name": spec}
                bot.send_message(chat_id, f"Введите новое имя для {spec}:", reply_markup=cancel_reply_kb())
            except Exception:
                bot.answer_callback_query(cb.id, "Ошибка при выборе специалиста для переименования.")
            return

        if payload.startswith("spec_editinfo|") and user_id in ADMINS:
            try:
                _, idx_s = payload.split("|", 1)
                idx = int(idx_s)
                spec = data.get("specialists", [])[idx]
                kb = types.InlineKeyboardMarkup()
                kb.add(types.InlineKeyboardButton("✏️ Профессия", callback_data=f"edit_infofield|{idx}|profession"),
                       types.InlineKeyboardButton("🧾 Опыт", callback_data=f"edit_infofield|{idx}|experience"))
                kb.add(types.InlineKeyboardButton("📅 График", callback_data=f"edit_infofield|{idx}|works"),
                       types.InlineKeyboardButton("🛠 Услуги", callback_data=f"edit_infofield|{idx}|services"))
                kb.add(types.InlineKeyboardButton("📞 Контакты", callback_data=f"edit_infofield|{idx}|contacts"),
                       types.InlineKeyboardButton("🖼 Аватар (URL или file_id)", callback_data=f"edit_infofield|{idx}|avatar"))
                kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data=f"show_spec_admin|{idx}"))
                safe_edit_message(chat_id, cb.message.message_id,
                                  f"Редактирование характеристик специалиста: {spec}\n\nПоля опциональны — если не заполнить, они не будут показаны пользователям.",
                                  reply_markup=kb)
            except Exception:
                print('spec_editinfo handling failed', traceback.format_exc())
            return

        if payload.startswith("show_spec_admin|") and user_id in ADMINS:
            try:
                _, idx_s = payload.split("|", 1)
                idx = int(idx_s)
                # show card for admin (will contain edit button too)
                show_specialist_card(chat_id, idx)
                try:
                    bot.delete_message(chat_id, cb.message.message_id)
                except Exception:
                    pass
            except Exception:
                print('show_spec_admin failed', traceback.format_exc())
            return

        if payload.startswith("edit_infofield|") and user_id in ADMINS:
            try:
                _, idx_s, field = payload.split("|", 2)
                idx = int(idx_s)
                spec = data.get("specialists", [])[idx]
                pending_action[chat_id] = {"action": "admin_edit_spec_field", "spec": spec, "field": field, "spec_idx": idx}
                hint = ""
                if field == 'services':
                    hint = "\nУкажите услуги каждую в новой строке. Оставьте пустым, чтобы удалить список."
                if field == 'contacts':
                    hint = "\nФормат: tg:@username;phone:+7999... или отправьте несколько строк 'tg:@..' 'phone:+..'. Оставьте пустым, чтобы удалить."
                if field == 'avatar':
                    hint = "\nОтправьте URL или file_id картинки (или оставьте пустым, чтобы убрать)."
                bot.send_message(chat_id, f"Отправьте новое значение для поля '{field}' для специалиста {spec}. {hint}\n\nОтправьте 'Отмена' чтобы отменить.", reply_markup=cancel_reply_kb())
            except Exception:
                print('edit_infofield failed', traceback.format_exc())
            return

        if payload.startswith("confirm_delete|") and user_id in ADMINS:
            try:
                _, idx_s = payload.split("|", 1)
                idx = int(idx_s)
                spec = data.get("specialists", [])[idx]
                # perform deletion
                data["specialists"].pop(idx)
                data.get("schedule", {}).pop(spec, None)
                recs_all = data.get("records", {})
                for uid in list(recs_all.keys()):
                    recs = recs_all[uid]
                    new_recs = [r for r in recs if r.get("spec") != spec]
                    if new_recs:
                        data["records"][uid] = new_recs
                    else:
                        data["records"].pop(uid, None)
                save_data(data)
                safe_edit_message(chat_id, cb.message.message_id, f"❌ Специалист '{spec}' удалён.")
                show_specialists_admin(chat_id, edit_message=True, message_id=cb.message.message_id)
            except Exception:
                safe_edit_message(chat_id, cb.message.message_id, "Ошибка при удалении специалиста.")
            return

        if payload == "cancel_spec_action" and user_id in ADMINS:
            # refresh specialists panel
            try:
                show_specialists_admin(chat_id, edit_message=True, message_id=cb.message.message_id)
            except Exception:
                safe_edit_message(chat_id, cb.message.message_id, "Отмена.")
            return

    except Exception:
        print("Error in inline_callbacks:", traceback.format_exc())

def send_my_records(chat_id, user_id):
    data = load_data()
    recs = data.get("records", {}).get(str(user_id), [])
    if not recs:
        bot.send_message(chat_id, "У вас нет записей.", reply_markup=main_keyboard(user_id))
        return
    out = "🗓 Ваши записи:\n\n"
    kb = types.InlineKeyboardMarkup()
    for r in recs:
        out += f"👩‍⚕️ {r['spec']} — {r['date']} {r['time']}\n"
        try:
            spec_idx = data["specialists"].index(r['spec'])
        except ValueError:
            spec_idx = 0
        kb.add(types.InlineKeyboardButton("✏️ Изменить время", callback_data=f"reschedule|{spec_idx}|{r['date']}|{r['time']}"),
               types.InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_my|{spec_idx}|{r['date']}|{r['time']}"))
    bot.send_message(chat_id, out, reply_markup=kb)


def send_my_messages(chat_id, user_id, edit_message=False, message_id=None, page=0):
    data = load_data()
    # Показываем все сообщения пользователя, исключая регистрации
    msgs = [m for m in data.get("messages", []) if int(m.get("from_id", -1)) == int(user_id) and m.get("tag") != "регистрация"]
    if not msgs:
        bot.send_message(chat_id, "У вас нет сообщений.", reply_markup=main_keyboard(user_id))
        return

    # Нормализуем и сохраняем при необходимости
    changed = False
    for m in msgs:
        if "id" not in m:
            m["id"] = data.get("next_message_id", 1)
            data["next_message_id"] = m["id"] + 1
            changed = True
        if "from_username" not in m:
            m["from_username"] = m.get("from_username") or m.get("username") or f"id{m.get('from_id','?')}"
            changed = True
    if changed:
        save_data(data)

    # Пагинация
    page_size = 5
    total = len(msgs)
    total_pages = (total + page_size - 1) // page_size
    if page < 0:
        page = 0
    if page >= total_pages:
        page = total_pages - 1
    start = page * page_size
    page_msgs = msgs[start:start + page_size]

    # Собираем текст и клавиатуру одной сообщения для страницы
    out_lines = []
    kb = types.InlineKeyboardMarkup()
    for m in page_msgs:
        mid = m.get("id")
        tag = m.get("tag", "")
        text = m.get("text", "")
        tag_with_hash = f"#{tag}" if tag else "Сообщение"
        out_lines.append(f"ID {mid} | {tag_with_hash}\n{text}")
        # Кнопки: редактировать (если не регистрация) и удалить
        if tag != "регистрация":
            kb.add(types.InlineKeyboardButton("✏️ Редактировать", callback_data=f"um_edit|{mid}"),
                   types.InlineKeyboardButton("🗑 Удалить", callback_data=f"um_del|{mid}"))
        else:
            kb.add(types.InlineKeyboardButton("🗑 Удалить", callback_data=f"um_del|{mid}"))

    # Навигация
    ctrl_kb = types.InlineKeyboardMarkup()
    prev_btn = None
    next_btn = None
    if page > 0:
        prev_btn = types.InlineKeyboardButton("◀️ Назад", callback_data=f"user_msg_page|{page-1}")
    if page < total_pages - 1:
        next_btn = types.InlineKeyboardButton("Вперёд ▶️", callback_data=f"user_msg_page|{page+1}")
    if prev_btn and next_btn:
        ctrl_kb.add(prev_btn, next_btn)
    elif prev_btn:
        ctrl_kb.add(prev_btn)
    elif next_btn:
        ctrl_kb.add(next_btn)
    ctrl_kb.add(types.InlineKeyboardButton("Удалить все сообщения", callback_data="um_del|all"))

    header = f"📨 Ваши сообщения — страница {page+1}/{total_pages} (всего: {total})\n\n"
    out = header + "\n\n".join(out_lines)

    # Если нужно редактировать существующее сообщение — заменяем его
    if edit_message and message_id:
        safe_edit_message(chat_id, message_id, out, reply_markup=ctrl_kb)
    else:
        # Отправляем одно сообщение с контролами
        try:
            bot.send_message(chat_id, out, reply_markup=ctrl_kb)
        except Exception:
            try:
                bot.send_message(chat_id, out)
            except Exception:
                pass

def show_all_records_admin(chat_id, edit_message=False, message_id=None, page=0):
    data = load_data()
    records_list = []
    for uid, recs in data.get("records", {}).items():
        for r in recs:
            records_list.append((uid, r))

    if not records_list:
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("🔙 На админку", callback_data="admin_menu_back"))
        text = "Записей нет."
        if edit_message and message_id:
            safe_edit_message(chat_id, message_id, text, reply_markup=kb)
        else:
            bot.send_message(chat_id, text, reply_markup=kb)
        return

    # Пагинация
    page_size = 5
    total = len(records_list)
    total_pages = (total + page_size - 1) // page_size
    if page < 0:
        page = 0
    if page >= total_pages:
        page = total_pages - 1
    start = page * page_size
    page_recs = records_list[start:start + page_size]

    out_lines = []
    kb = types.InlineKeyboardMarkup()
    for idx, (uid, r) in enumerate(page_recs):
        out_lines.append(f"👤 {r['username']} | {r['spec']} | {r['date']} {r['time']}")
        try:
            spec_idx = data["specialists"].index(r['spec'])
        except ValueError:
            spec_idx = 0
        cb_data = f"delete_record|{uid}|{spec_idx}|{r['date']}|{r['time']}"
        # Admin controls: allow reschedule or delete
        kb.add(types.InlineKeyboardButton("✏️ Перенести", callback_data=f"admin_reschedule|{uid}|{spec_idx}|{r['date']}|{r['time']}"),
               types.InlineKeyboardButton("🗑 Удалить", callback_data=cb_data))

    # Навигация
    ctrl_kb = types.InlineKeyboardMarkup()
    prev_btn = None
    next_btn = None
    if page > 0:
        prev_btn = types.InlineKeyboardButton("◀️ Назад", callback_data=f"admin_rec_page|{page-1}")
    if page < total_pages - 1:
        next_btn = types.InlineKeyboardButton("Вперёд ▶️", callback_data=f"admin_rec_page|{page+1}")
    if prev_btn and next_btn:
        ctrl_kb.add(prev_btn, next_btn)
    elif prev_btn:
        ctrl_kb.add(prev_btn)
    elif next_btn:
        ctrl_kb.add(next_btn)
    ctrl_kb.add(types.InlineKeyboardButton("🔙 На админку", callback_data="admin_menu_back"))

    header = f"📋 Все записи — страница {page+1}/{total_pages} (всего: {total})\n\n"
    out = header + "\n".join(out_lines)

    if edit_message and message_id:
        safe_edit_message(chat_id, message_id, out, reply_markup=ctrl_kb)
    else:
        bot.send_message(chat_id, out, reply_markup=ctrl_kb)

def show_messages_admin(chat_id, edit_message=False, message_id=None, filter_tag=None, page=0):
    try:
        data = load_data()
        msgs = data.get("messages", [])
        if filter_tag:
            msgs = [m for m in msgs if m.get("tag") == filter_tag]
        if not msgs:
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("🔙 На админку", callback_data="admin_menu_back"))
            if edit_message:
                safe_edit_message(chat_id, message_id, "Сообщений нет.", reply_markup=kb)
            else:
                bot.send_message(chat_id, "Сообщений нет.", reply_markup=kb)
            return

        changed = False
        for m in msgs:
            if "id" not in m:
                m["id"] = data.get("next_message_id", 1)
                data["next_message_id"] = m["id"] + 1
                changed = True
            if "from_username" not in m:
                m["from_username"] = m.get("from_username") or m.get("username") or f"id{m.get('from_id','?')}"
                changed = True
            if "tag" not in m:
                if "spec" in m:
                    m["tag"] = "сообщение_специалисту"
                    changed = True
                else:
                    m["tag"] = m.get("tag", "")
        if changed:
            save_data(data)

        # Пагинация
        page_size = 5
        total = len(msgs)
        total_pages = (total + page_size - 1) // page_size
        if page < 0:
            page = 0
        if page >= total_pages:
            page = total_pages - 1
        start = page * page_size
        page_msgs = msgs[start:start + page_size]

        # Отправка сообщений (по одному) для текущей страницы
        for m in page_msgs:
            mid = m.get("id")
            tag = m.get("tag", "")
            from_username = m.get("from_username", f"id{m.get('from_id','?')}")
            text = m.get("text", "")
            answered = bool(m.get("answered"))
            status = " ✅ Отвечено" if answered else ""
            tag_with_hash = f"#{tag}" if tag else "сообщение"
            spec_line = f"\n👩‍⚕️ {m.get('spec')}" if m.get('spec') else ""
            kb_item = types.InlineKeyboardMarkup()
            btn_del = types.InlineKeyboardButton("🗑 Удалить", callback_data=f"delmsg|{mid}")

            # Для регистраций заменяем кнопку 'Ответить' на 'Перенести' и 'Отменить'
            if tag == 'регистрация':
                # пытаемся извлечь дату и время из текста вида '📅 {date_iso} ⏰ {time}'
                date_iso = None
                time_str = None
                try:
                    if '📅' in text and '⏰' in text:
                        rest = text.split('📅', 1)[1].strip()
                        if '⏰' in rest:
                            date_part, time_part = rest.split('⏰', 1)
                            date_iso = date_part.strip()
                            time_str = time_part.strip()
                except Exception:
                    date_iso = None
                    time_str = None
                try:
                    spec_idx = data["specialists"].index(m.get('spec')) if m.get('spec') else 0
                except Exception:
                    spec_idx = 0
                # Перенести запись админом и Отменить (удалить запись пользователя)
                kb_item.add(
                    types.InlineKeyboardButton("✏️ Перенести", callback_data=f"admin_reschedule|{m.get('from_id')}|{spec_idx}|{date_iso}|{time_str}"),
                    types.InlineKeyboardButton("❌ Отменить", callback_data=f"admin_cancel_from_msg|{mid}")
                )
                kb_item.add(btn_del)
            else:
                if not answered:
                    btn_reply = types.InlineKeyboardButton("✉️ Ответить", callback_data=f"reply|{mid}")
                    kb_item.add(btn_reply, btn_del)
                else:
                    kb_item.add(btn_del)

            try:
                bot.send_message(chat_id, f"📨 ID {mid} | {tag_with_hash} | {from_username}{status}{spec_line}\n{text}", reply_markup=kb_item)
            except Exception:
                try:
                    bot.send_message(chat_id, f"📨 ID {mid} | {tag_with_hash} | {from_username}{status}\n{text}")
                except Exception:
                    pass

        # Контролы навигации
        ctrl_kb = types.InlineKeyboardMarkup()
        prev_btn = None
        next_btn = None
        if page > 0:
            prev_btn = types.InlineKeyboardButton("◀️ Назад", callback_data=f"admin_msg_page|{filter_tag or ''}|{page-1}")
        if page < total_pages - 1:
            next_btn = types.InlineKeyboardButton("Вперёд ▶️", callback_data=f"admin_msg_page|{filter_tag or ''}|{page+1}")
        if prev_btn and next_btn:
            ctrl_kb.add(prev_btn, next_btn)
        elif prev_btn:
            ctrl_kb.add(prev_btn)
        elif next_btn:
            ctrl_kb.add(next_btn)
        ctrl_kb.add(types.InlineKeyboardButton("Удалить все сообщения", callback_data="delmsg|all"))
        ctrl_kb.add(types.InlineKeyboardButton("🔙 На админку", callback_data="admin_menu_back"))

        header = f"📨 Сообщения ({'фильтр: '+filter_tag if filter_tag else 'все'}) — страница {page+1}/{total_pages} (всего: {total})"
        if edit_message and message_id:
            safe_edit_message(chat_id, message_id, header, reply_markup=ctrl_kb)
        else:
            bot.send_message(chat_id, header, reply_markup=ctrl_kb)

    except Exception:
        print("show_messages_admin error:", traceback.format_exc())
        try:
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("🔙 На админку", callback_data="admin_menu_back"))
            if edit_message:
                safe_edit_message(chat_id, message_id, "Ошибка при получении сообщений.", reply_markup=kb)
            else:
                bot.send_message(chat_id, "Ошибка при получении сообщений.", reply_markup=kb)
        except Exception:
            print("failed to notify admin about show_messages_admin error")

def show_edit_specialists(chat_id):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("Добавить специалиста", "Удалить специалиста", "Переименовать специалиста")
    kb.add("🔙 На главную")
    bot.send_message(chat_id, "Управление специалистами:", reply_markup=kb)


def show_specialists_admin(chat_id, edit_message=False, message_id=None):
    data = load_data()
    specs = data.get("specialists", [])
    if not specs:
        text = "Список специалистов пуст."
        if edit_message and message_id:
            safe_edit_message(chat_id, message_id, text)
        else:
            bot.send_message(chat_id, text)
        return

    text = "👥 Список специалистов:\n\n"
    kb = types.InlineKeyboardMarkup()
    for idx, s in enumerate(specs):
        kb.add(
            types.InlineKeyboardButton(f"🗑 {s}", callback_data=f"spec_delete|{idx}"),
            types.InlineKeyboardButton(f"✏️ {s}", callback_data=f"spec_rename|{idx}")
        )
    kb.add(types.InlineKeyboardButton("➕ Добавить специалиста", callback_data="spec_add"))
    kb.add(types.InlineKeyboardButton("➕ Добавить характеристики", callback_data="add_specs_info"))
    kb.add(types.InlineKeyboardButton("🔙 На главную", callback_data="back_to_main"), types.InlineKeyboardButton("🔙 На админку", callback_data="admin_menu_back"))
    if edit_message and message_id:
        safe_edit_message(chat_id, message_id, text, reply_markup=kb)
    else:
        bot.send_message(chat_id, text, reply_markup=kb)

def reminders_loop():
    while True:
        try:
            now = datetime.now()
            data = load_data()
            for uid_str, recs in data.get("records", {}).items():
                for r in recs:
                    try:
                        dt = datetime.fromisoformat(r["date"] + "T" + r["time"])
                    except Exception:
                        continue
                    seconds_left = (dt - now).total_seconds()
                    reminders = r.setdefault("reminders_sent", {"24h": False, "1h": False, "10m": False})
                    # 24 hours
                    data_users = data.get("users_settings", {})
                    user_settings = data_users.get(uid_str, {"24h": True, "1h": True, "10m": True})
                    if 23*3600 <= seconds_left <= 25*3600 and not reminders.get("24h") and user_settings.get("24h", True):
                        try:
                            bot.send_message(int(uid_str), f"🔔 Напоминание!\nДо вашей записи к {r['spec']} осталось ~24 часа.")
                        except Exception:
                            pass
                        reminders["24h"] = True
                    # 1 hour
                    if 3500 <= seconds_left <= 3700 and not reminders.get("1h") and user_settings.get("1h", True):
                        try:
                            bot.send_message(int(uid_str), f"⏰ Через 1 час у вас приём у {r['spec']}.")
                        except Exception:
                            pass
                        reminders["1h"] = True
                    # 10 minutes
                    if 9*60 <= seconds_left <= 11*60 and not reminders.get("10m") and user_settings.get("10m", True):
                        try:
                            bot.send_message(int(uid_str), f"⚠ Через 10 минут – ваш приём.")
                        except Exception:
                            pass
                        reminders["10m"] = True
            save_data(data)
        except Exception:
            print("reminders loop error:", traceback.format_exc())
        time.sleep(60)

threading.Thread(target=reminders_loop, daemon=True).start()
threading.Thread(target=auto_clean_loop, daemon=True).start()

# Web-панель полностью удалена — все соответствующие обработчики и серверы убраны.


@bot.message_handler(commands=['get_users'])
def cmd_get_users(message):
    """Выгрузить всех известных боту пользователей в CSV и JSON и отправить админу (в памяти)."""
    user_id = message.from_user.id
    chat_id = message.chat.id
    if user_id not in ADMINS:
        bot.send_message(chat_id, "❌ Доступ запрещён. Только администраторы могут использовать эту команду.")
        return

    data = load_data()
    uids = set()
    # из записей
    for k in data.get('records', {}).keys():
        try:
            uids.add(int(k))
        except Exception:
            pass
    # из сообщений
    for m in data.get('messages', []):
        try:
            uids.add(int(m.get('from_id')))
        except Exception:
            pass
    # из users_settings
    for k in data.get('users_settings', {}).keys():
        try:
            uids.add(int(k))
        except Exception:
            pass

    if not uids:
        bot.send_message(chat_id, "Нет известных пользователю записей для экспорта.")
        return

    rows = []
    for uid in sorted(uids):
        username = ''
        first_name = ''
        last_name = ''
        # Попробуем получить через API (если бот может получить данные)
        try:
            ch = bot.get_chat(uid)
            username = getattr(ch, 'username', '') or ''
            first_name = getattr(ch, 'first_name', '') or ''
            last_name = getattr(ch, 'last_name', '') or ''
        except Exception:
            # fallback: искать в local data
            for m in data.get('messages', []):
                try:
                    if int(m.get('from_id', -1)) == uid:
                        username = username or (m.get('from_username') or '')
                except Exception:
                    continue
            try:
                recs = data.get('records', {}).get(str(uid), [])
                if recs:
                    username = username or (recs[0].get('username') or '')
            except Exception:
                pass

        rows.append({
            'user_id': uid,
            'username': username,
            'first_name': first_name,
            'last_name': last_name
        })

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Создаём CSV файл — откроется как таблица в Excel/Google Sheets с колонками
    try:
        csv_buf = io.StringIO()
        writer = csv.writer(csv_buf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        
        # Заголовки как на скриншоте
        writer.writerow(['ts', 'type', 'user_id', 'username', 'spec', 'date', 'time', 'text', 'msg_id'])
        
        # Записываем все записи (records)
        for uid_str, recs in data.get('records', {}).items():
            for r in recs:
                writer.writerow([
                    r.get('ts', ''),
                    'record',
                    r.get('user_id', uid_str),
                    r.get('username', ''),
                    r.get('spec', ''),
                    r.get('date', ''),
                    r.get('time', ''),
                    '',
                    ''
                ])
        
        # Записываем все сообщения
        for m in data.get('messages', []):
            writer.writerow([
                m.get('ts', ''),
                m.get('tag', ''),
                m.get('from_id', ''),
                m.get('from_username', ''),
                m.get('spec', ''),
                '',
                '',
                m.get('text', ''),
                m.get('id', '')
            ])
        
        csv_bytes = io.BytesIO(csv_buf.getvalue().encode('utf-8-sig'))  # UTF-8 with BOM для корректного отображения в Excel
        csv_bytes.name = f'export_{ts}.csv'
        csv_bytes.seek(0)
        
        bot.send_document(chat_id, csv_bytes)
        bot.send_message(chat_id, f"✅ Экспорт выполнен. Файл откроется как таблица: {csv_bytes.name}")
    except Exception:
        err = traceback.format_exc()
        print('TXT export failed:', err)
        try:
            bot.send_message(chat_id, f"❌ Ошибка при формировании или отправке файла:\n{err}")
        except Exception:
            pass

# Регистрируем команды, чтобы /get_users отображалась слева от поля ввода
try:
    from telebot import types as _types_for_cmds
    bot.set_my_commands([
        _types_for_cmds.BotCommand('/start', 'Запустить бота'),
        _types_for_cmds.BotCommand('/get_users', 'Выгрузить всех пользователей (админ)')
    ])
except Exception:
    pass
# Debug: показать зарегистрированные у Telegram команды (админ)
@bot.message_handler(commands=['show_cmds'])
def cmd_show_cmds(message):
    if message.from_user.id not in ADMINS:
        return
    try:
        cmds = bot.get_my_commands()
        lines = [f"{c.command} — {c.description}" for c in cmds]
        bot.send_message(message.chat.id, "Текущие команды:\n" + "\n".join(lines))
    except Exception as e:
        bot.send_message(message.chat.id, "Не удалось получить команды: " + str(e))


print("Бот запущен...")
bot.infinity_polling()