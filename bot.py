import telebot
from telebot import types
import sqlite3
import datetime
import re
import threading
import time
import openpyxl
from openpyxl import Workbook
import os
import gspread
from google.oauth2.service_account import Credentials
import pytz
import logging
from dotenv import load_dotenv
import json

# Загрузка переменных окружения
load_dotenv()

# Настройки из config.py
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_CHAT_IDS = json.loads(os.getenv("ADMIN_CHAT_IDS", "[]"))
WORK_START = int(os.getenv("WORK_START", 9))
WORK_END = int(os.getenv("WORK_END", 19))
TIME_SLOT_STEP = int(os.getenv("TIME_SLOT_STEP", 60))
MIN_BOOKING_TIME = int(os.getenv("MIN_BOOKING_TIME", 60))
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "K1")
SALON_ADDRESS = os.getenv("SALON_ADDRESS", "ул. Примерная, 123")
SALON_PHONE = os.getenv("SALON_PHONE", "+7 (3532) 123-456")

# Инициализация бота
bot = telebot.TeleBot(BOT_TOKEN)

# Настройка логгирования
logging.basicConfig(
    filename='bot.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Часовой пояс для Оренбурга (UTC+5)
ORENBURG_TZ = pytz.timezone('Asia/Yekaterinburg')

# Словарь для хранения состояния пользователей
USER_STATE = {}

# --- Вспомогательные функции ---
def get_db_connection():
    """Создает соединение с БД с таймаутом"""
    return sqlite3.connect('salon.db', timeout=10)

def get_masters():
    """Получает список мастеров из БД"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT id, name FROM masters")
            return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения мастеров: {e}")
        return []

def get_services():
    """Получает список услуг из БД"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT id, name, duration, price FROM services")
            return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения услуг: {e}")
        return []

def save_appointment(chat_id, state):
    """Сохраняет запись в БД"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('''INSERT INTO appointments 
                        (client_id, client_name, phone, master_id, service_id, date, time) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)''',
                    (chat_id, state['client_name'], state['phone'], 
                     state['master_id'], state['service_id'], state['date'], state['time']))
            conn.commit()
            return c.lastrowid
    except Exception as e:
        logger.error(f"Ошибка сохранения записи: {e}")
        return None

# --- Google Sheets Integration ---
def get_google_sheet():
    """Аутентификация и доступ к таблице"""
    try:
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file(
            'credentials.json',
            scopes=scope
        )
        
        client = gspread.authorize(creds)
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        return sheet.worksheet(GOOGLE_SHEET_NAME)
    except Exception as e:
        logger.error(f"Ошибка доступа к Google Sheets: {e}")
        return None

def init_google_sheet():
    """Инициализация структуры таблицы"""
    try:
        worksheet = get_google_sheet()
        if not worksheet:
            return
            
        headers = [
            "ID", "Дата записи", "Время", "Клиент", "Телефон",
            "Мастер", "Услуга", "Длительность", "Цена", "Статус"
        ]
        worksheet.clear()
        worksheet.append_row(headers)
        logger.info("Google Sheet инициализирована")
    except Exception as e:
        logger.error(f"Ошибка инициализации Google Sheet: {e}")

def update_google_sheet(appointment_id, action="add"):
    """Обновляет Google Sheets при изменениях"""
    try:
        worksheet = get_google_sheet()
        if not worksheet:
            return
            
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""SELECT 
                        a.id, a.date, a.time, a.client_name, a.phone,
                        m.name, s.name, s.duration, s.price, a.status
                        FROM appointments a
                        JOIN masters m ON a.master_id = m.id
                        JOIN services s ON a.service_id = s.id
                        WHERE a.id = ?""", (appointment_id,))
            appointment = c.fetchone()
        
        if not appointment:
            return
        
        row = list(appointment)
        row[1] = datetime.datetime.strptime(row[1], '%Y-%m-%d').strftime('%d.%m.%Y')
        
        if action == "add":
            worksheet.append_row(row)
        elif action == "update":
            cell = worksheet.find(str(appointment_id))
            if cell:
                for i, value in enumerate(row, start=1):
                    worksheet.update_cell(cell.row, i, value)
                
    except Exception as e:
        logger.error(f"Ошибка обновления Google Sheet: {e}")

def sync_all_to_google():
    """Полная синхронизация с Google Sheets"""
    try:
        worksheet = get_google_sheet()
        if not worksheet:
            return
            
        worksheet.clear()
        headers = [
            "ID", "Дата записи", "Время", "Клиент", "Телефон",
            "Мастер", "Услуга", "Длительность", "Цена", "Статус"
        ]
        worksheet.append_row(headers)
        
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""SELECT 
                        a.id, a.date, a.time, a.client_name, a.phone,
                        m.name, s.name, s.duration, s.price, a.status
                        FROM appointments a
                        JOIN masters m ON a.master_id = m.id
                        JOIN services s ON a.service_id = s.id""")
            appointments = c.fetchall()
        
        batch = []
        for app in appointments:
            row = list(app)
            row[1] = datetime.datetime.strptime(row[1], '%Y-%m-%d').strftime('%d.%m.%Y')
            batch.append(row)
        
        if batch:
            worksheet.append_rows(batch)
        
        logger.info("Полная синхронизация с Google Sheets выполнена")
    except Exception as e:
        logger.error(f"Ошибка полной синхронизации: {e}")

# --- Автоматические напоминания ---
def send_reminders():
    """Функция отправки напоминаний"""
    while True:
        try:
            # Текущее время в Оренбурге
            now = datetime.datetime.now(ORENBURG_TZ)
            
            # Проверяем записи на завтра
            tomorrow = now + datetime.timedelta(days=1)
            date_str = tomorrow.strftime("%Y-%m-%d")
            send_day_reminders(date_str, "завтра")
            
            # Проверяем записи на сегодня (если сейчас утро)
            if now.hour < 10:  # Если сейчас утро (до 10:00)
                today_str = now.strftime("%Y-%m-%d")
                send_day_reminders(today_str, "сегодня")
            
            # Проверяем каждые 1 час
            time.sleep(3600)
            
        except Exception as e:
            logger.error(f"Ошибка в потоке напоминаний: {e}")
            time.sleep(60)

def send_day_reminders(date_str, day_text):
    """Отправляет напоминания на конкретный день"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""SELECT 
                        a.id, a.client_id, a.client_name, a.time,
                        m.name, s.name
                        FROM appointments a
                        JOIN masters m ON a.master_id = m.id
                        JOIN services s ON a.service_id = s.id
                        WHERE a.date = ? AND a.status = 'active'""", (date_str,))
            appointments = c.fetchall()
        
        for app in appointments:
            app_id, client_id, client_name, app_time, master_name, service_name = app
            message = (
                f"⏰ Напоминание о записи!\n\n"
                f"Здравствуйте, {client_name}!\n"
                f"{day_text.capitalize()} в {app_time} у вас запись к мастеру {master_name}\n"
                f"Услуга: {service_name}\n\n"
                f"📍 Адрес: {SALON_ADDRESS}\n"
                f"📱 Контакты: {SALON_PHONE}\n\n"
                f"Если не можете прийти, отмените запись через меню 'Мои записи'"
            )
            try:
                bot.send_message(client_id, message)
                logger.info(f"Отправлено напоминание клиенту {client_id}")
                
            except Exception as e:
                # Если пользователь заблокировал бота, помечаем запись
                if "bot was blocked" in str(e).lower():
                    logger.warning(f"Клиент {client_id} заблокировал бота, отменяем запись")
                    with get_db_connection() as conn:
                        c = conn.cursor()
                        c.execute("UPDATE appointments SET status='canceled' WHERE id=?", (app_id,))
                        conn.commit()
                else:
                    logger.error(f"Не удалось отправить напоминание {client_id}: {e}")
    
    except Exception as e:
        logger.error(f"Ошибка отправки напоминаний на {date_str}: {e}")

# --- Основные обработчики бота ---
def show_main_menu(chat_id):
    """Показывает главное меню с кнопками"""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton('📅 Записаться'))
    markup.add(types.KeyboardButton('📋 Мои записи'))
    markup.add(types.KeyboardButton('ℹ️ О салоне'))
    
    bot.send_message(
        chat_id,
        "👋 Добро пожаловать в наш салон красоты!\n"
        "Выберите действие:",
        reply_markup=markup
    )

@bot.message_handler(commands=['start'])
def start(message):
    """Обработчик команды /start"""
    show_main_menu(message.chat.id)

@bot.message_handler(func=lambda message: message.text == 'ℹ️ О салоне')
def about_salon(message):
    """Информация о салоне"""
    text = (
        f"💈 Наш салон красоты\n\n"
        f"🕒 Часы работы: {WORK_START}:00 - {WORK_END}:00\n"
        f"📍 Адрес: {SALON_ADDRESS}\n"
        f"📱 Телефон: {SALON_PHONE}\n\n"
        f"Мы предлагаем широкий спектр услуг по уходу за ногтями и кожей рук. "
        f"Наши мастера - профессионалы с большим опытом работы."
    )
    bot.send_message(message.chat.id, text)

@bot.message_handler(func=lambda message: message.text == '📅 Записаться')
def start_booking(message):
    """Начало процесса записи"""
    show_masters(message.chat.id)

def show_masters(chat_id):
    """Показывает список мастеров"""
    try:
        masters = get_masters()
        if not masters:
            bot.send_message(chat_id, "❌ В данный момент нет доступных мастеров")
            return
        
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        for master_id, name in masters:
            markup.add(types.KeyboardButton(f"Мастер {name}"))
        
        markup.add(types.KeyboardButton('↩️ Назад'))
        bot.send_message(chat_id, "👩‍🎨 Выберите мастера:", reply_markup=markup)
        USER_STATE[chat_id] = {'step': 'select_master'}
    except Exception as e:
        logger.error(f"Ошибка показа мастеров: {e}")
        bot.send_message(chat_id, "❌ Произошла ошибка. Попробуйте позже.")

@bot.message_handler(func=lambda message: message.text == '↩️ Назад')
def back_to_main(message):
    """Возврат в главное меню"""
    show_main_menu(message.chat.id)

@bot.message_handler(func=lambda message: USER_STATE.get(message.chat.id, {}).get('step') == 'select_master')
def select_master(message):
    """Обрабатывает выбор мастера"""
    try:
        if message.text == '↩️ Назад':
            show_main_menu(message.chat.id)
            return
            
        masters = get_masters()
        selected = None
        
        for master_id, name in masters:
            if f"Мастер {name}" in message.text:
                selected = (master_id, name)
                break
        
        if selected:
            USER_STATE[message.chat.id] = {
                'step': 'select_service',
                'master_id': selected[0],
                'master_name': selected[1]
            }
            show_services(message.chat.id)
        else:
            bot.send_message(message.chat.id, "❌ Пожалуйста, выберите мастера из списка")
            show_masters(message.chat.id)
    except Exception as e:
        logger.error(f"Ошибка выбора мастера: {e}")
        bot.send_message(message.chat.id, "❌ Произошла ошибка. Попробуйте снова.")

def show_services(chat_id):
    """Показывает список услуг"""
    try:
        services = get_services()
        if not services:
            bot.send_message(chat_id, "❌ В данный момент нет доступных услуг")
            return
        
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        for service_id, name, duration, price in services:
            hours = duration // 60
            minutes = duration % 60
            duration_str = f"{hours}ч {minutes}мин" if hours else f"{minutes}мин"
            markup.add(types.KeyboardButton(f"{name} ({duration_str}) - {price}₽"))
        
        markup.add(types.KeyboardButton('↩️ Назад'))
        bot.send_message(chat_id, "💅 Выберите услугу:", reply_markup=markup)
    except Exception as e:
        logger.error(f"Ошибка показа услуг: {e}")
        bot.send_message(chat_id, "❌ Произошла ошибка. Попробуйте позже.")

@bot.message_handler(func=lambda message: USER_STATE.get(message.chat.id, {}).get('step') == 'select_service')
def select_service(message):
    """Обрабатывает выбор услуги"""
    try:
        if message.text == '↩️ Назад':
            show_masters(message.chat.id)
            return
            
        services = get_services()
        selected = None
        
        for service_id, name, duration, price in services:
            if name in message.text:
                selected = (service_id, name, duration, price)
                break
        
        if selected:
            USER_STATE[message.chat.id].update({
                'step': 'get_name',
                'service_id': selected[0],
                'service_name': selected[1],
                'duration': selected[2],
                'price': selected[3]
            })
            bot.send_message(
                message.chat.id, 
                "📝 Введите ваше имя:",
                reply_markup=types.ReplyKeyboardRemove()
            )
        else:
            bot.send_message(message.chat.id, "❌ Пожалуйста, выберите услугу из списка")
            show_services(message.chat.id)
    except Exception as e:
        logger.error(f"Ошибка выбора услуги: {e}")
        bot.send_message(message.chat.id, "❌ Произошла ошибка. Попробуйте снова.")

@bot.message_handler(func=lambda message: USER_STATE.get(message.chat.id, {}).get('step') == 'get_name')
def get_client_name(message):
    """Получает имя клиента"""
    try:
        name = message.text.strip()
        if name and 2 <= len(name) <= 50:
            USER_STATE[message.chat.id]['client_name'] = name
            USER_STATE[message.chat.id]['step'] = 'get_phone'
            bot.send_message(
                message.chat.id, 
                "📱 Введите ваш телефон (пример: +79161234567):"
            )
        else:
            bot.send_message(message.chat.id, "❌ Имя должно быть от 2 до 50 символов. Введите ваше имя:")
    except Exception as e:
        logger.error(f"Ошибка получения имени: {e}")
        bot.send_message(message.chat.id, "❌ Произошла ошибка. Попробуйте снова.")

@bot.message_handler(func=lambda message: USER_STATE.get(message.chat.id, {}).get('step') == 'get_phone')
def get_client_phone(message):
    """Получает телефон клиента"""
    try:
        phone = message.text.strip()
        cleaned_phone = re.sub(r'\D', '', phone)  # Удаляем все нецифровые символы
        
        # Проверяем российские номера
        if len(cleaned_phone) == 11 and cleaned_phone.startswith(('7', '8')):
            formatted_phone = f"+7{cleaned_phone[1:]}"
            USER_STATE[message.chat.id]['phone'] = formatted_phone
            USER_STATE[message.chat.id]['step'] = 'select_date'
            show_calendar(message.chat.id)
        else:
            bot.send_message(
                message.chat.id, 
                "❌ Неверный формат телефона. Пример: +79161234567 или 89161234567\n" 
                "Пожалуйста, введите телефон еще раз:"
            )
    except Exception as e:
        logger.error(f"Ошибка получения телефона: {e}")
        bot.send_message(message.chat.id, "❌ Произошла ошибка. Попробуйте снова.")

def show_calendar(chat_id):
    """Показывает календарь на 7 дней"""
    try:
        today = datetime.datetime.now(ORENBURG_TZ).date()
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=7)
        
        for i in range(7):
            date = today + datetime.timedelta(days=i)
            btn_text = date.strftime("%d.%m")
            markup.add(types.KeyboardButton(btn_text))
        
        markup.add(types.KeyboardButton('↩️ Назад'))
        bot.send_message(chat_id, "📅 Выберите дату:", reply_markup=markup)
        USER_STATE[chat_id]['step'] = 'select_date'
    except Exception as e:
        logger.error(f"Ошибка показа календаря: {e}")
        bot.send_message(chat_id, "❌ Произошла ошибка. Попробуйте позже.")

@bot.message_handler(func=lambda message: USER_STATE.get(message.chat.id, {}).get('step') == 'select_date')
def select_date(message):
    """Обрабатывает выбор даты"""
    try:
        if message.text == '↩️ Назад':
            show_services(message.chat.id)
            return
            
        day, month = map(int, message.text.split('.'))
        now = datetime.datetime.now(ORENBURG_TZ)
        today = now.date()
        year = today.year
        
        # Проверяем, не прошла ли дата в этом году
        try:
            selected_date = datetime.date(year, month, day)
        except ValueError:
            selected_date = None
        
        # Если дата в прошлом, пробуем следующий год
        if not selected_date or selected_date < today:
            try:
                selected_date = datetime.date(year + 1, month, day)
            except ValueError:
                selected_date = None
        
        if selected_date and selected_date >= today:
            USER_STATE[message.chat.id]['date'] = selected_date.strftime("%Y-%m-%d")
            USER_STATE[message.chat.id]['step'] = 'select_time'
            show_time_slots(message.chat.id)
        else:
            bot.send_message(message.chat.id, "❌ Неверная дата! Используйте формат ДД.ММ")
            show_calendar(message.chat.id)
    except:
        bot.send_message(message.chat.id, "❌ Неверный формат даты! Используйте ДД.ММ")
        show_calendar(message.chat.id)

def show_time_slots(chat_id):
    """Показывает доступные временные слоты с учетом текущего времени"""
    try:
        state = USER_STATE[chat_id]
        master_id = state['master_id']
        selected_date = state['date']
        service_duration = state['duration']
        
        # Текущее время в Оренбурге
        now = datetime.datetime.now(ORENBURG_TZ)
        today = now.date()
        selected_date_obj = datetime.datetime.strptime(selected_date, '%Y-%m-%d').date()
        
        # Получаем занятые слоты из БД
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("""SELECT a.time, s.duration 
                       FROM appointments a
                       JOIN services s ON a.service_id = s.id
                       WHERE master_id=? AND date=? AND status='active'""", 
                     (master_id, selected_date))
            booked_slots = c.fetchall()
        
        # Генерируем доступные слоты
        available_slots = []
        current_time = datetime.time(WORK_START, 0)
        end_time = datetime.time(WORK_END, 0)
        
        # Если выбрана сегодняшняя дата, начинаем с текущего времени + минимальный интервал
        if selected_date_obj == today:
            min_time = (now + datetime.timedelta(minutes=MIN_BOOKING_TIME)).time()
            if min_time > current_time:
                current_time = min_time
        
        while current_time < end_time:
            slot_str = current_time.strftime("%H:%M")
            
            # Рассчитываем время окончания услуги
            current_dt = datetime.datetime.combine(datetime.date.today(), current_time)
            end_service_dt = current_dt + datetime.timedelta(minutes=service_duration)
            end_service_time = end_service_dt.time()
            
            # Проверяем, что услуга помещается в рабочее время
            if end_service_time > end_time:
                current_time = (current_dt + datetime.timedelta(minutes=TIME_SLOT_STEP)).time()
                continue
            
            # Проверяем пересечение с занятыми слотами
            slot_free = True
            for booked_time, booked_duration in booked_slots:
                booked_time = datetime.datetime.strptime(booked_time, "%H:%M").time()
                booked_end_dt = datetime.datetime.combine(datetime.date.today(), booked_time) + datetime.timedelta(minutes=booked_duration)
                booked_end_time = booked_end_dt.time()
                
                # Проверка пересечения временных интервалов
                if not (end_service_time <= booked_time or current_time >= booked_end_time):
                    slot_free = False
                    break
            
            if slot_free:
                available_slots.append(slot_str)
            
            # Переходим к следующему слоту
            current_time = (current_dt + datetime.timedelta(minutes=TIME_SLOT_STEP)).time()
        
        # Создаем клавиатуру
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=4)
        for time_slot in available_slots:
            markup.add(types.KeyboardButton(time_slot))
        
        markup.add(types.KeyboardButton('↩️ Назад'))
        
        if available_slots:
            bot.send_message(chat_id, "⏰ Выберите время:", reply_markup=markup)
        else:
            bot.send_message(chat_id, "😢 На этот день нет свободных слотов")
            show_calendar(chat_id)
    except Exception as e:
        logger.error(f"Ошибка показа слотов времени: {e}")
        bot.send_message(chat_id, "❌ Произошла ошибка. Попробуйте выбрать другую дату.")

@bot.message_handler(func=lambda message: USER_STATE.get(message.chat.id, {}).get('step') == 'select_time')
def select_time(message):
    """Обрабатывает выбор времени"""
    try:
        if message.text == '↩️ Назад':
            show_calendar(message.chat.id)
            return
            
        time_str = message.text
        if re.match(r'^\d{1,2}:\d{2}$', time_str):
            USER_STATE[message.chat.id]['time'] = time_str
            confirm_booking(message.chat.id)
        else:
            bot.send_message(message.chat.id, "❌ Неверный формат времени! Используйте ЧЧ:ММ")
            show_time_slots(message.chat.id)
    except Exception as e:
        logger.error(f"Ошибка выбора времени: {e}")
        bot.send_message(message.chat.id, "❌ Произошла ошибка. Попробуйте снова.")

def confirm_booking(chat_id):
    """Показывает подтверждение записи"""
    try:
        state = USER_STATE[chat_id]
        
        # Форматируем дату
        date_obj = datetime.datetime.strptime(state['date'], '%Y-%m-%d')
        formatted_date = date_obj.strftime('%d.%m.%Y')
        
        # Создаем сообщение без номера записи
        text = (
            f"✅ Подтвердите запись:\n\n"
            f"👩‍🎨 Мастер: {state['master_name']}\n"
            f"💅 Услуга: {state['service_name']} - {state['price']}₽\n"
            f"⏱ Длительность: {state['duration']} мин\n"
            f"📅 Дата: {formatted_date}\n"
            f"⏰ Время: {state['time']}\n"
            f"👤 Имя: {state['client_name']}\n"
            f"📱 Телефон: {state['phone']}"
        )
        
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        markup.add(types.KeyboardButton('Да, подтверждаю'))
        markup.add(types.KeyboardButton('Отменить запись'))
        
        bot.send_message(chat_id, text, reply_markup=markup)
        USER_STATE[chat_id]['step'] = 'confirmation'
    except Exception as e:
        logger.error(f"Ошибка подтверждения записи: {e}")
        bot.send_message(chat_id, "❌ Произошла ошибка. Попробуйте снова.")

@bot.message_handler(func=lambda message: USER_STATE.get(message.chat.id, {}).get('step') == 'confirmation')
def finalize_booking(message):
    """Завершает процесс записи и показывает главное меню"""
    try:
        chat_id = message.chat.id
        
        if message.text == 'Да, подтверждаю':
            appointment_id = save_appointment(chat_id, USER_STATE[chat_id])
            
            if appointment_id:
                # Отправляем сообщение об успехе
                bot.send_message(
                    chat_id, 
                    "🎉 Запись успешно сохранена! Ждем вас в салоне.",
                    reply_markup=types.ReplyKeyboardRemove()
                )
                
                # Отправляем уведомление администраторам
                state = USER_STATE[chat_id]
                admin_msg = (
                    f"📝 Новая запись! (#{appointment_id})\n"
                    f"👤 Клиент: {state['client_name']}\n"
                    f"📱 Тел: {state['phone']}\n"
                    f"👩‍🎨 Мастер: {state['master_name']}\n"
                    f"💅 Услуга: {state['service_name']}\n"
                    f"📅 {state['date']} {state['time']}"
                )
                
                for admin_id in ADMIN_CHAT_IDS:
                    try:
                        bot.send_message(admin_id, admin_msg)
                    except Exception as e:
                        logger.error(f"Не удалось отправить уведомление админу {admin_id}: {e}")
                
                # Обновляем Google Sheets
                update_google_sheet(appointment_id, "add")
            else:
                bot.send_message(chat_id, "❌ Ошибка при сохранении записи")
        else:
            bot.send_message(chat_id, "❌ Запись отменена", reply_markup=types.ReplyKeyboardRemove())
        
        # Очищаем состояние
        if chat_id in USER_STATE:
            del USER_STATE[chat_id]
            
        # Всегда показываем главное меню после завершения
        show_main_menu(chat_id)
        
    except Exception as e:
        logger.error(f"Ошибка завершения записи: {e}")
        bot.send_message(chat_id, "❌ Произошла ошибка. Пожалуйста, начните заново.")
        show_main_menu(chat_id)

# --- Просмотр и отмена записей пользователем ---
@bot.message_handler(func=lambda message: message.text == '📋 Мои записи')
def view_my_bookings(message):
    """Показывает активные записи пользователя с порядковыми номерами"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('''SELECT 
                        a.id, a.date, a.time, m.name, s.name 
                        FROM appointments a
                        JOIN masters m ON a.master_id = m.id
                        JOIN services s ON a.service_id = s.id
                        WHERE a.client_id = ? AND a.status = 'active'
                        ORDER BY a.date, a.time''',
                    (message.chat.id,))
            bookings = c.fetchall()
        
        if not bookings:
            bot.send_message(message.chat.id, "📭 У вас нет активных записей")
            return
        
        response = "📋 Ваши активные записи:\n\n"
        markup = types.InlineKeyboardMarkup()
        
        # Используем порядковый номер вместо ID записи
        for idx, booking in enumerate(bookings, 1):
            app_id, date, time, master, service = booking
            date_formatted = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%d.%m.%Y')
            
            response += (
                f"🔹 <b>Запись #{idx}</b>\n"
                f"⏰ {date_formatted} в {time}\n"
                f"👩‍🎨 Мастер: {master}\n"
                f"💅 Услуга: {service}\n"
                f"——————————————\n"
            )
            
            # Используем реальный ID записи в callback_data
            markup.add(types.InlineKeyboardButton(
                text=f"❌ Отменить запись #{idx}",
                callback_data=f"cancel_{app_id}"
            ))
        
        bot.send_message(
            message.chat.id, 
            response, 
            reply_markup=markup,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Ошибка показа записей пользователя: {e}")
        bot.send_message(message.chat.id, "❌ Произошла ошибка. Попробуйте позже.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('cancel_'))
def cancel_booking_callback(call):
    """Обрабатывает отмену записи клиентом"""
    try:
        # Получаем реальный ID записи из callback_data
        appointment_id = call.data.split('_')[1]
        chat_id = call.message.chat.id
        
        with get_db_connection() as conn:
            c = conn.cursor()
            
            # Проверяем принадлежность записи
            c.execute("SELECT id, date, time FROM appointments WHERE id=? AND client_id=?", 
                      (appointment_id, chat_id))
            appointment = c.fetchone()
            
            if not appointment:
                bot.answer_callback_query(call.id, "❌ Запись не найдена или не принадлежит вам")
                return
                
            # Обновляем статус записи
            c.execute("UPDATE appointments SET status='canceled' WHERE id=?", (appointment_id,))
            conn.commit()
            
            # Обновляем Google Sheets
            update_google_sheet(appointment_id, "update")
            
            # Форматируем дату для сообщения
            app_id, date, time = appointment
            date_formatted = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%d.%m.%Y')
            
            # Уведомляем пользователя (без номера записи)
            bot.answer_callback_query(call.id, "✅ Запись отменена")
            bot.send_message(
                chat_id, 
                f"❌ Ваша запись на {date_formatted} в {time} отменена"
            )
            
            # Уведомляем администраторов
            for admin_id in ADMIN_CHAT_IDS:
                try:
                    bot.send_message(
                        admin_id, 
                        f"❌ Клиент отменил запись #{appointment_id}\n"
                        f"Дата: {date} {time}\n"
                        f"ID клиента: {chat_id}"
                    )
                except Exception as e:
                    logger.error(f"Не удалось отправить уведомление админу {admin_id}: {e}")
            
            # Обновляем список записей
            view_my_bookings(call.message)
            
    except Exception as e:
        logger.error(f"Ошибка отмены записи: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка при отмене записи")

# --- Административные команды ---
@bot.message_handler(commands=['admin'])
def admin_panel(message):
    """Панель администратора"""
    if message.chat.id not in ADMIN_CHAT_IDS:
        bot.send_message(message.chat.id, "⛔ Доступ запрещен")
        return
    
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    btn1 = types.KeyboardButton('Активные записи')
    btn2 = types.KeyboardButton('Все записи')
    btn3 = types.KeyboardButton('Экспорт в Excel')
    btn4 = types.KeyboardButton('Синхронизировать с Google')
    markup.add(btn1, btn2, btn3, btn4)
    bot.send_message(message.chat.id, "Админ-панель:", reply_markup=markup)

def get_appointments(status='active'):
    """Получает записи из БД"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            if status == 'all':
                c.execute("""SELECT 
                            a.id, a.client_name, a.phone, 
                            m.name, s.name, a.date, a.time 
                            FROM appointments a
                            JOIN masters m ON a.master_id = m.id
                            JOIN services s ON a.service_id = s.id
                            ORDER BY a.date, a.time""")
            else:
                c.execute("""SELECT 
                            a.id, a.client_name, a.phone, 
                            m.name, s.name, a.date, a.time 
                            FROM appointments a
                            JOIN masters m ON a.master_id = m.id
                            JOIN services s ON a.service_id = s.id
                            WHERE a.status = 'active'
                            ORDER BY a.date, a.time""")
            return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения записей: {e}")
        return []

@bot.message_handler(func=lambda message: message.text == 'Активные записи' and message.chat.id in ADMIN_CHAT_IDS)
def show_active_appointments(message):
    """Показывает активные записи"""
    appointments = get_appointments()
    
    if not appointments:
        bot.send_message(message.chat.id, "Активных записей нет")
        return
    
    response = "📋 Активные записи:\n\n"
    for app in appointments:
        app_id, client_name, phone, master_name, service_name, date, time = app
        date_formatted = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%d.%m.%Y')
        response += (
            f"🔹 #{app_id}\n"
            f"👤 {client_name} | 📱 {phone}\n"
            f"👩‍🎨 Мастер: {master_name}\n"
            f"💅 Услуга: {service_name}\n"
            f"⏰ {date_formatted} в {time}\n"
            f"————————————————\n"
        )
    
    bot.send_message(message.chat.id, response)

@bot.message_handler(func=lambda message: message.text == 'Все записи' and message.chat.id in ADMIN_CHAT_IDS)
def show_all_appointments(message):
    """Показывает все записи"""
    appointments = get_appointments('all')
    
    if not appointments:
        bot.send_message(message.chat.id, "Записей нет")
        return
    
    response = "📋 Все записи:\n\n"
    for app in appointments:
        app_id, client_name, phone, master_name, service_name, date, time = app
        date_formatted = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%d.%m.%Y')
        response += (
            f"🔹 #{app_id}\n"
            f"👤 {client_name} | 📱 {phone}\n"
            f"👩‍🎨 Мастер: {master_name}\n"
            f"💅 Услуга: {service_name}\n"
            f"⏰ {date_formatted} в {time}\n"
            f"————————————————\n"
        )
    
    bot.send_message(message.chat.id, response)

@bot.message_handler(func=lambda message: message.text == 'Экспорт в Excel' and message.chat.id in ADMIN_CHAT_IDS)
def export_to_excel(message):
    """Экспорт расписания в Excel"""
    appointments = get_appointments('all')
    
    if not appointments:
        bot.send_message(message.chat.id, "Нет записей для экспорта")
        return
    
    try:
        wb = Workbook()
        ws = wb.active
        ws.title = "Расписание"
        
        headers = ["ID", "Дата", "Время", "Клиент", "Телефон", "Мастер", "Услуга"]
        ws.append(headers)
        
        for app in appointments:
            app_id, client_name, phone, master_name, service_name, date, time = app
            date_formatted = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%d.%m.%Y')
            ws.append([app_id, date_formatted, time, client_name, phone, master_name, service_name])
        
        for col in range(1, len(headers) + 1):
            cell = ws.cell(row=1, column=col)
            cell.font = openpyxl.styles.Font(bold=True)
        
        for col in ws.columns:
            max_length = 0
            column = col[0].column_letter
            for cell in col:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = (max_length + 2)
            ws.column_dimensions[column].width = adjusted_width
        
        filename = f"schedule_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        wb.save(filename)
        
        with open(filename, 'rb') as file:
            bot.send_document(message.chat.id, file, caption="📊 Расписание записей")
        
        os.remove(filename)
    except Exception as e:
        logger.error(f"Ошибка экспорта в Excel: {e}")
        bot.send_message(message.chat.id, f"❌ Ошибка при экспорте: {str(e)}")

@bot.message_handler(func=lambda message: message.text == 'Синхронизировать с Google' and message.chat.id in ADMIN_CHAT_IDS)
def sync_google_sheet(message):
    """Ручная синхронизация с Google Sheets"""
    try:
        sync_all_to_google()
        bot.send_message(message.chat.id, "✅ Google Sheets успешно синхронизирована")
    except Exception as e:
        logger.error(f"Ошибка синхронизации с Google Sheets: {e}")
        bot.send_message(message.chat.id, f"❌ Ошибка синхронизации: {str(e)}")

@bot.message_handler(func=lambda message: message.text.startswith('/cancel_') and message.chat.id in ADMIN_CHAT_IDS)
def cancel_appointment(message):
    """Отменяет запись по ID"""
    try:
        appointment_id = message.text.split('_')[1]
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE appointments SET status='canceled' WHERE id=?", (appointment_id,))
            conn.commit()
            
            c.execute("SELECT client_id, date, time FROM appointments WHERE id=?", (appointment_id,))
            result = c.fetchone()
        
        if result:
            client_id, date, time = result
            # Обновляем Google Sheets
            update_google_sheet(appointment_id, "update")
            
            # Уведомляем клиента
            date_formatted = datetime.datetime.strptime(date, '%Y-%m-%d').strftime('%d.%m.%Y')
            bot.send_message(
                client_id, 
                f"❗ Ваша запись на {date_formatted} в {time} отменена администратором"
            )
        
        bot.send_message(message.chat.id, f"✅ Запись #{appointment_id} отменена")
    except Exception as e:
        logger.error(f"Ошибка отмены записи: {e}")
        bot.send_message(message.chat.id, "❌ Ошибка отмены записи. Используйте: /cancel_ID")

# --- Фоновая синхронизация ---
def background_sync():
    """Фоновая синхронизация каждые 10 минут"""
    while True:
        try:
            sync_all_to_google()
            time.sleep(600)  # 10 минут
        except Exception as e:
            logger.error(f"Ошибка фоновой синхронизации: {e}")
            time.sleep(60)

# Запуск бота
if __name__ == "__main__":
    # Инициализируем базу данных
    from database import init_db
    init_db()
    
    # Инициализация Google Sheets
    try:
        init_google_sheet()
        sync_all_to_google()
        logger.info("Google Sheets инициализирована")
    except Exception as e:
        logger.error(f"Ошибка инициализации Google Sheets: {e}")
    
    # Запускаем фоновые потоки
    threading.Thread(target=send_reminders, daemon=True).start()
    threading.Thread(target=background_sync, daemon=True).start()
    
    logger.info("Бот запущен...")
    bot.infinity_polling()