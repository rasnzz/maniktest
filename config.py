import os
import json
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Проверка обязательных переменных
required_vars = ["BOT_TOKEN", "ADMIN_CHAT_IDS", "GOOGLE_SHEET_ID"]
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    raise EnvironmentError(
        f"Критические переменные окружения не установлены: {', '.join(missing_vars)}. "
        "Пожалуйста, создайте файл .env в корне проекта."
    )

# Настройки бота
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Список ID администраторов (в формате JSON-массива)
try:
    ADMIN_CHAT_IDS = json.loads(os.getenv("ADMIN_CHAT_IDS"))
    if not isinstance(ADMIN_CHAT_IDS, list):
        raise ValueError("ADMIN_CHAT_IDS должен быть списком")
except (json.JSONDecodeError, TypeError, ValueError) as e:
    raise ValueError("Неверный формат ADMIN_CHAT_IDS. Используйте JSON-массив, например: [123456, 789012]") from e

# Настройки работы салона
WORK_START = int(os.getenv("WORK_START", 9))    # Начало работы (9 утра)
WORK_END = int(os.getenv("WORK_END", 19))       # Конец работы (7 вечера)
TIME_SLOT_STEP = int(os.getenv("TIME_SLOT_STEP", 60))  # Шаг временных слотов в минутах

# Настройки Google Sheets
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "K1")  # Название листа по умолчанию

# Дополнительные проверки
if WORK_START < 0 or WORK_START > 23:
    raise ValueError("WORK_START должен быть между 0 и 23")

if WORK_END < 0 or WORK_END > 23:
    raise ValueError("WORK_END должен быть между 0 и 23")

if WORK_START >= WORK_END:
    raise ValueError("WORK_START должен быть меньше WORK_END")

if TIME_SLOT_STEP <= 0 or TIME_SLOT_STEP > 240:
    raise ValueError("TIME_SLOT_STEP должен быть между 1 и 240 минутами")

# Вывод информации о конфигурации (для отладки)
if __name__ == "__main__":
    print("Конфигурация успешно загружена:")
    print(f"BOT_TOKEN: {'установлен' if BOT_TOKEN else 'отсутствует'}")
    print(f"ADMIN_CHAT_IDS: {ADMIN_CHAT_IDS}")
    print(f"WORK_START: {WORK_START}:00")
    print(f"WORK_END: {WORK_END}:00")
    print(f"TIME_SLOT_STEP: {TIME_SLOT_STEP} мин")
    print(f"GOOGLE_SHEET_ID: {GOOGLE_SHEET_ID}")
    print(f"GOOGLE_SHEET_NAME: {GOOGLE_SHEET_NAME}")