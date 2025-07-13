import sqlite3
import logging
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(
    filename='database.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('database')

def get_db_connection():
    """Создает и возвращает соединение с БД"""
    conn = sqlite3.connect('salon.db', timeout=10)
    conn.execute("PRAGMA foreign_keys = ON")  # Включаем поддержку внешних ключей
    return conn

def init_db():
    """Инициализирует структуру базы данных"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Таблица мастеров
        c.execute('''CREATE TABLE IF NOT EXISTS masters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    is_active BOOLEAN DEFAULT 1)''')
        
        # Таблица услуг
        c.execute('''CREATE TABLE IF NOT EXISTS services (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    duration INTEGER DEFAULT 60 CHECK(duration > 0 AND duration <= 240),
                    price REAL CHECK(price >= 0),
                    is_active BOOLEAN DEFAULT 1)''')
        
        # Таблица записей
        c.execute('''CREATE TABLE IF NOT EXISTS appointments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id INTEGER NOT NULL,
                    client_name TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    master_id INTEGER NOT NULL,
                    service_id INTEGER NOT NULL,
                    date TEXT NOT NULL,  -- Формат: YYYY-MM-DD
                    time TEXT NOT NULL,  -- Формат: HH:MM
                    status TEXT DEFAULT 'active' CHECK(status IN ('active', 'canceled', 'completed')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(master_id) REFERENCES masters(id) ON DELETE RESTRICT,
                    FOREIGN KEY(service_id) REFERENCES services(id) ON DELETE RESTRICT)''')
        
        # Проверяем наличие столбца reminder_sent и добавляем если нужно
        c.execute("PRAGMA table_info(appointments)")
        columns = [col[1] for col in c.fetchall()]
        
        if 'reminder_sent' not in columns:
            c.execute("ALTER TABLE appointments ADD COLUMN reminder_sent BOOLEAN DEFAULT 0")
            logger.info("Добавлен столбец reminder_sent")
        
        # Индексы для ускорения запросов
        c.execute("CREATE INDEX IF NOT EXISTS idx_appointments_date ON appointments(date)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_appointments_master ON appointments(master_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_appointments_status ON appointments(status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_appointments_reminder ON appointments(reminder_sent)")
        
        # Добавляем мастеров только если их нет
        default_masters = [('Анна',), ('Мария',), ('Екатерина',)]
        c.executemany("INSERT OR IGNORE INTO masters (name) VALUES (?)", default_masters)
        
        # Добавляем услуги только если их нет
        default_services = [
            ('Маникюр', 60, 1200),
            ('Покрытие гель-лаком', 60, 1800),
            ('Наращивание ногтей', 90, 2500),
            ('Дизайн ногтей', 30, 500)
        ]
        c.executemany("INSERT OR IGNORE INTO services (name, duration, price) VALUES (?, ?, ?)", default_services)
        
        conn.commit()
        logger.info("База данных успешно инициализирована")
    except Exception as e:
        logger.error(f"Ошибка инициализации БД: {e}")
        raise
    finally:
        conn.close()

def add_test_data():
    """Добавляет тестовые данные в БД"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Тестовые записи
        test_appointments = [
            (123456789, 'Иван Иванов', '+79161234567', 1, 1, '2023-12-15', '10:00'),
            (987654321, 'Мария Петрова', '+79167654321', 2, 2, '2023-12-15', '11:30'),
        ]
        
        c.executemany('''INSERT INTO appointments 
                      (client_id, client_name, phone, master_id, service_id, date, time)
                      VALUES (?, ?, ?, ?, ?, ?, ?)''', test_appointments)
        
        conn.commit()
        logger.info("Тестовые данные успешно добавлены")
    except Exception as e:
        logger.error(f"Ошибка добавления тестовых данных: {e}")
    finally:
        conn.close()

def get_masters(only_active=True):
    """Возвращает список мастеров"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        if only_active:
            c.execute("SELECT id, name FROM masters WHERE is_active = 1")
        else:
            c.execute("SELECT id, name FROM masters")
            
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения мастеров: {e}")
        return []
    finally:
        conn.close()

def get_services(only_active=True):
    """Возвращает список услуг"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        if only_active:
            c.execute("SELECT id, name, duration, price FROM services WHERE is_active = 1")
        else:
            c.execute("SELECT id, name, duration, price FROM services")
            
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения услуг: {e}")
        return []
    finally:
        conn.close()

def get_appointments_by_master(master_id, date, status='active'):
    """Возвращает записи мастера на указанную дату"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        query = """SELECT a.time, s.duration 
                   FROM appointments a
                   JOIN services s ON a.service_id = s.id
                   WHERE a.master_id = ? AND a.date = ? AND a.status = ?"""
        c.execute(query, (master_id, date, status))
        
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения записей мастера: {e}")
        return []
    finally:
        conn.close()

def add_appointment(client_id, client_name, phone, master_id, service_id, date, time):
    """Добавляет новую запись"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute('''INSERT INTO appointments 
                    (client_id, client_name, phone, master_id, service_id, date, time) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (client_id, client_name, phone, master_id, service_id, date, time))
        
        appointment_id = c.lastrowid
        conn.commit()
        logger.info(f"Запись #{appointment_id} успешно добавлена")
        return appointment_id
    except Exception as e:
        logger.error(f"Ошибка добавления записи: {e}")
        return None
    finally:
        conn.close()

def update_appointment_status(appointment_id, status):
    """Обновляет статус записи"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute('''UPDATE appointments 
                    SET status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?''', 
                (status, appointment_id))
        
        conn.commit()
        logger.info(f"Статус записи #{appointment_id} изменен на '{status}'")
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления статуса записи #{appointment_id}: {e}")
        return False
    finally:
        conn.close()

def mark_reminder_sent(appointment_id):
    """Помечает, что напоминание для записи было отправлено"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute('''UPDATE appointments 
                    SET reminder_sent = 1
                    WHERE id = ?''', 
                (appointment_id,))
        
        conn.commit()
        logger.info(f"Напоминание для записи #{appointment_id} помечено как отправленное")
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления статуса напоминания #{appointment_id}: {e}")
        return False
    finally:
        conn.close()

def get_tomorrows_appointments():
    """Возвращает активные записи на завтра"""
    try:
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute('''SELECT 
                    a.id, a.client_id, a.client_name, a.time,
                    m.name, s.name
                    FROM appointments a
                    JOIN masters m ON a.master_id = m.id
                    JOIN services s ON a.service_id = s.id
                    WHERE a.date = ? AND a.status = 'active' AND a.reminder_sent = 0''', 
                (tomorrow,))
        
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения завтрашних записей: {e}")
        return []
    finally:
        conn.close()

def get_client_appointments(client_id, status='active'):
    """Возвращает записи клиента"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute('''SELECT 
                    a.id, a.date, a.time, m.name, s.name 
                    FROM appointments a
                    JOIN masters m ON a.master_id = m.id
                    JOIN services s ON a.service_id = s.id
                    WHERE a.client_id = ? AND a.status = ?
                    ORDER BY a.date, a.time''',
                (client_id, status))
        
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения записей клиента {client_id}: {e}")
        return []
    finally:
        conn.close()

def get_all_appointments(status=None):
    """Возвращает все записи (для администратора)"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        if status:
            c.execute('''SELECT 
                        a.id, a.client_name, a.phone, 
                        m.name, s.name, a.date, a.time 
                        FROM appointments a
                        JOIN masters m ON a.master_id = m.id
                        JOIN services s ON a.service_id = s.id
                        WHERE a.status = ?
                        ORDER BY a.date, a.time''', (status,))
        else:
            c.execute('''SELECT 
                        a.id, a.client_name, a.phone, 
                        m.name, s.name, a.date, a.time, a.status
                        FROM appointments a
                        JOIN masters m ON a.master_id = m.id
                        JOIN services s ON a.service_id = s.id
                        ORDER BY a.date, a.time''')
        
        return c.fetchall()
    except Exception as e:
        logger.error(f"Ошибка получения всех записей: {e}")
        return []
    finally:
        conn.close()

def get_appointment_details(appointment_id):
    """Возвращает детали записи по ID"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        c.execute('''SELECT 
                    a.id, a.client_id, a.client_name, a.phone,
                    a.master_id, m.name AS master_name,
                    a.service_id, s.name AS service_name,
                    a.date, a.time, a.status
                    FROM appointments a
                    JOIN masters m ON a.master_id = m.id
                    JOIN services s ON a.service_id = s.id
                    WHERE a.id = ?''', (appointment_id,))
        
        return c.fetchone()
    except Exception as e:
        logger.error(f"Ошибка получения деталей записи #{appointment_id}: {e}")
        return None
    finally:
        conn.close()

if __name__ == "__main__":
    # Инициализация БД при прямом запуске
    print("Инициализация базы данных...")
    init_db()
    
    # Опционально: добавить тестовые данные
    # add_test_data()
    
    print("Проверка мастеров:")
    print(get_masters())
    
    print("\nПроверка услуг:")
    print(get_services())
    
    print("\nБаза данных готова к использованию")