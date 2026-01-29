from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from datetime import datetime, timedelta, timezone
import os
import logging

Base = declarative_base()
logger = logging.getLogger(__name__)

class WeatherData(Base):
    __tablename__ = 'weather_data'
    
    id = Column(Integer, primary_key=True)
    city = Column(String(100), nullable=False)
    temperature = Column(Float, nullable=False)
    humidity = Column(Integer)
    pressure = Column(Integer)
    description = Column(String(200))
    wind_speed = Column(Float)
    clouds = Column(Integer)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    def __repr__(self):
        return f"Weather(city='{self.city}', temp={self.temperature}°C, desc='{self.description}')"

def get_database_url():
    """Получаем URL БД из переменных окружения или используем дефолтный"""
    return os.getenv('DATABASE_URL', 'postgresql://airflow:airflow@postgres/airflow')

def init_db():
    """Инициализация базы данных и создание таблиц"""
    try:
        engine = create_engine(get_database_url())
        
        # Проверяем существование таблицы
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'weather_data'
                );
            """))
            table_exists = result.scalar()
        
        # Создаем таблицу если её нет
        if not table_exists:
            print("📦 Создаю таблицу weather_data...")
            Base.metadata.create_all(engine)
            print("✅ Таблица создана")
            
            # Создаём индексы для оптимизации запросов
            with engine.connect() as conn:
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_weather_data_city_created 
                    ON weather_data(city, created_at DESC);
                """))
                conn.commit()
            print("✅ Индексы созданы")
        else:
            print("✅ Таблица weather_data уже существует")
            # Проверяем и создаём индексы если их нет
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM pg_indexes 
                        WHERE tablename = 'weather_data' 
                        AND indexname = 'idx_weather_data_city_created'
                    );
                """))
                index_exists = result.scalar()
                if not index_exists:
                    conn.execute(text("""
                        CREATE INDEX idx_weather_data_city_created 
                        ON weather_data(city, created_at DESC);
                    """))
                    conn.commit()
                    print("✅ Индексы созданы")
        
        return engine
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")
        raise

def get_session():
    """Получить сессию БД"""
    engine = init_db()
    Session = sessionmaker(bind=engine)
    return Session()

def get_last_weather_record(city):
    """Получить последнюю запись о погоде для города"""
    session = get_session()
    try:
        last_record = session.query(WeatherData)\
            .filter(WeatherData.city == city)\
            .order_by(WeatherData.created_at.desc())\
            .first()
        return last_record
    except Exception as e:
        logger.error(f"Error getting last record for {city}: {e}")
        return None
    finally:
        session.close()


def is_data_fresh(city, min_age_minutes=30):
    """
    Проверяет, есть ли свежие данные для города (младше min_age_minutes минут)
    
    Args:
        city: Название города
        min_age_minutes: Минимальный возраст данных в минутах (по умолчанию 30)
    
    Returns:
        tuple: (is_fresh: bool, last_record: WeatherData или None, age_minutes: float или None)
    """
    last_record = get_last_weather_record(city)
    
    if not last_record or not last_record.created_at:
        return False, None, None
    
    # Вычисляем возраст записи
    now = datetime.now(timezone.utc)
    if last_record.created_at.tzinfo is None:
        # Если created_at без timezone, считаем что это UTC
        last_time = last_record.created_at.replace(tzinfo=timezone.utc)
    else:
        last_time = last_record.created_at
    
    age_delta = now - last_time
    age_minutes = age_delta.total_seconds() / 60
    
    # Данные свежие, если им меньше min_age_minutes минут
    is_fresh = age_minutes < min_age_minutes
    
    return is_fresh, last_record, age_minutes


def save_weather_data(city, temperature, humidity, pressure, description, wind_speed=None, clouds=None, min_age_minutes=None, skip_if_fresh=True):
    """
    Сохранить данные погоды в БД с проверкой актуальности
    
    Args:
        city: Название города
        temperature: Температура
        humidity: Влажность
        pressure: Давление
        description: Описание погоды
        wind_speed: Скорость ветра (опционально)
        clouds: Облачность (опционально)
        min_age_minutes: Минимальный возраст данных в минутах для создания новой записи 
                        (по умолчанию из переменной окружения MIN_AGE_MINUTES или 30)
        skip_if_fresh: Пропускать сохранение если данные свежие (по умолчанию True)
    
    Returns:
        dict: {
            'saved': bool,           # Была ли создана новая запись
            'skipped': bool,         # Была ли пропущена запись из-за свежих данных
            'reason': str,           # Причина (saved, skipped_fresh, error)
            'age_minutes': float,    # Возраст последней записи (если пропущено)
            'message': str           # Сообщение для лога
        }
    """
    # Получаем min_age_minutes из переменной окружения или используем значение по умолчанию
    if min_age_minutes is None:
        min_age_minutes = int(os.getenv('MIN_AGE_MINUTES', '30'))
    
    session = get_session()
    try:
        # Проверяем актуальность данных если включена проверка
        if skip_if_fresh:
            is_fresh, last_record, age_minutes = is_data_fresh(city, min_age_minutes)
            
            if is_fresh:
                message = f"⏭️  Данные для {city} пропущены (последняя запись свежая, возраст: {age_minutes:.1f} мин, требуется: {min_age_minutes} мин)"
                print(message)
                return {
                    'saved': False,
                    'skipped': True,
                    'reason': 'skipped_fresh',
                    'age_minutes': age_minutes,
                    'message': message
                }
        
        # Создаём новую запись
        weather = WeatherData(
            city=city,
            temperature=round(temperature, 1),  # Округляем до 1 знака
            humidity=humidity,
            pressure=pressure,
            description=description,
            wind_speed=wind_speed,
            clouds=clouds
        )
        session.add(weather)
        session.commit()
        message = f"✅ Данные для {city} сохранены в БД"
        print(message)
        return {
            'saved': True,
            'skipped': False,
            'reason': 'saved',
            'age_minutes': None,
            'message': message
        }
    except Exception as e:
        session.rollback()
        message = f"❌ Ошибка при сохранении {city}: {e}"
        print(message)
        logger.error(f"Database error: {e}")
        return {
            'saved': False,
            'skipped': False,
            'reason': 'error',
            'age_minutes': None,
            'message': message
        }
    finally:
        session.close()

def get_recent_weather(limit=10):
    """Получить последние записи о погоде"""
    session = get_session()
    try:
        results = session.query(WeatherData)\
            .order_by(WeatherData.created_at.desc())\
            .limit(limit)\
            .all()
        return results
    except Exception as e:
        print(f"Ошибка при получении данных: {e}")
        return []
    finally:
        session.close()