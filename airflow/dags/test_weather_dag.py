from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Добавляем корень Airflow в sys.path, чтобы был доступен пакет shared
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

try:
    from shared.database import init_db, get_recent_weather
    print("✅ Модуль shared.database успешно импортирован")
except ImportError as e:
    print(f"❌ Ошибка импорта shared.database: {e}")
    print(f"🐍 sys.path: {sys.path}")
    shared_dir = os.path.join(BASE_DIR, "shared")
    if os.path.exists(shared_dir):
        print(f"📁 Содержимое {shared_dir}: {os.listdir(shared_dir)}")
    else:
        print(f"⚠️ Папка {shared_dir} не существует")
    raise

def test_database():
    """Тест подключения к БД"""
    try:
        print("🔍 Тестирую подключение к БД...")
        engine = init_db()
        print("✅ Подключение к БД успешно!")
        
        # Проверяем таблицу
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print(f"📋 Таблицы в БД: {tables}")
        
        if 'weather_data' in tables:
            print("✅ Таблица weather_data существует")
        else:
            print("❌ Таблица weather_data не найдена")
            
        return True
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return False

def show_recent_data():
    """Показать последние данные в БД"""
    print("📊 Последние записи в БД:")
    recent = get_recent_weather(10)
    
    if not recent:
        print("   БД пуста")
    else:
        for i, data in enumerate(recent, 1):
            print(f"   {i}. {data.created_at}: {data.city} - {data.temperature}°C, {data.description}")
        print(f"   Всего записей: {len(recent)}")
    return len(recent)

def check_shared_folder():
    """Проверить наличие shared папки"""
    print("🔍 Проверяю папку shared...")
    if os.path.exists('/opt/airflow/shared'):
        files = os.listdir('/opt/airflow/shared')
        print(f"✅ Папка shared существует")
        print(f"📁 Содержимое: {files}")
        
        # Проверяем database.py
        if 'database.py' in files:
            print("✅ Файл database.py найден")
            with open('/opt/airflow/shared/database.py', 'r') as f:
                first_line = f.readline().strip()
                print(f"   Первая строка: {first_line}")
        else:
            print("❌ Файл database.py не найден")
            
        return True
    else:
        print("❌ Папка shared не существует!")
        return False

def check_python_path():
    """Проверить Python path"""
    print("🐍 Python путь:")
    for path in sys.path:
        print(f"   {path}")
    return True

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'test_weather_system',
    default_args=default_args,
    description='Тест системы парсинга погоды',
    schedule_interval=None,  # Только ручной запуск
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    check_path = PythonOperator(
        task_id='check_python_path',
        python_callable=check_python_path,
    )
    
    check_folder = PythonOperator(
        task_id='check_shared_folder',
        python_callable=check_shared_folder,
    )
    
    test_db = PythonOperator(
        task_id='test_database_connection',
        python_callable=test_database,
    )
    
    show_data = PythonOperator(
        task_id='show_recent_data',
        python_callable=show_recent_data,
    )
    
    end = DummyOperator(task_id='end')
    
    start >> check_path >> check_folder >> test_db >> show_data >> end