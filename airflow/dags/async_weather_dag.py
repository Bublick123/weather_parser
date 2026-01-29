from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import sys
import os

# Добавляем КОРЕНЬ Airflow (/opt/airflow), чтобы был доступен пакет shared
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

try:
    from shared.database import save_weather_data, init_db
    from shared.async_parser import parse_all_cities_sync
    print("✅ Модули shared успешно импортированы в async_weather_dag")
except ImportError as e:
    print(f"❌ Ошибка импорта модулей shared в async_weather_dag: {e}")
    print(f"🐍 sys.path: {sys.path}")
    shared_dir = os.path.join(BASE_DIR, "shared")
    if os.path.exists(shared_dir):
        print(f"📁 Содержимое {shared_dir}: {os.listdir(shared_dir)}")
    else:
        print(f"⚠️ Папка {shared_dir} не существует")
    raise

# Города для парсинга (можно больше!)
CITIES = [
    "Chelyabinsk,ru",
    "Izhevsk,ru",
    "Moscow,ru",
    "Saint Petersburg,ru",
    "Krasnodar,ru",
    "Groningen,nl",
]

def save_results_to_db(**context):
    """
    Сохраняет результаты асинхронного парсинга в БД
    """
    task_instance = context['task_instance']
    weather_data_list = task_instance.xcom_pull(task_ids='async_parse_all_cities')
    
    if not weather_data_list:
        print("❌ Нет данных для сохранения")
        return 0
    
    saved_count = 0
    skipped_count = 0
    for weather_data in weather_data_list:
        if weather_data:  # Пропускаем None
            try:
                result = save_weather_data(
                    city=weather_data['city'],
                    temperature=weather_data['temperature'],
                    humidity=weather_data['humidity'],
                    pressure=weather_data['pressure'],
                    description=weather_data['description'],
                    wind_speed=weather_data.get('wind_speed'),
                    clouds=weather_data.get('clouds'),
                    min_age_minutes=30,  # Минимум 30 минут между записями для одного города
                    skip_if_fresh=True
                )
                if result['saved']:
                    saved_count += 1
                elif result['skipped']:
                    skipped_count += 1
                    print(result['message'])  # Уже содержит эмодзи и информацию
            except Exception as e:
                print(f"❌ Ошибка сохранения {weather_data.get('city', 'unknown')}: {e}")
    
    print(f"📊 Результат: сохранено {saved_count}, пропущено (свежие данные) {skipped_count} из {len(weather_data_list)} городов")
    return saved_count

def compare_performance(**context):
    """
    Сравнивает производительность синхронного и асинхронного подходов
    """
    import time
    import requests
    
    # Тест синхронного подхода (просто для сравнения)
    print("🧪 Тестирую синхронный подход...")
    cities = ["Moscow", "London", "Berlin"]
    
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    if not API_KEY:
        print("❌ API Key не найден!")
        return 0
        
    start = time.time()
    for city in cities:
        try:
            response = requests.get(
                "http://api.openweathermap.org/data/2.5/weather",
                params={'q': city, 'appid': API_KEY, 'units': 'metric'},
                timeout=10
            )
            if response.status_code == 200:
                print(f"  {city}: OK")
        except:
            pass
    sync_time = time.time() - start
    
    print(f"\n⏱️  Синхронное выполнение 3 городов: {sync_time:.2f} секунд")
    print("💡 Примечание: Асинхронный подход будет значительно быстрее!")
    
    return sync_time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
       
}


with DAG(
    'async_weather_parser_dag',
    default_args=default_args,
    description='Асинхронный парсинг погоды с использованием asyncio',
    schedule_interval='0 12 * * *',  # Каждый день в 12:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['weather', 'async', 'performance'],
    max_active_runs=1,
) as dag:
    
    start = DummyOperator(task_id='start')
    
    # Инициализация БД
    init_db_task = PythonOperator(
        task_id='initialize_database',
        python_callable=lambda: init_db() and True,
    )
    
    # Асинхронный парсинг ВСЕХ городов в ОДНОЙ задаче
    async_parse_task = PythonOperator(
        task_id='async_parse_all_cities',
        python_callable=parse_all_cities_sync,
        op_args=[CITIES],
    )
    
    # Сохранение результатов
    save_task = PythonOperator(
        task_id='save_async_results',
        python_callable=save_results_to_db,
    )
    
    # Тест производительности (опционально)
    perf_test_task = PythonOperator(
        task_id='performance_comparison',
        python_callable=compare_performance,
    )
    
    end = DummyOperator(task_id='end')
    
    # Оркестрация
    start >> init_db_task >> async_parse_task >> save_task >> perf_test_task >> end