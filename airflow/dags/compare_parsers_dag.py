from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import sys
import os
import time
import requests
import asyncio
import aiohttp

# Добавляем КОРЕНЬ Airflow (/opt/airflow), чтобы был доступен пакет shared
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

try:
    from shared.async_parser import fetch_all_cities_async
    print("✅ Модуль shared.async_parser успешно импортирован в compare_parsers_dag")
except ImportError as e:
    print(f"❌ Ошибка импорта shared.async_parser в compare_parsers_dag: {e}")
    print(f"🐍 sys.path: {sys.path}")
    shared_dir = os.path.join(BASE_DIR, "shared")
    if os.path.exists(shared_dir):
        print(f"📁 Содержимое {shared_dir}: {os.listdir(shared_dir)}")
    else:
        print(f"⚠️ Папка {shared_dir} не существует")
    raise

API_KEY = "78f695dd4093ed73ad14db84433d9e17"  # ← ПОДСТАВЬ!

def sync_parse_cities(cities):
    """Синхронный парсинг (старый подход)"""
    print(f"🐌 Начинаю синхронный парсинг {len(cities)} городов...")
    start = time.time()
    
    results = []
    for city in cities:
        try:
            response = requests.get(
                "http://api.openweathermap.org/data/2.5/weather",
                params={'q': city, 'appid': API_KEY, 'units': 'metric'},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                results.append({
                    'city': city,
                    'temp': data['main']['temp']
                })
                print(f"  ✅ {city}")
            else:
                print(f"  ❌ {city}: HTTP {response.status_code}")
        except Exception as e:
            print(f"  ❌ {city}: {e}")
    
    elapsed = time.time() - start
    print(f"🐌 Синхронный парсинг завершён за {elapsed:.2f} секунд")
    return {'time': elapsed, 'results': results}

def async_parse_cities(cities):
    """Асинхронный парсинг (новый подход)"""
    print(f"🚀 Начинаю асинхронный парсинг {len(cities)} городов...")
    start = time.time()
    
    # Используем функцию из async_parser
    results = asyncio.run(fetch_all_cities_async(cities))
    
    elapsed = time.time() - start
    print(f"🚀 Асинхронный парсинг завершён за {elapsed:.2f} секунд")
    return {'time': elapsed, 'results': results}

def compare_results(**context):
    """Сравнивает результаты двух подходов"""
    task_instance = context['task_instance']
    
    sync_result = task_instance.xcom_pull(task_ids='sync_parsing')
    async_result = task_instance.xcom_pull(task_ids='async_parsing')
    
    print("\n" + "="*50)
    print("📊 СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ")
    print("="*50)
    
    print(f"\n🐌 СИНХРОННЫЙ подход:")
    print(f"   Время: {sync_result['time']:.2f} секунд")
    print(f"   Успешно: {len(sync_result['results'])} городов")
    
    print(f"\n🚀 АСИНХРОННЫЙ подход:")
    print(f"   Время: {async_result['time']:.2f} секунд")
    print(f"   Успешно: {len(async_result['results'])} городов")
    
    if sync_result['time'] > 0:
        speedup = sync_result['time'] / async_result['time']
        print(f"\n⚡ Ускорение: в {speedup:.1f} раз!")
    
    print("\n💡 Вывод: Асинхронный подход эффективнее для I/O bound задач!")
    
    return {
        'sync_time': sync_result['time'],
        'async_time': async_result['time'],
        'speedup': speedup if sync_result['time'] > 0 else 0
    }

with DAG(
    'compare_parsers_dag',
    schedule_interval=None,  # Только ручной запуск
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'performance', 'async'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    test_cities = ["Moscow", "London", "Berlin", "Paris", "Tokyo"]
    
    sync_task = PythonOperator(
        task_id='sync_parsing',
        python_callable=sync_parse_cities,
        op_args=[test_cities],
    )
    
    async_task = PythonOperator(
        task_id='async_parsing',
        python_callable=async_parse_cities,
        op_args=[test_cities],
    )
    
    compare_task = PythonOperator(
        task_id='compare_performance',
        python_callable=compare_results,
        provide_context=True,
    )
    
    end = DummyOperator(task_id='end')
    
    # Запускаем оба подхода параллельно, потом сравниваем
    start >> [sync_task, async_task] >> compare_task >> end