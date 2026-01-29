import aiohttp
import asyncio
from typing import List, Dict, Optional
import time
import os

# API ключ из переменных окружения
API_KEY = os.getenv("OPENWEATHER_API_KEY")

async def fetch_city_weather(session: aiohttp.ClientSession, city: str) -> Optional[Dict]:
    """
    Асинхронно получает погоду для одного города
    """
    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric',
        'lang': 'ru'
    }
    
    try:
        async with session.get(url, params=params, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                return {
                    'city': city,
                    'temperature': round(data['main']['temp'], 1),
                    'humidity': data['main']['humidity'],
                    'pressure': data['main']['pressure'],
                    'description': data['weather'][0]['description'],
                    'wind_speed': data['wind'].get('speed', 0),
                    'clouds': data['clouds'].get('all', 0),
                    'timestamp': time.time()
                }
            elif response.status == 401:
                print(f"❌ Неверный API ключ для города {city}")
                return None
            else:
                print(f"⚠️  Ошибка {response.status} для {city}: {await response.text()}")
                return None
                
    except asyncio.TimeoutError:
        print(f"⏰ Таймаут при запросе погоды для {city}")
        return None
    except Exception as e:
        print(f"❌ Ошибка при парсинге {city}: {e}")
        return None

async def fetch_all_cities_async(cities: List[str]) -> List[Dict]:
    """
    Асинхронно получает погоду для всех городов одновременно
    """
    print(f"🚀 Начинаю асинхронный парсинг {len(cities)} городов...")
    start_time = time.time()
    
    # Создаём сессию с общими настройками
    connector = aiohttp.TCPConnector(limit_per_host=10)  # Максимум 10 соединений на хост
    async with aiohttp.ClientSession(connector=connector) as session:
        # Создаём задачи для каждого города
        tasks = [fetch_city_weather(session, city) for city in cities]
        
        # Запускаем ВСЕ задачи одновременно
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Фильтруем успешные результаты
    successful_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"❌ Исключение для {cities[i]}: {result}")
        elif result:  # result не None и не Exception
            successful_results.append(result)
    
    elapsed = time.time() - start_time
    print(f"✅ Асинхронный парсинг завершён за {elapsed:.2f} секунд")
    print(f"📊 Успешно: {len(successful_results)} из {len(cities)} городов")
    
    return successful_results

def parse_all_cities_sync(cities: List[str]) -> List[Dict]:
    """
    Синхронная обёртка для асинхронной функции
    (нужна для Airflow PythonOperator)
    """
    return asyncio.run(fetch_all_cities_async(cities))

# Функция для тестирования
async def test_async_parser():
    """Тест асинхронного парсера"""
    cities = ["Moscow", "London", "Berlin", "Paris", "Tokyo", "New York", "Beijing"]
    results = await fetch_all_cities_async(cities)
    
    print("\n📊 Результаты:")
    for result in results:
        if result:
            print(f"  {result['city']:15} {result['temperature']:5}°C {result['description']:20}")
    
    return results

if __name__ == "__main__":
    # Запуск теста
    print("🧪 Тестирую асинхронный парсер...")
    asyncio.run(test_async_parser())