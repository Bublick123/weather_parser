from celery import Celery
import sys
import os

# Добавляем shared в путь
sys.path.append('/app/shared')

from shared.database import save_weather_data

# Инициализация Celery
app = Celery(
    'weather_tasks',
    broker='redis://redis:6379/0',
    backend='redis://redis:6379/0'
)

# Конфигурация
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Europe/Moscow',
    enable_utc=True,
)

@app.task(name='save_weather_task')
def save_weather_task(weather_data):
    """Celery задача для сохранения данных в БД"""
    try:
        print(f"Получены данные для сохранения: {weather_data['city']}")
        
        success = save_weather_data(
            city=weather_data['city'],
            temperature=weather_data['temperature'],
            humidity=weather_data['humidity'],
            pressure=weather_data['pressure'],
            description=weather_data['description']
        )
        
        if success:
            print(f"✅ Данные для {weather_data['city']} успешно сохранены")
            return {'status': 'success', 'city': weather_data['city']}
        else:
            print(f"❌ Ошибка при сохранении данных для {weather_data['city']}")
            return {'status': 'error', 'city': weather_data['city']}
            
    except Exception as e:
        print(f"Ошибка в задаче Celery: {e}")
        return {'status': 'error', 'message': str(e)}

if __name__ == '__main__':
    app.start()