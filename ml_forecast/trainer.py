import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from typing import Dict
import logging
from forecast import WeatherForecaster

logger = logging.getLogger(__name__)

def fetch_historical_data(days: int = 30) -> pd.DataFrame:
    """
    Загружает исторические данные из PostgreSQL
    """
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        
        query = f"""
        SELECT city, temperature, created_at
        FROM weather_data
        WHERE created_at >= NOW() - INTERVAL '{days} days'
        ORDER BY city, created_at
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        logger.info(f"📥 Загружено {len(df)} записей за последние {days} дней")
        return df
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки данных: {e}")
        return pd.DataFrame()

def train_all_models():
    """
    Обучает модели для всех городов
    """
    # Загружаем данные
    df = fetch_historical_data(days=60)
    
    if df.empty:
        logger.error("❌ Нет данных для обучения")
        return False
    
    # Получаем уникальные города
    cities = df['city'].unique()
    
    results = {}
    
    for city in cities:
        logger.info(f"🧠 Обучаю модель для: {city}")
        
        forecaster = WeatherForecaster(model_path=f"models/{city}_model.pkl")
        
        if forecaster.train(df, city):
            forecaster.save_model()
            results[city] = "✅ Успешно"
        else:
            results[city] = "❌ Ошибка"
    
    # Сводка
    logger.info("\n📊 Сводка обучения:")
    for city, status in results.items():
        logger.info(f"  {city}: {status}")
    
    return True

def get_forecast_for_city(city: str, days_back: int = 7) -> Dict:
    """
    Получает прогноз для конкретного города
    """
    try:
        # Загружаем последние данные города
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        
        query = f"""
        SELECT temperature, created_at
        FROM weather_data
        WHERE city = %s
        ORDER BY created_at DESC
        LIMIT %s
        """
        
        with conn.cursor() as cursor:
            cursor.execute(query, (city, days_back))
            rows = cursor.fetchall()
        
        conn.close()
        
        if not rows:
            return {"error": f"Нет данных для города {city}"}
        
        # Извлекаем температуры
        temps = [row[0] for row in rows][::-1]  # Реверс для хронологического порядка
        
        # Загружаем модель
        forecaster = WeatherForecaster(model_path=f"models/{city}_model.pkl")
        
        if not forecaster.load_model():
            # Если модель не существует, обучаем
            df = fetch_historical_data(days=30)
            if not df.empty:
                forecaster.train(df, city)
                forecaster.save_model()
        
        # Получаем прогноз
        prediction = forecaster.predict(city, temps)
        
        if prediction:
            from forecast import calculate_trend, analyze_weather_pattern
            
            trend = calculate_trend(temps)
            pattern = analyze_weather_pattern(temps)
            
            return {
                'city': city,
                'prediction': prediction,
                'current_temp': temps[-1],
                'trend': trend,
                'pattern': pattern,
                'historical_temps': temps,
                'timestamp': datetime.now().isoformat()
            }
        else:
            return {"error": "Не удалось получить прогноз"}
            
    except Exception as e:
        logger.error(f"❌ Ошибка получения прогноза: {e}")
        return {"error": str(e)}

if __name__ == "__main__":
    # Тестируем
    logging.basicConfig(level=logging.INFO)
    print("🧪 Тестирую ML прогноз...")
    
    # Обучаем модели
    success = train_all_models()
    
    if success:
        # Получаем прогноз для Москвы
        forecast = get_forecast_for_city("Moscow")
        print(f"\n📊 Прогноз для Москвы:")
        print(f"   Текущая: {forecast.get('current_temp', 'N/A')}°C")
        print(f"   Завтра: {forecast.get('prediction', 'N/A')}°C")
        print(f"   Тренд: {forecast.get('trend', {}).get('direction', 'N/A')}")