import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pickle
import os
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import mean_absolute_error
import logging

logger = logging.getLogger(__name__)

class WeatherForecaster:
    """
    ML модель для прогнозирования температуры на основе исторических данных
    """
    
    def __init__(self, model_path: str = "models/weather_forecast.pkl"):
        self.model_path = model_path
        self.model = None
        self.poly = PolynomialFeatures(degree=2)
        self.is_trained = False
        
    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Подготавливает фичи для модели:
        - день года (сезонность)
        - температура вчера
        - температура позавчера
        - скользящее среднее
        """
        df = df.copy()
        df['date'] = pd.to_datetime(df['created_at'])
        df = df.sort_values('date')
        
        # Основные фичи
        df['day_of_year'] = df['date'].dt.dayofyear
        df['day_of_year_sin'] = np.sin(2 * np.pi * df['day_of_year'] / 365)
        df['day_of_year_cos'] = np.cos(2 * np.pi * df['day_of_year'] / 365)
        
        # Лаги (предыдущие дни)
        df['temp_lag_1'] = df['temperature'].shift(1)  # Вчера
        df['temp_lag_2'] = df['temperature'].shift(2)  # Позавчера
        df['temp_lag_3'] = df['temperature'].shift(3)  # 3 дня назад
        
        # Скользящие средние
        df['temp_ma_3'] = df['temperature'].rolling(window=3).mean()
        df['temp_ma_7'] = df['temperature'].rolling(window=7).mean()
        
        # Разница с предыдущим днём
        df['temp_diff_1'] = df['temperature'].diff(1)
        
        # Удаляем строки с NaN
        df = df.dropna()
        
        return df
    
    def train(self, historical_data: pd.DataFrame, city: str):
        """
        Обучает модель на исторических данных для конкретного города
        """
        logger.info(f"🔄 Обучаю модель для города: {city}")
        
        # Фильтруем по городу
        city_data = historical_data[historical_data['city'] == city].copy()
        
        if len(city_data) < 10:
            logger.warning(f"⚠️  Недостаточно данных для {city}: {len(city_data)} записей")
            return False
        
        # Подготавливаем фичи
        df = self.prepare_features(city_data)
        
        if len(df) < 7:
            logger.warning(f"⚠️  Недостаточно данных после подготовки: {len(df)}")
            return False
        
        # Целевая переменная: температура завтра
        df['target'] = df['temperature'].shift(-1)
        df = df.dropna()
        
        if len(df) < 5:
            return False
        
        # Фичи и target
        feature_cols = [
            'day_of_year_sin', 'day_of_year_cos',
            'temp_lag_1', 'temp_lag_2', 'temp_lag_3',
            'temp_ma_3', 'temp_ma_7',
            'temp_diff_1'
        ]
        
        X = df[feature_cols].values
        y = df['target'].values
        
        # Полиномиальные фичи
        X_poly = self.poly.fit_transform(X)
        
        # Обучаем модель
        self.model = LinearRegression()
        self.model.fit(X_poly, y)
        
        # Оцениваем качество
        y_pred = self.model.predict(X_poly)
        mae = mean_absolute_error(y, y_pred)
        
        logger.info(f"✅ Модель обучена для {city}")
        logger.info(f"   MAE: {mae:.2f}°C")
        logger.info(f"   Точность: {self.model.score(X_poly, y):.2%}")
        
        self.is_trained = True
        return True
    
    def predict(self, city: str, recent_temps: List[float]) -> Optional[float]:
        """
        Предсказывает температуру на завтра
        """
        if not self.is_trained or not self.model:
            logger.error("❌ Модель не обучена!")
            return None
        
        if len(recent_temps) < 4:
            logger.error("❌ Недостаточно данных для предсказания")
            return None
        
        # Создаём фичи для предсказания
        today = datetime.now()
        day_of_year = today.timetuple().tm_yday
        
        # Вычисляем необходимые значения
        temp_lag_1 = recent_temps[-1]  # Сегодня
        temp_lag_2 = recent_temps[-2] if len(recent_temps) >= 2 else recent_temps[-1]
        temp_lag_3 = recent_temps[-3] if len(recent_temps) >= 3 else recent_temps[-1]
        
        # Скользящие средние
        temp_ma_3 = np.mean(recent_temps[-3:]) if len(recent_temps) >= 3 else np.mean(recent_temps)
        temp_ma_7 = np.mean(recent_temps[-7:]) if len(recent_temps) >= 7 else np.mean(recent_temps)
        
        # Разница
        temp_diff_1 = recent_temps[-1] - (recent_temps[-2] if len(recent_temps) >= 2 else recent_temps[-1])
        
        # Создаём вектор фичей
        features = np.array([[
            np.sin(2 * np.pi * day_of_year / 365),
            np.cos(2 * np.pi * day_of_year / 365),
            temp_lag_1,
            temp_lag_2,
            temp_lag_3,
            temp_ma_3,
            temp_ma_7,
            temp_diff_1
        ]])
        
        # Преобразуем в полиномиальные фичи
        features_poly = self.poly.transform(features)
        
        # Предсказываем
        prediction = self.model.predict(features_poly)[0]
        
        logger.info(f"🌡️  Прогноз для {city}: {prediction:.1f}°C")
        return round(prediction, 1)
    
    def save_model(self):
        """Сохраняет обученную модель"""
        if self.model:
            os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
            with open(self.model_path, 'wb') as f:
                pickle.dump({
                    'model': self.model,
                    'poly': self.poly,
                    'is_trained': self.is_trained
                }, f)
            logger.info(f"💾 Модель сохранена: {self.model_path}")
    
    def load_model(self):
        """Загружает обученную модель"""
        if os.path.exists(self.model_path):
            with open(self.model_path, 'rb') as f:
                data = pickle.load(f)
                self.model = data['model']
                self.poly = data['poly']
                self.is_trained = data['is_trained']
            logger.info(f"📂 Модель загружена: {self.model_path}")
            return True
        return False

def calculate_trend(temps: List[float]) -> Dict:
    """
    Анализирует тренд температуры
    """
    if len(temps) < 2:
        return {'direction': 'stable', 'change': 0}
    
    # Простая линейная регрессия для тренда
    x = np.arange(len(temps))
    y = np.array(temps)
    
    # Коэффициент наклона
    slope = np.polyfit(x, y, 1)[0]
    
    if slope > 0.5:
        direction = 'up'
    elif slope < -0.5:
        direction = 'down'
    else:
        direction = 'stable'
    
    change = temps[-1] - temps[0]
    
    return {
        'direction': direction,
        'change': round(change, 1),
        'slope': round(slope, 2)
    }

def analyze_weather_pattern(temps: List[float]) -> str:
    """
    Анализирует паттерны погоды
    """
    if len(temps) < 3:
        return "Недостаточно данных для анализа"
    
    # Проверяем на стабильность
    std_dev = np.std(temps)
    if std_dev < 2:
        return "Стабильная погода"
    elif std_dev < 5:
        return "Умеренная изменчивость"
    else:
        return "Высокая изменчивость"