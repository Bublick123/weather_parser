import asyncio
import logging
from typing import Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)

class NotificationManager:
    """Управляет уведомлениями для подписчиков"""
    
    def __init__(self, bot_token: str):
        self.bot_token = bot_token
        self.subscribers: Dict[int, bool] = {}  # chat_id -> is_active
        
    async def send_notification(self, chat_id: int, message: str):
        """Отправляет уведомление конкретному пользователю"""
        try:
            import requests
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            data = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML"
            }
            response = requests.post(url, json=data, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления: {e}")
            return False
    
    async def notify_all(self, message: str):
        """Уведомляет всех активных подписчиков"""
        for chat_id, is_active in self.subscribers.items():
            if is_active:
                await self.send_notification(chat_id, message)
    
    def on_dag_success(self, dag_id: str, execution_date: datetime, **kwargs):
        """Callback при успешном выполнении DAG"""
        message = f"""
✅ <b>DAG выполнен успешно!</b>

📊 <b>{dag_id}</b>
📅 Время: {execution_date.strftime('%Y-%m-%d %H:%M:%S')}
🎯 Статус: Успех

Подробности в Airflow UI.
        """
        asyncio.create_task(self.notify_all(message))
    
    def on_dag_failure(self, dag_id: str, execution_date: datetime, **kwargs):
        """Callback при ошибке выполнения DAG"""
        message = f"""
❌ <b>DAG завершился с ошибкой!</b>

📊 <b>{dag_id}</b>
📅 Время: {execution_date.strftime('%Y-%m-%d %H:%M:%S')}
🎯 Статус: Ошибка

Проверьте логи в Airflow UI.
        """
        asyncio.create_task(self.notify_all(message))
    
    def on_temperature_alert(self, city: str, temp: float, threshold: float = 0):
        """Уведомление об аномальной температуре"""
        if temp < threshold:
            message = f"""
⚠️ <b>Аномальная температура!</b>

🌆 Город: <b>{city}</b>
🌡️ Температура: <b>{temp}°C</b>
📉 Ниже порога: {threshold}°C

Будьте осторожны!
            """
            asyncio.create_task(self.notify_all(message))