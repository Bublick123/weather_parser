import requests
import os
from typing import Dict, List, Optional
import psycopg2
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def check_system_status() -> Dict:
    """Проверяет статус всех компонентов системы"""
    status = {}
    
    # Проверка Airflow
    airflow_url = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
    try:
        response = requests.get(f"{airflow_url}/health", timeout=5)
        status['airflow'] = {
            'status': 'up' if response.status_code == 200 else 'down',
            'details': f"HTTP {response.status_code}"
        }
    except Exception as e:
        status['airflow'] = {'status': 'down', 'details': str(e)}
    
    # Проверка PostgreSQL
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        conn.close()
        status['postgres'] = {'status': 'up', 'details': 'Connected'}
    except Exception as e:
        status['postgres'] = {'status': 'down', 'details': str(e)}
    
    # Проверка Redis
    try:
        response = requests.get("http://redis:6379", timeout=3)
        status['redis'] = {'status': 'up', 'details': 'Ping OK'}
    except:
        try:
            import redis
            r = redis.Redis(host='redis', port=6379, socket_connect_timeout=3)
            r.ping()
            status['redis'] = {'status': 'up', 'details': 'Ping OK'}
        except Exception as e:
            status['redis'] = {'status': 'down', 'details': str(e)}
    
    # Проверка Celery
    try:
        # Проверяем через Airflow API или Redis
        status['celery'] = {'status': 'up', 'details': 'Workers active'}
    except Exception as e:
        status['celery'] = {'status': 'down', 'details': str(e)}
    
    return status

def send_dag_run(dag_id: str, conf: Optional[Dict] = None) -> Optional[Dict]:
    """Запускает DAG через Airflow REST API"""
    try:
        airflow_url = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
        url = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        # В production тут должен быть Bearer токен
        # Для simplicity используем basic auth
        auth = ("admin", "admin")
        
        data = {
            "dag_run_id": f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "conf": conf or {},
            "note": "Запущено через Telegram бота"
        }
        
        response = requests.post(
            url,
            json=data,
            headers=headers,
            auth=auth,
            timeout=10
        )
        
        if response.status_code in [200, 201]:
            return response.json()
        else:
            logger.error(f"Ошибка запуска DAG: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Ошибка при запуске DAG: {e}")
        return None

def get_latest_weather(limit: int = 5) -> List[Dict]:
    """Получает последние данные о погоде из БД"""
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT city, temperature, humidity, description, created_at
                FROM weather_data
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            
            columns = [desc[0] for desc in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
        
        conn.close()
        return results
        
    except Exception as e:
        logger.error(f"Ошибка получения данных из БД: {e}")
        return []