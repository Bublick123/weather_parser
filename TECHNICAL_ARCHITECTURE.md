# Техническая архитектура Weather Parser System

## 📋 Обзор системы

Система представляет собой распределённое приложение для парсинга погодных данных с использованием Apache Airflow для оркестрации задач, Celery для распределённой обработки, PostgreSQL для хранения данных и FastAPI для веб-интерфейса.

---

## 🏗️ Архитектура компонентов

### 1. **Docker Compose - Оркестрация контейнеров**

```yaml
services:
  - postgres          # База данных
  - redis             # Брокер сообщений
  - airflow-init      # Инициализация Airflow
  - airflow-webserver # Web UI Airflow
  - airflow-scheduler # Планировщик задач
  - airflow-worker    # Исполнитель задач (Celery)
  - celery-worker-1   # Дополнительный Celery воркер
  - weather-frontend  # FastAPI фронтенд
```

**Технические детали:**
- Все сервисы в одной Docker сети (`weather_parser_default`)
- Используются healthchecks для контроля готовности сервисов
- Volumes для персистентности данных (PostgreSQL) и логов
- Зависимости через `depends_on` с условиями `service_healthy`

---

## 🔄 Поток данных и взаимодействие компонентов

### **Сценарий 1: Запуск DAG через фронтенд**

```
[Пользователь] 
    ↓ HTTP POST /trigger/{dag_id}
[FastAPI Frontend (port 8002)]
    ↓ HTTP POST с Basic Auth (admin/admin)
[Airflow REST API (port 8001)]
    ↓ Создание DAG Run в PostgreSQL
[Airflow Scheduler]
    ↓ Читает метаданные из PostgreSQL
    ↓ Публикует задачи в Redis (Celery broker)
[Airflow Worker / Celery Worker]
    ↓ Подписывается на задачи из Redis
    ↓ Выполняет Python функции из DAG
    ↓ Запрашивает данные через HTTP
[OpenWeatherMap API]
    ↓ Возвращает JSON с погодой
[Python функция в DAG]
    ↓ Обрабатывает данные
    ↓ Вызывает shared.database.save_weather_data()
[SQLAlchemy ORM]
    ↓ INSERT INTO weather_data
[PostgreSQL]
    ↓ Сохраняет данные
[Airflow Worker]
    ↓ Обновляет статус задачи в PostgreSQL
[Airflow Scheduler]
    ↓ Отмечает задачу как completed
[Frontend /api/monitor]
    ↓ Читает статусы из PostgreSQL через Airflow API
[Пользователь видит результат]
```

---

## 🗄️ База данных PostgreSQL

### **Схема Airflow (метаданные)**

Airflow использует PostgreSQL для хранения:
- `dag` - определения DAG'ов
- `dag_run` - запуски DAG'ов
- `task_instance` - экземпляры задач
- `log` - логи выполнения
- `connection` - подключения к внешним системам
- `variable` - переменные конфигурации

### **Схема приложения (weather_data)**

```sql
CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    temperature FLOAT NOT NULL,
    humidity INTEGER,
    pressure INTEGER,
    description VARCHAR(200),
    wind_speed FLOAT,
    clouds INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

**Технические детали:**
- SQLAlchemy ORM для работы с БД
- Модель `WeatherData` наследуется от `declarative_base()`
- Автоматическое создание таблицы через `Base.metadata.create_all()`
- Connection pooling через SQLAlchemy engine

---

## 🔀 Redis - Message Broker

### **Роль Redis в системе:**

1. **Celery Broker** (`redis://redis:6379/0`)
   - Очередь задач для Airflow Worker
   - Формат: JSON сообщения с метаданными задачи

2. **Celery Result Backend** (`redis://redis:6379/0`)
   - Хранение результатов выполнения задач
   - TTL для автоматической очистки

**Технические детали:**
- Redis используется как in-memory хранилище
- Протокол: Redis Protocol (RESP)
- Сериализация: JSON
- Паттерн: Pub/Sub для распределения задач

---

## ✈️ Apache Airflow - Оркестрация

### **Компоненты Airflow:**

#### **1. Airflow Webserver (port 8001)**
- **Технология:** Flask + Gunicorn
- **Функции:**
  - Web UI для управления DAG'ами
  - REST API (`/api/v1/*`)
  - Аутентификация через Basic Auth
- **Порт:** 8001 (host) → 8080 (container)

#### **2. Airflow Scheduler**
- **Технология:** Python daemon процесс
- **Функции:**
  - Парсит DAG файлы из `/opt/airflow/dags`
  - Планирует выполнение задач по расписанию
  - Публикует задачи в Redis через Celery
  - Отслеживает статусы выполнения
- **Цикл работы:**
  ```
  while True:
      1. Парсит DAG файлы (каждые 30 сек)
      2. Проверяет расписание (schedule_interval)
      3. Создаёт DAG Run в PostgreSQL
      4. Публикует задачи в Redis
      5. Ждёт завершения задач
      6. Обновляет статусы в PostgreSQL
  ```

#### **3. Airflow Worker**
- **Технология:** Celery Worker
- **Функции:**
  - Подписывается на задачи из Redis
  - Выполняет Python функции из DAG
  - Обновляет статусы в PostgreSQL
- **Команда:** `celery worker`

#### **4. Executor: CeleryExecutor**
- **Принцип работы:**
  - Scheduler не выполняет задачи напрямую
  - Задачи отправляются в Redis очередь
  - Worker'ы забирают задачи и выполняют
  - Результаты возвращаются через Redis

---

## 📝 DAG (Directed Acyclic Graph) - Определение задач

### **Структура DAG файла:**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'weather_parser_dag',
    schedule_interval='0 12 * * *',  # Каждый день в 12:00
    start_date=datetime(2024, 1, 1),
) as dag:
    
    task1 = PythonOperator(
        task_id='parse_city',
        python_callable=parse_weather,
        op_args=['Moscow']
    )
    
    task2 = PythonOperator(
        task_id='save_data',
        python_callable=save_to_db,
    )
    
    task1 >> task2  # Зависимость: task1 выполняется перед task2
```

### **Типы операторов:**

1. **PythonOperator** - выполняет Python функцию
2. **DummyOperator** - заглушка для логической группировки
3. **BashOperator** - выполняет bash команду

### **Жизненный цикл задачи:**

```
queued → scheduled → running → success/failed
```

---

## 🌐 FastAPI Frontend - Веб-интерфейс

### **Архитектура:**

```
[Browser]
    ↓ HTTP GET/POST
[Uvicorn ASGI Server]
    ↓
[FastAPI Application]
    ↓
[Route Handlers]
    ├── HTML Templates (Jinja2)
    ├── REST API Endpoints
    └── Static Files (CSS)
```

### **Компоненты:**

#### **1. ASGI Server: Uvicorn**
- **Порт:** 8000 (container) → 8002 (host)
- **Протокол:** HTTP/1.1, HTTP/2
- **Многопоточность:** AsyncIO event loop

#### **2. FastAPI Application**
- **Технология:** FastAPI framework
- **Роутинг:**
  ```python
  @app.get("/")              # Главная страница
  @app.get("/results")        # Страница результатов
  @app.get("/monitor")        # Страница мониторинга
  @app.post("/trigger/{dag_id}")  # API запуска DAG
  @app.get("/api/results")    # API получения данных
  @app.get("/api/monitor")    # API статусов DAG'ов
  ```

#### **3. Интеграция с Airflow REST API**

```python
async def airflow_request(method, path, payload):
    url = f"{AIRFLOW_API_BASE}/{path}"
    auth = aiohttp.BasicAuth(USERNAME, PASSWORD)
    
    async with aiohttp.ClientSession(auth=auth) as session:
        async with session.request(method, url, json=payload) as resp:
            return await resp.json()
```

**Эндпоинты Airflow API:**
- `POST /api/v1/dags/{dag_id}/dagRuns` - запуск DAG
- `GET /api/v1/dags/{dag_id}/dagRuns` - получение запусков

#### **4. Работа с PostgreSQL**

```python
from sqlalchemy import create_engine, text

engine = create_engine(DATABASE_URL)
db = SessionLocal()

# Прямой SQL запрос
rows = db.execute(text("SELECT * FROM weather_data")).fetchall()
```

**Технические детали:**
- Connection pooling через SQLAlchemy
- Синхронные запросы (можно улучшить через asyncpg)
- Dependency Injection через FastAPI Depends

---

## 📦 Shared Module - Общий код

### **Структура:**

```
shared/
├── __init__.py
├── database.py      # SQLAlchemy модели и функции БД
├── models.py        # Дополнительные модели
└── async_parser.py  # Асинхронный парсинг
```

### **Импорт в DAG:**

```python
# Добавляем корень Airflow в sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(BASE_DIR)

# Теперь можем импортировать
from shared.database import save_weather_data
```

**Технические детали:**
- Монтируется как volume в контейнеры Airflow
- Путь: `./shared:/opt/airflow/shared`
- Python видит его как пакет через sys.path

---

## 🔌 OpenWeatherMap API - Внешний сервис

### **Запрос:**

```python
url = "http://api.openweathermap.org/data/2.5/weather"
params = {
    'q': 'Moscow',
    'appid': API_KEY,
    'units': 'metric',
    'lang': 'ru'
}
response = requests.get(url, params=params)
data = response.json()
```

### **Ответ:**

```json
{
  "main": {
    "temp": 15.5,
    "humidity": 65,
    "pressure": 1013
  },
  "weather": [{
    "description": "ясно"
  }],
  "wind": {
    "speed": 3.2
  },
  "clouds": {
    "all": 20
  }
}
```

---

## 🔄 Полный цикл выполнения задачи

### **Пример: weather_parser_dag**

```
1. [Scheduler] Парсит weather_dag.py
   ↓
2. [Scheduler] Видит schedule_interval='0 12 * * *'
   ↓
3. [Scheduler] Создаёт DAG Run в PostgreSQL
   ↓
4. [Scheduler] Публикует task 'parse_moscow' в Redis
   ↓
5. [Worker] Забирает задачу из Redis
   ↓
6. [Worker] Выполняет parse_weather('Moscow')
   ↓
7. [Worker] Делает HTTP запрос к OpenWeatherMap
   ↓
8. [OpenWeatherMap] Возвращает JSON
   ↓
9. [Worker] Обрабатывает данные
   ↓
10. [Worker] Вызывает save_weather_data()
    ↓
11. [SQLAlchemy] INSERT INTO weather_data
    ↓
12. [PostgreSQL] Сохраняет запись
    ↓
13. [Worker] Обновляет task_instance.status = 'success'
    ↓
14. [Scheduler] Видит успешное завершение
    ↓
15. [Scheduler] Публикует следующую задачу 'save_to_db'
    ↓
16. [Worker] Выполняет save_to_db()
    ↓
17. [Worker] Завершает DAG Run
```

---

## 🔐 Аутентификация и безопасность

### **Airflow REST API:**
- **Метод:** Basic Auth
- **Конфигурация:** `AIRFLOW__API__AUTH_BACKENDS: airflow.api.auth.backend.basic_auth`
- **Учётные данные:** admin/admin (из airflow-init)

### **PostgreSQL:**
- **Пользователь:** airflow
- **Пароль:** airflow
- **База:** airflow
- **Подключение:** `postgresql+psycopg2://airflow:airflow@postgres/airflow`

### **Redis:**
- **Аутентификация:** Отсутствует (внутренняя сеть Docker)
- **База:** 0 (по умолчанию)

---

## 📊 Мониторинг и логирование

### **Логи Airflow:**
- **Расположение:** `./logs/`
- **Структура:** `logs/dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={attempt}/task.log`
- **Формат:** Текстовые файлы с stdout/stderr задач

### **Мониторинг через фронтенд:**
- **Эндпоинт:** `/api/monitor`
- **Данные:** Статусы DAG Runs из PostgreSQL через Airflow API
- **Обновление:** Каждые 10 секунд (setInterval)

---

## 🚀 Производительность и масштабирование

### **Текущая конфигурация:**

1. **Airflow Worker:** 1 контейнер
2. **Celery Worker:** 1 контейнер (concurrency=3)
3. **PostgreSQL:** 1 инстанс (без репликации)
4. **Redis:** 1 инстанс (без кластеризации)

### **Возможности масштабирования:**

1. **Горизонтальное масштабирование:**
   - Добавить больше Airflow Workers
   - Добавить больше Celery Workers
   - Использовать Redis Cluster

2. **Вертикальное масштабирование:**
   - Увеличить concurrency в Celery
   - Настроить connection pool в PostgreSQL
   - Увеличить memory limits контейнеров

3. **Оптимизация:**
   - Использовать async/await в DAG задачах
   - Кэширование результатов в Redis
   - Batch обработка данных

---

## 🐛 Отладка и troubleshooting

### **Проверка статусов:**

```bash
# Статус контейнеров
docker-compose ps

# Логи Airflow Scheduler
docker-compose logs airflow-scheduler

# Логи Airflow Worker
docker-compose logs airflow-worker

# Логи фронтенда
docker-compose logs weather-frontend

# Проверка Redis очереди
docker-compose exec redis redis-cli LLEN celery

# Проверка PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM dag_run;"
```

### **Типичные проблемы:**

1. **Задачи висят в queued:**
   - Проверить, что airflow-worker запущен
   - Проверить подключение к Redis
   - Проверить логи worker'а

2. **Ошибки импорта shared:**
   - Проверить, что volume смонтирован
   - Проверить sys.path в DAG файле
   - Проверить наличие __init__.py

3. **Ошибки подключения к БД:**
   - Проверить DATABASE_URL
   - Проверить, что PostgreSQL запущен
   - Проверить network connectivity

---

## 📚 Технологический стек

| Компонент | Технология | Версия |
|-----------|------------|--------|
| Оркестрация | Docker Compose | 3.8 |
| База данных | PostgreSQL | 13 |
| Message Broker | Redis | 7-alpine |
| Workflow Engine | Apache Airflow | 2.7.0 |
| Task Queue | Celery | (встроен в Airflow) |
| Web Framework | FastAPI | 0.115.0 |
| ASGI Server | Uvicorn | 0.30.1 |
| Template Engine | Jinja2 | 3.1.4 |
| ORM | SQLAlchemy | 2.0.32 |
| HTTP Client | aiohttp | 3.10.5 |
| Python | Python | 3.9 / 3.11 |

---

## 🎯 Ключевые архитектурные решения

1. **Разделение ответственности:**
   - Airflow для оркестрации
   - Celery для выполнения задач
   - FastAPI для пользовательского интерфейса
   - PostgreSQL для персистентности

2. **Микросервисная архитектура:**
   - Каждый компонент в отдельном контейнере
   - Слабая связанность через очереди и API
   - Легко масштабировать отдельные компоненты

3. **Идемпотентность:**
   - Задачи можно перезапускать безопасно
   - Проверка существования таблиц перед созданием
   - Транзакции в БД для атомарности

4. **Отказоустойчивость:**
   - Healthchecks для контроля состояния
   - Автоматический restart при падении
   - Логирование для диагностики

---

Это полное техническое описание архитектуры системы. Если нужны детали по какому-то конкретному компоненту - спрашивай!

