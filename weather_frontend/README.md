# Weather Frontend (FastAPI + Jinja2)

Минимальный фронт для запуска DAG'ов и просмотра данных погоды.

## Фичи
- Кнопки запуска `weather_parser_dag` и `async_weather_parser_dag` через Airflow REST API.
- Таблица последних записей из Postgres (`weather_data`) с автообновлением каждые 10с.
- Мониторинг последних запусков DAG'ов: статусы, времена старта/финиша, длительность.
- Фильтр по городу, ручное обновление таблиц, оповещения об ошибках.

## Быстрый старт (локально)
```bash
cd weather_frontend
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp env.example .env   # при необходимости поправь значения
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```
Открой `http://localhost:8000`.

## Docker
По умолчанию сервис ожидает, что Airflow и Postgres доступны в сети `weather_parser_default`
и под хостами `airflow-webserver` и `postgres` соответственно. Если у тебя другая сеть/имена,
обнови `docker-compose.yml` и `env.example`.

```bash
cd weather_frontend
cp env.example .env  # подставь свои значения
docker compose up --build
```

## Переменные окружения
- `AIRFLOW_API_BASE` — базовый URL Airflow REST API, например `http://airflow-webserver:8080/api/v1`.
- `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` — учётка Airflow для Basic Auth.
- `DATABASE_URL` — строка подключения к Postgres (SQLAlchemy), напр. `postgresql+psycopg2://airflow:airflow@postgres/airflow`.
- `HOST` / `PORT` — хост и порт FastAPI (по умолчанию 0.0.0.0:8000).

## Страницы
- `/` — Главная, кнопки запуска DAG'ов.
- `/results` — Таблица последних результатов, автообновление 10с, фильтр по городу, кнопка «Обновить».
- `/monitor` — Статусы последних запусков DAG'ов, автообновление 10с, кнопка «Обновить».

## Замечания
- Если Airflow недоступен, кнопки/монитор вернут понятное сообщение об ошибке.
- Для корректной работы фронтенд контейнер должен быть в одной сети с Airflow/Postgres
  или используй `AIRFLOW_API_BASE`/`DATABASE_URL`, указывая доступные хосты (например, `localhost` при прямом доступе).***

