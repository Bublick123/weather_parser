import asyncio
import logging
import os
from datetime import datetime
from typing import AsyncGenerator, Dict, List, Optional

import aiohttp
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AIRFLOW_API_BASE = os.getenv("AIRFLOW_API_BASE", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://airflow:airflow@postgres/airflow")

# Разрешённые DAG'и для кнопок
ALLOWED_DAGS = [
    "async_weather_parser_dag",
]

# Настройка БД
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

app = FastAPI(title="Weather Frontend", docs_url=None, redoc_url=None)
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


def parse_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        # Airflow возвращает ISO8601, иногда с Z на конце
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


async def airflow_request(method: str, path: str, payload: Optional[dict] = None) -> dict:
    """
    Унифицированный запрос к Airflow REST API с Basic Auth авторизацией.
    """
    url = f"{AIRFLOW_API_BASE.rstrip('/')}/{path.lstrip('/')}"
    timeout = aiohttp.ClientTimeout(total=15)
    auth = aiohttp.BasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)

    async with aiohttp.ClientSession(timeout=timeout, auth=auth) as session:
        try:
            async with session.request(method, url, json=payload) as resp:
                text_body = await resp.text()
                if resp.status >= 400:
                    logger.error("Airflow API error %s: %s", resp.status, text_body)
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=f"Airflow API error {resp.status}: {text_body}",
                    )
                try:
                    return await resp.json()
                except Exception:
                    return {"raw": text_body}
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Airflow API timeout",
            )
        except aiohttp.ClientError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Airflow API unavailable: {exc}",
            )


async def trigger_dag(dag_id: str) -> dict:
    return await airflow_request("POST", f"dags/{dag_id}/dagRuns", payload={"conf": {}})


async def fetch_dag_runs(dag_id: str, limit: int = 5) -> dict:
    return await airflow_request(
        "GET",
        f"dags/{dag_id}/dagRuns?limit={limit}&order_by=-start_date",
    )


def get_db() -> AsyncGenerator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "dag_ids": ALLOWED_DAGS,
        },
    )


@app.get("/results")
async def results_page(request: Request):
    return templates.TemplateResponse("results.html", {"request": request})


@app.get("/monitor")
async def monitor_page(request: Request):
    return templates.TemplateResponse("monitor.html", {"request": request, "dag_ids": ALLOWED_DAGS})


@app.post("/trigger/{dag_id}")
async def trigger(dag_id: str):
    if dag_id not in ALLOWED_DAGS:
        raise HTTPException(status_code=404, detail="Unknown DAG")
    data = await trigger_dag(dag_id)
    return {"status": "ok", "dag_id": dag_id, "response": data}


@app.get("/api/results")
async def api_results(
    city: Optional[str] = None,
    limit: int = 50,
    db=Depends(get_db),
):
    """
    Возвращает последние записи из weather_data с фильтром по городу.
    """
    limit = max(1, min(limit, 200))

    base_query = """
        SELECT city, temperature, humidity, pressure, description, wind_speed, clouds, created_at
        FROM weather_data
    """
    where_clause = ""
    params: Dict[str, object] = {"limit": limit}

    if city:
        where_clause = "WHERE LOWER(city) LIKE LOWER(:city_pattern)"
        params["city_pattern"] = f"%{city}%"

    query = f"""
        {base_query}
        {where_clause}
        ORDER BY created_at DESC
        LIMIT :limit
    """

    rows = db.execute(text(query), params).fetchall()
    mapped = [
        {
            "city": row.city,
            "temperature": row.temperature,
            "humidity": row.humidity,
            "pressure": row.pressure,
            "description": row.description,
            "wind_speed": row.wind_speed,
            "clouds": row.clouds,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
        for row in rows
    ]

    stats = db.execute(
        text(
            """
            SELECT COUNT(*) AS total, COUNT(DISTINCT city) AS unique_cities
            FROM weather_data
            """
        )
    ).first()

    return {
        "items": mapped,
        "count": len(mapped),
        "total_records": stats.total if stats else 0,
        "unique_cities": stats.unique_cities if stats else 0,
    }


@app.get("/api/monitor")
async def api_monitor(db=Depends(get_db)):
    """
    Статусы последних запусков DAG'ов + короткие метрики по данным.
    """
    tasks = [fetch_dag_runs(dag_id) for dag_id in ALLOWED_DAGS]
    try:
        dag_runs_list = await asyncio.gather(*tasks)
    except HTTPException as exc:
        return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})

    monitor: List[dict] = []
    for dag_id, dag_payload in zip(ALLOWED_DAGS, dag_runs_list):
        dag_runs = dag_payload.get("dag_runs", []) if isinstance(dag_payload, dict) else []
        formatted_runs = []
        for run in dag_runs:
            start = parse_dt(run.get("start_date"))
            end = parse_dt(run.get("end_date"))
            duration = (end - start).total_seconds() if start and end else None
            formatted_runs.append(
                {
                    "run_id": run.get("dag_run_id") or run.get("run_id"),
                    "state": run.get("state"),
                    "start_date": run.get("start_date"),
                    "end_date": run.get("end_date"),
                    "duration_seconds": duration,
                    "external_trigger": run.get("external_trigger"),
                }
            )
        monitor.append(
            {
                "dag_id": dag_id,
                "runs": formatted_runs,
                "latest_state": formatted_runs[0]["state"] if formatted_runs else "unknown",
            }
        )

    counts = db.execute(
        text(
            """
            SELECT COUNT(*) AS total, COUNT(DISTINCT city) AS unique_cities
            FROM weather_data
            """
        )
    ).first()

    return {
        "dags": monitor,
        "data_stats": {
            "total_records": counts.total if counts else 0,
            "unique_cities": counts.unique_cities if counts else 0,
        },
    }


@app.get("/forecast")
async def forecast_page(request: Request):
    return templates.TemplateResponse("forecast.html", {"request": request})


@app.get("/api/forecast")
async def api_forecast(city: str):
    """
    Проксирует запрос к сервису ML Forecast.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://ml-forecast:8000/forecast/{city}") as resp:
                if resp.status == 404:
                    return JSONResponse(status_code=404, content={"error": "Город не найден или недостаточно данных"})
                if resp.status != 200:
                    text = await resp.text()
                    return JSONResponse(status_code=resp.status, content={"error": f"ML Service error: {text}"})
                data = await resp.json()
                return data
    except Exception as e:
        logger.error(f"Error proxying to ML service: {e}")
        return JSONResponse(status_code=503, content={"error": "ML Service unavailable"})


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    # Дружелюбный JSON для AJAX запросов
    if request.url.path.startswith("/api") or request.url.path.startswith("/trigger"):
        return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)

