import logging
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from trainer import train_all_models, get_forecast_for_city

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ML Forecast Service")

class ForecastResponse(BaseModel):
    city: str
    prediction: float
    current_temp: float
    trend: dict
    pattern: str
    historical_temps: list
    timestamp: str

@app.post("/train")
async def train_models():
    """Запускает обучение моделей для всех городов"""
    try:
        success = train_all_models()
        if success:
            return {"status": "success", "message": "Models trained successfully"}
        else:
            raise HTTPException(status_code=500, detail="Training failed")
    except Exception as e:
        logger.error(f"Training error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/forecast/{city}")
async def get_forecast(city: str):
    """Получает прогноз для указанного города"""
    try:
        result = get_forecast_for_city(city)
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    except Exception as e:
        logger.error(f"Forecast error for {city}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "ok"}
