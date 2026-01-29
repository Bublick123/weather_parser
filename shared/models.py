from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import os

Base = declarative_base()

class WeatherData(Base):
    __tablename__ = 'weather_data'
    
    id = Column(Integer, primary_key=True)
    city = Column(String(100), nullable=False)
    temperature = Column(Float, nullable=False)
    humidity = Column(Integer)
    pressure = Column(Integer)
    description = Column(String(200))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    def __repr__(self):
        return f"Weather(city='{self.city}', temp={self.temperature}C)"