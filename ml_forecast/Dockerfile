FROM python:3.9-slim

WORKDIR /app

# Устанавливаем системные зависимости для psycopg2 и ML библиотек
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Создаём директории для моделей и логов
RUN mkdir -p /app/models /app/logs

# Копируем код
COPY . .

# Создаём пользователя для безопасности (опционально)
RUN useradd -m -u 1000 mluser && chown -R mluser:mluser /app
USER mluser

# Команда по умолчанию - обучение моделей
CMD ["python", "trainer.py"]