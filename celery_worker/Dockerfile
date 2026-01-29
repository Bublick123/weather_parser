# Используем Alpine - он меньше и стабильнее
FROM python:3.9-alpine

WORKDIR /app

# Устанавливаем зависимости для сборки
RUN apk add --no-cache gcc musl-dev linux-headers postgresql-dev

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .