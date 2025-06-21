# Шаг 1: Используем официальный образ от Microsoft
FROM mcr.microsoft.com/playwright/python:v1.52.0-jammy

# Шаг 2: Устанавливаем Tesseract и языковые пакеты
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-rus \
    && rm -rf /var/lib/apt/lists/*

# Шаг 3: Устанавливаем рабочую директорию
WORKDIR /app

# Шаг 4: Устанавливаем Python-зависимости
COPY requirements.txt .
RUN pip install -r requirements.txt

# Шаг 5: Копируем код проекта
COPY . .

# Шаг 6: Указываем порт
EXPOSE 10000

# Шаг 7: Команда запуска
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]