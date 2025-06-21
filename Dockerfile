# Шаг 1: Используем официальный образ от Microsoft, где уже есть Python и ВСЕ зависимости Playwright
FROM mcr.microsoft.com/playwright/python:v1.52.0-jammy

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-rus \
    && rm -rf /var/lib/apt/lists/*

# Диагностический шаг: Находим реальный путь к файлам tessdata и устанавливаем его в переменную
RUN TESSDATA_PATH=$(find /usr/share -name "tessdata") && \
    echo "Found tessdata at: $TESSDATA_PATH" && \
    export TESSDATA_PREFIX=$(dirname "$TESSDATA_PATH")

# Шаг 2: Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Шаг 3: Копируем файл с зависимостями и устанавливаем их
COPY requirements.txt .
RUN pip install -r requirements.txt

# Шаг 4: Копируем весь остальной код твоего проекта в контейнер
COPY . .

# Шаг 5: Указываем, какой порт будет слушать наше приложение
EXPOSE 10000

# Шаг 6: Команда, которая запустит твое приложение при старте контейнера
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]