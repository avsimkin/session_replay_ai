# Шаг 1: Используем официальный образ от Microsoft, где уже есть Python и ВСЕ зависимости Playwright
FROM mcr.microsoft.com/playwright/python:v1.52.0-jammy

# --- НОВЫЕ СТРОКИ ДЛЯ УСТАНОВКИ TESSERACT OCR ---
# Обновляем списки пакетов и устанавливаем Tesseract + пакет для английского языка
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-eng \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем переменную окружения, чтобы Tesseract точно знал, где искать языки
ENV TESSDATA_PREFIX=/usr/share/tesseract-ocr/5/tessdata
# ----------------------------------------------------

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