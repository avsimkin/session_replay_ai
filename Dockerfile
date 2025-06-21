# Используем официальный образ от Microsoft
FROM mcr.microsoft.com/playwright/python:v1.52.0-jammy

# Устанавливаем переменные окружения для Tesseract
ENV DEBIAN_FRONTEND=noninteractive
ENV TESSDATA_PREFIX=/usr/share/tesseract-ocr/4.00/tessdata

# Обновляем пакеты и устанавливаем Tesseract
RUN apt-get update && \
    apt-get install -y \
        tesseract-ocr \
        tesseract-ocr-eng \
        tesseract-ocr-rus \
        libtesseract-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Проверяем установку Tesseract и создаем симлинки при необходимости
RUN tesseract --version && \
    echo "TESSDATA_PREFIX: $TESSDATA_PREFIX" && \
    ls -la /usr/share/tesseract-ocr/4.00/tessdata/ && \
    # Создаем альтернативные пути для совместимости
    mkdir -p /usr/share/tesseract-ocr/5/tessdata && \
    ln -sf /usr/share/tesseract-ocr/4.00/tessdata/* /usr/share/tesseract-ocr/5/tessdata/ && \
    mkdir -p /usr/share/tessdata && \
    ln -sf /usr/share/tesseract-ocr/4.00/tessdata/* /usr/share/tessdata/

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем requirements.txt и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код проекта
COPY . .

# Указываем порт
EXPOSE 10000

# Команда запуска с переменными окружения
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]