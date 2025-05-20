FROM python:3.11-slim

# Ставим системные пакеты: tesseract, chromium и др.
RUN apt-get update && \
    apt-get install -y tesseract-ocr wget gnupg2 && \
    # Chromium deps
    apt-get install -y chromium chromium-driver fonts-liberation libappindicator3-1 libasound2 libatk-bridge2.0-0 libatk1.0-0 libcups2 libdbus-1-3 libgdk-pixbuf2.0-0 libnspr4 libnss3 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 xdg-utils && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Установить node (для playwright)
RUN apt-get update && \
    apt-get install -y nodejs npm

# Копируем проект
WORKDIR /app
COPY . .

# pip + playwright
RUN pip install --upgrade pip && pip install -r requirements.txt && playwright install chromium

CMD ["python", "main.py"]