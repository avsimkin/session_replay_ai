FROM python:3.11-slim

# 1. Установить системные зависимости для Chromium
RUN apt-get update && apt-get install -y \
    libgtk-3-0 \
    libgtk-4-1 \
    libgraphene-1.0-0 \
    libgstreamer-gl1.0-0 \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    libenchant-2-2 \
    libsecret-1-0 \
    libmanette-0.2-0 \
    libgles2 \
    libnss3 \
    libxss1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libnspr4 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    --no-install-recommends && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Установить Python-зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Установить Playwright и браузеры
RUN pip install playwright && playwright install --with-deps

# 4. Копировать проект
COPY . /app
WORKDIR /app

# 5. Запуск приложения
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
