FROM python:3.11-slim

# 1. Установить системные зависимости для Chromium
RUN apt-get update && apt-get install -y \
    wget \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libexpat1 \
    libgbm1 \
    libgcc1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    lsb-release \
    xdg-utils \
    libgbm-dev \
    libxshmfence1 \
    libgl1-mesa-glx \
    libgl1 \
    libgles2 \
    && rm -rf /var/lib/apt/lists/*

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
