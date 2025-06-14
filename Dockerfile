# 1. Используем официальный базовый образ Playwright для Python
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# 2. Установить Python-зависимости
# Копируем только requirements.txt для кэширования этого слоя
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Копировать весь проект
# Копируем остальную часть приложения
COPY . /app
WORKDIR /app

# 4. Указываем порт, который будет слушать приложение
ENV PORT=10000
EXPOSE 10000

# 5. Запуск приложения
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]