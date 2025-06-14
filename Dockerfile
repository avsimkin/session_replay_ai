# Используем официальный базовый образ Playwright для Python 3.11
# Этот образ содержит ВСЕ необходимые системные зависимости.
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# Устанавливаем рабочую директорию в контейнере
WORKDIR /app

# Копируем файл с зависимостями и устанавливаем их
# Этот шаг кэшируется, если requirements.txt не меняется
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем всё остальное приложение в рабочую директорию
COPY . .

# Сообщаем Docker, что приложение будет слушать этот порт
ENV PORT=10000
EXPOSE 10000

# Команда для запуска приложения
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
