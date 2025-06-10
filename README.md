# Analytics Scripts API

Автоматизированная система обработки Session Replay данных из Amplitude с использованием BigQuery, Playwright и OpenAI.

## Функциональность

### 🔄 Автоматический пайплайн (ежедневно в 09:00 MSK):
1. **Сбор ссылок** (`collect_links_put_gbq.py`) - извлечение Session Replay ID из BigQuery
2. **Создание скриншотов** (`replay_ai_gbq.py`) - автоматизированные скриншоты через Playwright
3. **Кластеризация** (`get_clasters_gbq.py`) - ML-анализ и группировка сессий
4. **Саммари** (`summarazing.py`) - генерация отчетов через OpenAI

### 🎯 API Endpoints:
- `POST /api/collect-links` - запуск сбора ссылок
- `POST /api/replay-screenshots` - создание скриншотов
- `POST /api/clustering` - кластеризация данных
- `POST /api/summarize` - генерация саммари
- `POST /api/full-pipeline` - полный пайплайн
- `GET /api/scripts/status` - статус скриптов
- `GET /scheduler/status` - статус планировщика

## Быстрый старт

### 1. Подготовка репозитория:
```bash
# Клонируйте и настройте структуру
git clone <your-repo>
cd session_replay_ai

# Создайте структуру папок
mkdir -p app scripts config logs temp tests
touch app/__init__.py config/__init__.py tests/__init__.py

# Переместите скрипты
mv *.py scripts/ # (кроме main.py)
```

### 2. Настройка переменных окружения:
Создайте `.env` файл (для локальной разработки):
```env
BQ_PROJECT_ID=codellon-dwh
BQ_DATASET_ID=amplitude_session_replay
GOOGLE_APPLICATION_CREDENTIALS=config/bigquery-credentials.json
OPENAI_API_KEY=your_openai_key
GDRIVE_FOLDER_ID=your_folder_id
AMPLITUDE_PROJECT_ID=258068
```

### 3. Деплой на Render:

#### В Render Dashboard:
1. **New → Web Service**
2. **Connect GitHub repository**
3. **Build Command:**
   ```bash
   pip install --upgrade pip && pip install -r requirements.txt && playwright install chromium && python -c "import nltk; nltk.download('stopwords')"
   ```
4. **Start Command:**
   ```bash
   uvicorn main:app --host 0.0.0.0 --port $PORT
   ```

#### Environment Variables в Render:
```
PYTHON_VERSION=3.11
TZ=Europe/Moscow
ENVIRONMENT=production
OPENAI_API_KEY=your_key
GDRIVE_FOLDER_ID=your_folder_id
```

#### Secret Files в Render:
- Загрузите `bigquery-credentials.json` как секретный файл
- Path: `/etc/secrets/bigquery-credentials.json`

## Локальная разработка

```bash
# Установка зависимостей
pip install -r requirements.txt
playwright install chromium
python -c "import nltk; nltk.download('stopwords')"

# Запуск
uvicorn main:app --reload --port 8000
```

Откройте http://localhost:8000 для просмотра API.

## Мониторинг

### Логи:
- **Локально**: выводятся в консоль
- **Render**: Dashboard → Service → Logs

### Endpoints для мониторинга:
- `GET /health` - проверка здоровья
- `GET /scheduler/status` - статус планировщика  
- `GET /` - общая информация о сервисе

### Ручной запуск:
- `POST /run/daily-pipeline` - запуск полного пайплайна вручную

## Структура проекта

```
session_replay_ai/
├── main.py                    # FastAPI приложение
├── requirements.txt           # Зависимости
├── render.yaml               # Конфигурация Render
├── app/
│   └── endpoints.py          # API endpoints
├── scripts/                  # Рабочие скрипты
│   ├── collect_links_put_gbq.py
│   ├── replay_ai_gbq.py
│   ├── get_clasters_gbq.py
│   └── summarazing.py
├── config/
│   ├── settings.py           # Настройки приложения
│   └── credentials.json      # Google Cloud credentials
└── temp/                     # Временные файлы
```

## Безопасность

- ✅ Секретные файлы в `.gitignore`
- ✅ Переменные окружения для всех ключей
- ✅ Таймауты для скриптов (30 мин)
- ✅ Валидация настроек при запуске

## Поддержка

При возникновении проблем:
1. Проверьте логи в Render Dashboard
2. Убедитесь, что все переменные окружения настроены
3. Проверьте статус через `/health` endpoint

## Технологии

- **FastAPI** - веб-фреймворк
- **BigQuery** - хранилище данных
- **Playwright** - автоматизация браузера
- **OpenAI** - генерация саммари
- **Google Drive** - хранение файлов
- **scikit-learn** - машинное обучение
- **Render** - хостинг