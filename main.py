from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import schedule
import time
import threading
import logging
import os
from datetime import datetime
import pytz
from typing import Dict, Any

# Импортируем роутер и хранилище из папки app
from app.endpoints import router
from app.state import task_statuses
from app.endpoints import run_script_safe

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Глобальная переменная для управления планировщиком
scheduler_running = True
moscow_tz = pytz.timezone("Europe/Moscow")

def run_daily_analytics_pipeline():
    """Ежедневный запуск полного пайплайна аналитики"""
    logger.info("🚀 Запуск ежедневного пайплайна аналитики")
    
    # ОПТИМИЗИРОВАННЫЙ ПОРЯДОК ПАЙПЛАЙНА
    pipeline_steps = [
        ("scripts/collect_links.py", "Сбор Session Replay ссылок"),          # 1. Собираем новые ссылки
        ("scripts/extract_text.py", "Извлечение текста OCR"),                # 2. Обрабатываем накопленные скриншоты  
        ("scripts/clustering_analysis.py", "Кластеризация и анализ"),        # 3. Анализируем данные
        # ("scripts/summarize.py", "Создание саммари"),                      # 4. Будущий этап: саммари
        # ("scripts/send_summaries.py", "Отправка саммари"),                 # 5. Будущий этап: отправка
        ("scripts/replay_screenshots.py", "Создание скриншотов"),             # 6. ПОСЛЕДНИЙ: долгая работа со скриншотами
    ]
    
    for script_path, step_name in pipeline_steps:
        logger.info(f"📝 Выполняем этап: {step_name}")
        result = run_script_safe(script_path, step_name)
        if result["status"] != "success":
            logger.error(f"❌ Этап {step_name} завершен с ошибкой, пайплайн остановлен.")
            
            # ИСКЛЮЧЕНИЕ: если ошибка в скриншотах - не останавливаем пайплайн полностью
            if "скриншот" in step_name.lower():
                logger.info("ℹ️ Ошибка в скриншотах не критична, но останавливаем дальнейшие этапы.")
            break
        logger.info(f"✅ Этап {step_name} завершен успешно.")


def run_scheduler():
    """Запуск планировщика задач в отдельном потоке"""
    logger.info("⏰ Планировщик задач запущен")
    while scheduler_running:
        schedule.run_pending()
        time.sleep(60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    schedule.every().day.at("06:00").do(run_daily_analytics_pipeline)
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    logger.info("🚀 Приложение запущено")
    yield
    
    global scheduler_running
    scheduler_running = False
    logger.info("🛑 Планировщик остановлен")

app = FastAPI(
    title="📊 Analytics Scripts API",
    description="Система автоматизации аналитических скриптов.",
    version="1.2.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")

@app.get("/api/task-status/{task_id}", tags=["📊 Monitoring"])
async def get_task_status(task_id: str):
    """Получить статус и прогресс выполнения фоновой задачи."""
    status = task_statuses.get(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    return status

@app.get("/", tags=["📍 General"])
async def root():
    """Главная страница API"""
    return {
        "service": "Analytics Scripts API",
        "status": "running",
        "version": "1.2.0"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
