from fastapi import FastAPI, BackgroundTasks, HTTPException
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

scheduler_running = True
moscow_tz = pytz.timezone("Europe/Moscow")

def run_daily_analytics_pipeline():
    """Ежедневный запуск полного пайплайна аналитики"""
    logger.info("🚀 Запуск ежедневного пайплайна аналитики")
    
    # --- ИСПРАВЛЕННЫЙ ПОРЯДОК ПАЙПЛАЙНА ---
    pipeline_steps = [
        ("scripts/collect_links.py", "Сбор Session Replay ссылок"),
        ("scripts/extract_text.py", "Извлечение текста OCR"),
        ("scripts/clustering_analysis.py", "Кластеризация и анализ"),
        ("scripts/replay_screenshots.py", "Создание скриншотов"), # Этот шаг теперь последний
    ]
    # ----------------------------------------
    
    for script_path, step_name in pipeline_steps:
        logger.info(f"📝 Выполняем этап: {step_name}")
        result = run_script_safe(script_path, step_name)
        if result["status"] != "success":
            logger.error(f"❌ Этап {step_name} завершен с ошибкой, пайплайн остановлен.")
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
    version="1.3.0", # Обновим версию
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.include_router(router, prefix="/api")

@app.get("/api/task-status/{task_id}", tags=["📊 Monitoring"])
async def get_task_status(task_id: str):
    status = task_statuses.get(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    return status

@app.get("/", tags=["📍 General"])
async def root():
    return {"service": "Analytics Scripts API", "status": "running", "version": "1.3.0"}
