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

from app.endpoints import router
from app.state import task_statuses
from app.endpoints import run_script_safe

from app.endpoints import run_screenshot_task, run_ocr_task, run_clustering_task
from scripts.collect_links import main as run_collect_links_main

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

scheduler_running = True
moscow_tz = pytz.timezone("Europe/Moscow")

def run_daily_analytics_pipeline():
    """Ежедневный запуск полного пайплайна аналитики"""
    logger.info("🚀 Запуск ежедневного пайплайна аналитики")

    try:
        # --- ШАГ 1: Сбор ссылок ---
        logger.info("📝 Выполняем этап: Сбор Session Replay ссылок")
        result_links = run_collect_links_main()
        if result_links.get("status") != "success":
            logger.error(f"❌ Этап 'Сбор ссылок' завершен с ошибкой: {result_links.get('error')}. Пайплайн остановлен.")
            return # Прерываем выполнение
        logger.info(f"✅ Этап 'Сбор ссылок' завершен успешно. Собрано URL: {result_links.get('collected_urls', 0)}")

        # --- ШАГ 2: Извлечение текста OCR ---
        # Этот шаг теперь должен запускаться как задача, но мы ждем ее завершения.
        # Для простоты пайплайна, мы можем вызвать основную логику напрямую.
        logger.info("📝 Выполняем этап: Извлечение текста OCR")
        from scripts.extract_text import TextExtractionProcessor
        ocr_processor = TextExtractionProcessor()
        result_ocr = ocr_processor.run()
        if result_ocr.get("status") != "completed":
             logger.error(f"❌ Этап 'Извлечение текста OCR' завершен с ошибкой. Пайплайн остановлен.")
             return
        logger.info(f"✅ Этап 'Извлечение текста OCR' завершен успешно. Обработано: {result_ocr.get('total_processed', 0)}")


        # --- ШАГ 3: Кластеризация и анализ ---
        logger.info("📝 Выполняем этап: Кластеризация и анализ")
        from scripts.clustering_analysis import ClusteringAnalysisProcessor
        clustering_processor = ClusteringAnalysisProcessor()
        result_clustering = clustering_processor.run()
        if result_clustering.get("status") != "completed":
            logger.error(f"❌ Этап 'Кластеризация' завершен с ошибкой. Пайплайн остановлен.")
            return
        logger.info(f"✅ Этап 'Кластеризация' завершен успешно. Обработано: {result_clustering.get('total_processed', 0)}")

        # --- ШАГ 4: Создание скриншотов (самый долгий) ---
        # Запускаем его как и раньше, но напрямую
        logger.info("📝 Выполняем этап: Создание скриншотов")
        from scripts.replay_screenshots import RenderScreenshotCollector
        screenshot_collector = RenderScreenshotCollector()
        result_screenshots = screenshot_collector.run()
        # Этот процесс будет работать долго, как и задумано
        logger.info(f"✅ Этап 'Создание скриншотов' завершен. Результат: {result_screenshots}")

    except Exception as e:
        logger.error(f"💥 Критическая ошибка в ежедневном пайплайне: {e}", exc_info=True)

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
