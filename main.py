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
import multiprocessing # <-- 1. ДОБАВЛЯЕМ ИМПОРТ

from app.endpoints import router
from app.state import task_statuses
# Эти импорты больше не нужны в API, т.к. запуск идет из пайплайна
# from app.endpoints import run_screenshot_task, run_ocr_task, run_clustering_task
from scripts.collect_links import main as run_collect_links_main

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

scheduler_running = False
moscow_tz = pytz.timezone("Europe/Moscow")

def run_daily_analytics_pipeline():
    """
    Полный пайплайн аналитики. Эта функция будет выполняться в отдельном процессе.
    ВАЖНО: все импорты должны быть внутри, чтобы процесс был самодостаточным.
    """
    try:
        logger.info("🚀 [ФОНОВЫЙ ПРОЦЕСС] Запуск ежедневного пайплайна аналитики")

        # --- ШАГ 1: Сбор ссылок ---
        logger.info("📝 [ФОНОВЫЙ ПРОЦЕСС] Выполняем этап: Сбор Session Replay ссылок")
        # Тут может быть прямой вызов вашей функции сбора ссылок
        # from scripts.collect_links import main as run_collect_links_main
        # result_links = run_collect_links_main()
        # logger.info(f"✅ [ФОНОВЫЙ ПРОЦЕСС] Этап 'Сбор ссылок' завершен.")

        # --- ШАГ 2: Извлечение текста OCR ---
        logger.info("📝 [ФОНОВЫЙ ПРОЦЕСС] Выполняем этап: Извлечение текста OCR")
        from scripts.extract_text import TextExtractionProcessor
        ocr_processor = TextExtractionProcessor()
        ocr_processor.run()
        logger.info(f"✅ [ФОНОВЫЙ ПРОЦЕСС] Этап 'Извлечение текста OCR' завершен.")

        # --- ШАГ 3: Кластеризация и анализ ---
        logger.info("📝 [ФОНОВЫЙ ПРОЦЕСС] Выполняем этап: Кластеризация и анализ")
        from scripts.clustering_analysis import ClusteringAnalysisProcessor
        clustering_processor = ClusteringAnalysisProcessor()
        clustering_processor.run()
        logger.info(f"✅ [ФОНОВЫЙ ПРОЦЕСС] Этап 'Кластеризация' завершен.")

        # --- ШАГ 4: Создание скриншотов (самый долгий) ---
        logger.info("📝 [ФОНОВЫЙ ПРОЦЕСС] Выполняем этап: Создание скриншотов")
        from scripts.replay_screenshots import RenderScreenshotCollector
        screenshot_collector = RenderScreenshotCollector()
        screenshot_collector.run()
        logger.info(f"✅ [ФОНОВЫЙ ПРОЦЕСС] Этап 'Создание скриншотов' завершен.")

    except Exception as e:
        logger.error(f"💥 [ФОНОВЫЙ ПРОЦЕСС] Критическая ошибка в ежедневном пайплайне: {e}", exc_info=True)
    
    logger.info("🏁 [ФОНОВЫЙ ПРОЦЕСС] Пайплайн завершил свою работу.")


# --- 2. НОВАЯ ФУНКЦИЯ-ОБЕРТКА ---
def run_pipeline_in_background():
    """
    Эта функция запускается по расписанию.
    Ее единственная задача - создать и запустить пайплайн в новом процессе.
    """
    logger.info("⏰ Сработало расписание. Запускаем пайплайн в отдельном процессе...")
    
    # Создаем новый процесс, который будет выполнять наш тяжелый пайплайн
    pipeline_process = multiprocessing.Process(target=run_daily_analytics_pipeline)
    pipeline_process.start()
    
    logger.info(f"✅ Пайплайн запущен в фоновом процессе с PID: {pipeline_process.pid}. API продолжает работать.")


def run_scheduler():
    """Запуск планировщика задач в отдельном потоке"""
    logger.info("⏰ Планировщик задач запущен")
    logger.info(f"📅 Текущие задачи: {len(schedule.jobs)} шт.")
    
    while scheduler_running:
        logger.info(f"⏰ Планировщик проверяет задачи... Время: {datetime.now(moscow_tz)}")
        schedule.run_pending()
        time.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    
    # ИСПРАВЛЯЕМ: Сначала устанавливаем флаг
    global scheduler_running
    scheduler_running = True
    
    # Добавляем задачи
    schedule.every().day.at("06:00", moscow_tz).do(run_pipeline_in_background)
    # Тестовая задача каждые 5 минут
    schedule.every(5).minutes.do(lambda: logger.info("🔔 ТЕСТ: Планировщик работает!"))
    
    # Запускаем планировщик
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    logger.info("🚀 Приложение запущено")
    yield
    
    # При завершении
    scheduler_running = False
    logger.info("🛑 Планировщик остановлен")

app = FastAPI(
    title="📊 Analytics Scripts API",
    description="Система автоматизации аналитических скриптов.",
    version="1.4.0", # Обновим версию
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.include_router(router, prefix="/api")

@app.get("/api/task-status/{task_id}", tags=["📊 Monitoring"])
async def get_task_status(task_id: str):
    # Этот эндпоинт теперь для ручных запусков, которые мы можем добавить позже
    status = task_statuses.get(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    return status

@app.get("/", tags=["📍 General"])
async def root():
    return {"service": "Analytics Scripts API", "status": "running", "version": "1.4.0"}

# --- 4. НЕ ЗАБЫВАЕМ УКАЗАТЬ МЕТОД ЗАПУСКА ПРОЦЕССОВ ---
if __name__ == "__main__":
    # Это нужно для стабильности на Linux (Render)
    # Этот блок не выполняется при запуске через uvicorn,
    # поэтому set_start_method нужно вызвать до создания первого процесса.
    # Лучше всего это сделать в самом начале файла, если возможно, или здесь.
    try:
        multiprocessing.set_start_method('spawn', force=True)
        logger.info("Установлен метод 'spawn' для multiprocessing.")
    except RuntimeError:
        logger.warning("Метод multiprocessing уже был установлен.")
        pass

    # Этот код для локального запуска, на Render он не будет выполняться
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)