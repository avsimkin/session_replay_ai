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

from app.endpoints import router, run_script_safe

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- НОВОЕ: Хранилище статусов задач ---
# Ключ - task_id, значение - информация о задаче
task_statuses: Dict[str, Any] = {}
# ----------------------------------------

# Глобальная переменная для управления планировщиком
scheduler_running = True
moscow_tz = pytz.timezone("Europe/Moscow")

def run_daily_analytics_pipeline():
    """Ежедневный запуск полного пайплайна аналитики"""
    logger.info("🚀 Запуск ежедневного пайплайна аналитики")
    
    pipeline_steps = [
        ("scripts/1_collect_links_put_gbq.py", "Сбор Session Replay ссылок"),
        ("scripts/2_replay_ai_gbq.py", "Создание скриншотов"),
        ("scripts/3_collect_replay_screens.py", "Извлечение текста"),
        ("scripts/4_get_clasters_gbq.py", "Кластеризация данных"),
        ("scripts/5_summarazing.py", "Создание саммари")
    ]
    
    results = []
    total_start = datetime.now(moscow_tz)
    
    for script_path, step_name in pipeline_steps:
        logger.info(f"📝 Выполняем этап: {step_name}")
        step_start = datetime.now(moscow_tz)
        
        try:
            # Примечание: для ежедневного пайплайна можно оставить старый запуск
            # или интегрировать новую систему отслеживания и сюда.
            # Пока оставляем как есть для простоты.
            result = run_script_safe(script_path, step_name)
            result["step_name"] = step_name
            results.append(result)
            
            step_end = datetime.now(moscow_tz)
            step_duration = (step_end - step_start).total_seconds()
            
            if result["status"] == "success":
                logger.info(f"✅ {step_name} завершен успешно за {step_duration:.1f} сек")
            else:
                logger.error(f"❌ {step_name} завершен с ошибкой: {result.get('message', 'Unknown error')}")
                break
                
        except Exception as e:
            logger.error(f"💥 Критическая ошибка в этапе {step_name}: {str(e)}")
            results.append({
                "status": "critical_error",
                "step_name": step_name,
                "error": str(e),
                "start_time": step_start.isoformat(),
                "end_time": datetime.now(moscow_tz).isoformat()
            })
            break
    
    total_end = datetime.now(moscow_tz)
    total_duration = (total_end - total_start).total_seconds()
    
    successful_steps = len([r for r in results if r["status"] == "success"])
    
    pipeline_result = {
        "pipeline_status": "completed" if successful_steps == len(pipeline_steps) else "failed",
        "total_duration_seconds": total_duration,
        "start_time": total_start.isoformat(),
        "end_time": total_end.isoformat(),
        "successful_steps": successful_steps,
        "total_steps": len(results),
        "steps_results": results
    }
    
    logger.info(f"🏁 Пайплайн завершен: {successful_steps}/{len(pipeline_steps)} этапов успешно")
    logger.info(f"⏱️ Общее время выполнения: {total_duration/60:.1f} минут")
    
    return pipeline_result

def run_scheduler():
    """Запуск планировщика задач в отдельном потоке"""
    logger.info("⏰ Планировщик задач запущен")
    
    while scheduler_running:
        try:
            schedule.run_pending()
            time.sleep(60)
        except Exception as e:
            logger.error(f"Ошибка в планировщике: {str(e)}")
            time.sleep(60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    schedule.every().day.at("06:00").do(run_daily_analytics_pipeline)
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    logger.info("🚀 Приложение запущено")
    logger.info("📅 Ежедневный пайплайн: каждый день в 09:00 MSK")
    
    yield
    
    global scheduler_running
    scheduler_running = False
    logger.info("🛑 Планировщик остановлен")

app = FastAPI(
    title="📊 Analytics Scripts API",
    description="""
    ## 🚀 Автоматизация аналитических скриптов Session Replay
    
    Система автоматической обработки Session Replay данных.
    """,
    version="1.0.0",
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

# --- НОВЫЙ ЭНДПОИНТ для проверки статуса задачи ---
@app.get("/api/task-status/{task_id}", tags=["📊 Monitoring"])
async def get_task_status(task_id: str):
    """
    Получить статус и прогресс выполнения фоновой задачи.
    """
    status = task_statuses.get(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    return status
# ----------------------------------------------------

@app.get("/", tags=["📍 General"])
async def root():
    """🏠 Главная страница API"""
    return {
        "service": "📊 Analytics Scripts API",
        "status": "🟢 running",
        "version": "1.0.0",
        "description": "Автоматизация скриптов анализа Session Replay",
        "scheduler_active": scheduler_running,
        "endpoints": {
            "📈 track_task": "/api/task-status/{task_id}",
            "📖 documentation": "/docs"
        }
    }

@app.get("/health", tags=["📍 General"])
async def health_check():
    """💊 Проверка здоровья сервиса"""
    return {
        "status": "🟢 healthy",
        "timestamp": datetime.now().isoformat(),
        "scheduler_running": scheduler_running
    }

@app.get("/scheduler/status", tags=["⏰ Scheduler"])
async def scheduler_status():
    """⏰ Статус планировщика и расписания"""
    jobs_info = []
    for job in schedule.jobs:
        jobs_info.append(str(job))
    
    return {
        "scheduler_running": scheduler_running,
        "jobs_count": len(schedule.jobs),
        "jobs": jobs_info
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)