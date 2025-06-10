from fastapi import FastAPI, BackgroundTasks
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

# Глобальная переменная для управления планировщиком
scheduler_running = True
moscow_tz = pytz.timezone("Europe/Moscow")

def run_daily_analytics_pipeline():
    """Ежедневный запуск полного пайплайна аналитики"""
    logger.info("🚀 Запуск ежедневного пайплайна аналитики")
    
    pipeline_steps = [
        ("scripts/collect_links_put_gbq.py", "Сбор Session Replay ссылок"),
        ("scripts/replay_ai_gbq.py", "Создание скриншотов"),
        ("scripts/get_clasters_gbq.py", "Кластеризация данных"),
        ("scripts/summarazing.py", "Создание саммари")
    ]
    
    results = []
    total_start = datetime.now(moscow_tz)
    
    for script_path, step_name in pipeline_steps:
        logger.info(f"📝 Выполняем этап: {step_name}")
        step_start = datetime.now(moscow_tz)
        
        try:
            result = run_script_safe(script_path, step_name)
            result["step_name"] = step_name
            results.append(result)
            
            step_end = datetime.now(moscow_tz)
            step_duration = (step_end - step_start).total_seconds()
            
            if result["status"] == "success":
                logger.info(f"✅ {step_name} завершен успешно за {step_duration:.1f} сек")
            else:
                logger.error(f"❌ {step_name} завершен с ошибкой: {result.get('message', 'Unknown error')}")
                # При ошибке прерываем пайплайн
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
    
    # Финальный отчет
    successful_steps = len([r for r in results if r["status"] == "success"])
    total_steps = len(results)
    
    pipeline_result = {
        "pipeline_status": "completed" if successful_steps == len(pipeline_steps) else "failed",
        "total_duration_seconds": total_duration,
        "total_duration_minutes": total_duration / 60,
        "start_time": total_start.isoformat(),
        "end_time": total_end.isoformat(),
        "successful_steps": successful_steps,
        "total_steps": total_steps,
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
            time.sleep(60)  # Проверяем каждую минуту
        except Exception as e:
            logger.error(f"Ошибка в планировщике: {str(e)}")
            time.sleep(60)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    
    # Настройка расписания
    # Основной пайплайн каждый день в 09:00 MSK
    schedule.every().day.at("06:00").do(run_daily_analytics_pipeline)  # 09:00 MSK = 06:00 UTC
    
    # Дополнительные задачи (по желанию)
    # schedule.every().day.at("18:00").do(lambda: run_script_safe("scripts/summarazing.py", "Evening Summary"))
    
    # Запуск планировщика в отдельном потоке
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    logger.info("🚀 Приложение запущено")
    logger.info("📅 Ежедневный пайплайн: каждый день в 09:00 MSK")
    
    yield
    
    # Остановка планировщика
    global scheduler_running
    scheduler_running = False
    logger.info("🛑 Планировщик остановлен")

# Создание FastAPI приложения
app = FastAPI(
    title="Analytics Scripts API",
    description="API для автоматизации аналитических скриптов Session Replay",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware для веб-интерфейса (если нужен)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение роутеров
app.include_router(router, prefix="/api")

@app.get("/")
async def root():
    """Корневой endpoint"""
    return {
        "service": "Analytics Scripts API",
        "status": "running",
        "version": "1.0.0",
        "description": "Автоматизация скриптов анализа Session Replay",
        "scheduler_active": scheduler_running,
        "scheduled_jobs": len(schedule.jobs),
        "current_time_utc": datetime.now().isoformat(),
        "current_time_msk": datetime.now(moscow_tz).isoformat(),
        "endpoints": {
            "collect_links": "/api/collect-links",
            "screenshots": "/api/replay-screenshots", 
            "clustering": "/api/clustering",
            "summarize": "/api/summarize",
            "full_pipeline": "/api/full-pipeline",
            "scripts_status": "/api/scripts/status",
            "scheduler_status": "/scheduler/status",
            "manual_pipeline": "/run/daily-pipeline"
        }
    }

@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "scheduler_running": scheduler_running,
        "environment": os.environ.get("ENVIRONMENT", "production")
    }

@app.get("/scheduler/status")
async def scheduler_status():
    """Статус планировщика и расписания"""
    jobs_info = []
    
    for job in schedule.jobs:
        try:
            next_run = job.next_run
            if next_run:
                # Конвертируем в московское время для удобства
                next_run_msk = next_run.replace(tzinfo=pytz.UTC).astimezone(moscow_tz)
                next_run_str = next_run_msk.strftime("%Y-%m-%d %H:%M:%S MSK")
            else:
                next_run_str = "Not scheduled"
                
            jobs_info.append({
                "job_function": str(job.job_func.__name__),
                "interval": str(job.interval),
                "unit": str(job.unit),
                "next_run_utc": str(job.next_run) if job.next_run else None,
                "next_run_msk": next_run_str,
                "tags": list(job.tags) if job.tags else []
            })
        except Exception as e:
            jobs_info.append({
                "error": f"Error getting job info: {str(e)}"
            })
    
    return {
        "scheduler_running": scheduler_running,
        "jobs_count": len(schedule.jobs),
        "jobs": jobs_info,
        "current_time_utc": datetime.now().isoformat(),
        "current_time_msk": datetime.now(moscow_tz).isoformat()
    }

@app.post("/run/daily-pipeline")
async def run_daily_pipeline_manual(background_tasks: BackgroundTasks):
    """Ручной запуск ежедневного пайплайна"""
    
    def execute_pipeline():
        logger.info("🎯 Ручной запуск ежедневного пайплайна")
        return run_daily_analytics_pipeline()
    
    background_tasks.add_task(execute_pipeline)
    
    return {
        "message": "Ежедневный пайплайн запущен вручную",
        "status": "queued", 
        "start_time": datetime.now().isoformat(),
        "estimated_duration_minutes": "10-30"
    }

@app.get("/logs")
async def get_recent_logs():
    """Получение последних логов (упрощенная версия)"""
    # В продакшене лучше использовать внешнее логирование (например, через Render Dashboard)
    return {
        "message": "Для просмотра логов используйте Render Dashboard",
        "log_location": "Render Dashboard -> Service -> Logs",
        "note": "Все логи записываются через стандартный logger Python"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)