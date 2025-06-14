from fastapi import APIRouter, BackgroundTasks, HTTPException
import subprocess
import os
import logging
from datetime import datetime
import sys
import uuid
from typing import Dict, Any, Optional

# 1. Импортируем общее состояние из нового файла app/state.py, разрывая цикл.
from app.state import task_statuses

# 2. Импортируем логику скрипта напрямую.
#    Добавлен sys.path.append для надежности, если скрипт запускается из разных мест.
try:
    from scripts.s2_replay_ai_gbq import RenderScreenshotCollector
except ImportError:
    # Добавляем корневую директорию проекта в путь поиска модулей
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from scripts.s2_replay_ai_gbq import RenderScreenshotCollector


# Основной роутер для эндпоинтов
router = APIRouter()
logger = logging.getLogger(__name__)

def run_script_safe(script_path: str, script_name: str) -> Dict[str, Any]:
    """
    Безопасный запуск скрипта в виде отдельного процесса.
    Остается для простых скриптов, не требующих детального отслеживания.
    """
    start_time = datetime.now()
    try:
        logger.info(f"Запуск скрипта через subprocess: {script_name}")
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Скрипт не найден: {script_path}")
        
        process = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        stdout, stderr = process.communicate()
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        if process.returncode == 0:
            logger.info(f"Скрипт {script_name} выполнен успешно за {duration:.1f} сек")
            return {"status": "success", "script": script_name, "duration_seconds": duration, "output": stdout}
        else:
            logger.error(f"Ошибка в скрипте {script_name}: {stderr}")
            return {"status": "error", "script": script_name, "error": stderr}
            
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске скрипта {script_name}: {str(e)}")
        return {"status": "critical_error", "script": script_name, "error": str(e)}

def run_screenshot_task(task_id: str):
    """
    Функция-обёртка для запуска сборщика скриншотов с отслеживанием прогресса.
    """
    logger.info(f"🚀 Запуск задачи сбора скриншотов, ID: {task_id}")
    task_statuses[task_id].update({
        "status": "running",
        "progress": 0,
        "details": "Инициализация...",
        "start_time": datetime.now().isoformat()
    })

    def status_callback(details: str, progress: int):
        """Callback-функция для обновления статуса задачи изнутри скрипта."""
        if task_id in task_statuses:
            task_statuses[task_id]["details"] = details
            task_statuses[task_id]["progress"] = progress
            logger.info(f"Задача {task_id}: [{progress}%] {details}")

    try:
        collector = RenderScreenshotCollector(status_callback=status_callback)
        result = collector.run()
        
        task_statuses[task_id].update({
            "status": "completed",
            "progress": 100,
            "details": "Задача успешно завершена.",
            "end_time": datetime.now().isoformat(),
            "result": result
        })

    except Exception as e:
        error_message = f"Критическая ошибка в задаче: {str(e)}"
        logger.error(f"ID задачи {task_id}: {error_message}", exc_info=True)
        task_statuses[task_id].update({
            "status": "failed",
            "progress": 100, # Задача завершена, хоть и с ошибкой
            "details": error_message,
            "end_time": datetime.now().isoformat()
        })

# === API Эндпоинты для управления скриптами ===

@router.post("/scripts/collect-links", summary="🔗 1. Сбор Session Replay ссылок", tags=["🔧 Scripts Management"])
async def run_collect_links(background_tasks: BackgroundTasks):
    """Запуск сборщика Session Replay ссылок из BigQuery (фоновый режим)."""
    script_path = "scripts/1_collect_links_put_gbq.py"
    background_tasks.add_task(run_script_safe, script_path, "Collect Links")
    return {"message": "Скрипт 'Collect Links' добавлен в очередь выполнения."}


@router.post(
    "/scripts/screenshots",
    summary="📸 2. Создание скриншотов (с отслеживанием)",
    description="Запускает создание скриншотов и возвращает ID задачи для отслеживания прогресса.",
    tags=["🔧 Scripts Management"]
)
async def run_replay_screenshots_tracked(background_tasks: BackgroundTasks):
    """
    Запускает сбор скриншотов в фоне и возвращает ID задачи для отслеживания.
    """
    task_id = str(uuid.uuid4())
    task_statuses[task_id] = {
        "status": "queued", 
        "details": "Задача добавлена в очередь",
        "start_time": datetime.now().isoformat()
    }
    
    background_tasks.add_task(run_screenshot_task, task_id)
    
    return {
        "message": "Задача по созданию скриншотов запущена. Используйте ID для отслеживания статуса.",
        "task_id": task_id,
        "status_url": f"/api/task-status/{task_id}"
    }

@router.post("/scripts/extract-text", summary="📄 3. Извлечение текста", tags=["🔧 Scripts Management"])
async def run_extract_text(background_tasks: BackgroundTasks):
    """Запуск извлечения текста из скриншотов (фоновый режим)."""
    script_path = "scripts/3_collect_replay_screens.py"
    background_tasks.add_task(run_script_safe, script_path, "Extract Text")
    return {"message": "Скрипт 'Extract Text' добавлен в очередь выполнения."}

@router.post("/scripts/clustering", summary="🧠 4. ML-кластеризация", tags=["🔧 Scripts Management"])
async def run_clustering(background_tasks: BackgroundTasks):
    """Запуск кластеризации данных (фоновый режим)."""
    script_path = "scripts/4_get_clasters_gbq.py"
    background_tasks.add_task(run_script_safe, script_path, "Clustering")
    return {"message": "Скрипт 'Clustering' добавлен в очередь выполнения."}

@router.post("/scripts/summarize", summary="📝 5. AI Саммари", tags=["🔧 Scripts Management"])
async def run_summarize(background_tasks: BackgroundTasks):
    """Запуск создания саммари через LLM (фоновый режим)."""
    script_path = "scripts/5_summarazing.py"
    background_tasks.add_task(run_script_safe, script_path, "Summarize")
    return {"message": "Скрипт 'Summarize' добавлен в очередь выполнения."}
