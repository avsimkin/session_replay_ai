from fastapi import APIRouter, BackgroundTasks
import subprocess
import os
import logging
from datetime import datetime
import sys
import uuid
from typing import Dict, Any

# Импортируем общее состояние из app.state
from app.state import task_statuses

# ИСПРАВЛЕННЫЙ ИМПОРТ: используем новое имя файла
from scripts.replay_screenshots import RenderScreenshotCollector

router = APIRouter()
logger = logging.getLogger(__name__)

def run_script_safe(script_path: str, script_name: str) -> Dict[str, Any]:
    """Безопасный запуск скрипта в виде отдельного процесса."""
    logger.info(f"Запуск скрипта через subprocess: {script_name}")
    try:
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Скрипт не найден: {script_path}")
        
        process = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            check=False,
            timeout=1800
        )
        
        if process.returncode == 0:
            logger.info(f"Скрипт {script_name} выполнен успешно.")
            return {"status": "success", "script": script_name, "output": process.stdout}
        else:
            logger.error(f"Ошибка в скрипте {script_name}: {process.stderr}")
            return {"status": "error", "script": script_name, "error": process.stderr}
            
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске {script_name}: {str(e)}")
        return {"status": "critical_error", "script": script_name, "error": str(e)}

def run_screenshot_task(task_id: str):
    """Функция-обёртка для запуска сборщика скриншотов с отслеживанием."""
    logger.info(f"🚀 Запуск задачи сбора скриншотов, ID: {task_id}")
    task_statuses[task_id].update({
        "status": "running",
        "progress": 0,
        "details": "Инициализация...",
        "start_time": datetime.now().isoformat()
    })

    def status_callback(details: str, progress: int):
        """Callback для обновления статуса задачи."""
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
            "progress": 100,
            "details": error_message,
            "end_time": datetime.now().isoformat()
        })

# === API Эндпоинты ===

@router.post("/scripts/screenshots", summary="📸 Создание скриншотов (с отслеживанием)", tags=["🔧 Scripts Management"])
async def run_replay_screenshots_tracked(background_tasks: BackgroundTasks):
    """Запускает сбор скриншотов в фоне и возвращает ID задачи для отслеживания."""
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

@router.post("/scripts/collect-links", summary="🔗 Сбор ссылок", tags=["🔧 Scripts Management"])
async def run_collect_links(background_tasks: BackgroundTasks):
    """Запуск сборщика ссылок (без детального отслеживания)."""
    # ИСПОЛЬЗУЕМ НОВОЕ ИМЯ ФАЙЛА
    script_path = "scripts/collect_links.py"
    background_tasks.add_task(run_script_safe, script_path, "Collect Links")
    return {"message": "Скрипт 'Collect Links' добавлен в очередь выполнения."}
