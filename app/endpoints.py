from fastapi import APIRouter, BackgroundTasks, HTTPException
import subprocess
import os
import logging
from datetime import datetime
from typing import Dict, Any

router = APIRouter()
logger = logging.getLogger(__name__)

def run_script_safe(script_path: str, script_name: str) -> Dict[str, Any]:
    """Безопасный запуск скрипта с обработкой ошибок"""
    start_time = datetime.now()
    try:
        logger.info(f"Запуск скрипта: {script_name}")
        
        # Проверяем существование скрипта
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Скрипт не найден: {script_path}")
        
        # Запускаем скрипт
        result = subprocess.run(
            ['python', script_path], 
            capture_output=True, 
            text=True, 
            timeout=1800  # 30 минут максимум
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        if result.returncode == 0:
            logger.info(f"Скрипт {script_name} выполнен успешно за {duration:.1f} сек")
            return {
                "status": "success",
                "script": script_name,
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "stdout": result.stdout[-1000:],  # Последние 1000 символов
                "message": f"Скрипт выполнен успешно за {duration:.1f} сек"
            }
        else:
            logger.error(f"Ошибка в скрипте {script_name}: {result.stderr}")
            return {
                "status": "error",
                "script": script_name,
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "stdout": result.stdout[-500:],
                "stderr": result.stderr[-500:],
                "message": f"Скрипт завершился с ошибкой"
            }
            
    except subprocess.TimeoutExpired:
        logger.error(f"Таймаут скрипта {script_name}")
        return {
            "status": "timeout",
            "script": script_name,
            "duration_seconds": 1800,
            "message": "Скрипт превысил лимит времени выполнения (30 мин)"
        }
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.error(f"Критическая ошибка в скрипте {script_name}: {str(e)}")
        return {
            "status": "critical_error",
            "script": script_name,
            "duration_seconds": duration,
            "error": str(e),
            "message": f"Критическая ошибка: {str(e)}"
        }

async def run_script_background(script_path: str, script_name: str, background_tasks: BackgroundTasks):
    """Запуск скрипта в фоне"""
    def execute_script():
        return run_script_safe(script_path, script_name)
    
    background_tasks.add_task(execute_script)
    return {
        "message": f"Скрипт {script_name} добавлен в очередь выполнения",
        "status": "queued",
        "script": script_name
    }

# === ENDPOINTS ===

@router.post("/collect-links")
async def run_collect_links(background_tasks: BackgroundTasks, sync: bool = False):
    """Запуск сборщика Session Replay ссылок"""
    script_path = "scripts/collect_links_put_gbq.py"
    
    if sync:
        # Синхронный запуск - ждем выполнения
        result = run_script_safe(script_path, "Collect Links")
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result)
        return result
    else:
        # Асинхронный запуск
        return await run_script_background(script_path, "Collect Links", background_tasks)

@router.post("/replay-screenshots")
async def run_replay_screenshots(background_tasks: BackgroundTasks, sync: bool = False):
    """Запуск сборщика скриншотов Session Replay"""
    script_path = "scripts/replay_ai_gbq.py"
    
    if sync:
        result = run_script_safe(script_path, "Replay Screenshots")
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result)
        return result
    else:
        return await run_script_background(script_path, "Replay Screenshots", background_tasks)

@router.post("/clustering")
async def run_clustering(background_tasks: BackgroundTasks, sync: bool = False):
    """Запуск кластеризации данных"""
    script_path = "scripts/get_clasters_gbq.py"
    
    if sync:
        result = run_script_safe(script_path, "Clustering")
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result)
        return result
    else:
        return await run_script_background(script_path, "Clustering", background_tasks)

@router.post("/summarize")
async def run_summarize(background_tasks: BackgroundTasks, sync: bool = False):
    """Запуск создания саммари через LLM"""
    script_path = "scripts/summarazing.py"
    
    if sync:
        result = run_script_safe(script_path, "Summarize")
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result)
        return result
    else:
        return await run_script_background(script_path, "Summarize", background_tasks)

@router.post("/full-pipeline")
async def run_full_pipeline(background_tasks: BackgroundTasks):
    """Запуск полного пайплайна обработки данных"""
    
    def execute_pipeline():
        pipeline_results = []
        scripts = [
            ("scripts/collect_links_put_gbq.py", "Collect Links"),
            ("scripts/replay_ai_gbq.py", "Replay Screenshots"), 
            ("scripts/get_clasters_gbq.py", "Clustering"),
            ("scripts/summarazing.py", "Summarize")
        ]
        
        for script_path, script_name in scripts:
            result = run_script_safe(script_path, script_name)
            pipeline_results.append(result)
            
            # Если скрипт упал с ошибкой, останавливаем пайплайн
            if result["status"] in ["error", "critical_error", "timeout"]:
                logger.error(f"Пайплайн остановлен на этапе: {script_name}")
                break
                
        return {
            "status": "completed",
            "pipeline_results": pipeline_results,
            "total_scripts": len(scripts),
            "completed_scripts": len(pipeline_results)
        }
    
    background_tasks.add_task(execute_pipeline)
    return {
        "message": "Полный пайплайн добавлен в очередь выполнения",
        "status": "queued",
        "scripts": ["Collect Links", "Replay Screenshots", "Clustering", "Summarize"]
    }

@router.get("/scripts/status")
async def get_scripts_status():
    """Статус доступных скриптов"""
    scripts = [
        {"name": "Collect Links", "path": "scripts/collect_links_put_gbq.py"},
        {"name": "Replay Screenshots", "path": "scripts/replay_ai_gbq.py"},
        {"name": "Clustering", "path": "scripts/get_clasters_gbq.py"},
        {"name": "Summarize", "path": "scripts/summarazing.py"}
    ]
    
    scripts_status = []
    for script in scripts:
        exists = os.path.exists(script["path"])
        scripts_status.append({
            "name": script["name"],
            "path": script["path"],
            "exists": exists,
            "status": "ready" if exists else "missing"
        })
    
    return {
        "scripts": scripts_status,
        "total_scripts": len(scripts),
        "ready_scripts": sum(1 for s in scripts_status if s["status"] == "ready")
    }