from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
import subprocess
import os
import logging
from datetime import datetime
import sys
from typing import Dict, Any, Optional

# Основной роутер
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
            [sys.executable, script_path], 
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

# === SCRIPTS MANAGEMENT ENDPOINTS ===

@router.post("/scripts/collect-links", 
            summary="🔗 Сбор Session Replay ссылок",
            description="Извлечение Session Replay ID из BigQuery и формирование ссылок",
            tags=["🔧 Scripts Management"])
async def run_collect_links(background_tasks: BackgroundTasks, sync: bool = False):
    """Запуск сборщика Session Replay ссылок из BigQuery"""
    script_path = "scripts/1_collect_links_put_gbq.py"
    
    if sync:
        result = run_script_safe(script_path, "Collect Links")
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result)
        return result
    else:
        return await run_script_background(script_path, "Collect Links", background_tasks)

@router.post("/scripts/screenshots", 
            summary="📸 Создание скриншотов",
            description="Автоматизированное создание скриншотов Session Replay через Playwright",
            tags=["🔧 Scripts Management"])
async def run_replay_screenshots(background_tasks: BackgroundTasks, sync: bool = False):
    """Запуск сборщика скриншотов Session Replay"""
    script_path = "scripts/2_replay_ai_gbq.py"
    
    if sync:
        result = run_script_safe(script_path, "Replay Screenshots")
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result)
        return result
    else:
        return await run_script_background(script_path, "Replay Screenshots", background_tasks)

@router.post("/scripts/extract-text", 
            summary="📄 Извлечение текста",
            description="Обработка скриншотов и извлечение текста из Google Drive",
            tags=["🔧 Scripts Management"])
async def run_extract_text(background_tasks: BackgroundTasks, sync: bool = False):
    """Запуск извлечения текста из скриншотов"""
    script_path = "scripts/3_collect_replay_screens.py"
    
    if sync:
        result = run_script_safe(script_path, "Extract Text")
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result)
        return result
    else:
        return await run_script_background(script_path, "Extract Text", background_tasks)

@router.post("/scripts/clustering", 
            summary="🧠 ML-кластеризация",
            description="Машинное обучение и кластеризация пользовательских сессий",
            tags=["🔧 Scripts Management"])
async def run_clustering(background_tasks: BackgroundTasks, sync: bool = False):
    """Запуск кластеризации данных"""
    script_path = "scripts/4_get_clasters_gbq.py"
    
    if sync:
        result = run_script_safe(script_path, "Clustering")
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result)
        return result
    else:
        return await run_script_background(script_path, "Clustering", background_tasks)

@router.post("/scripts/summarize", 
            summary="📝 AI Саммари",
            description="Генерация аналитических отчетов через OpenAI",
            tags=["🔧 Scripts Management"])
async def run_summarize(background_tasks: BackgroundTasks, sync: bool = False):
    """Запуск создания саммари через LLM"""
    script_path = "scripts/5_summarazing.py"
    
    if sync:
        result = run_script_safe(script_path, "Summarize")
        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result)
        return result
    else:
        return await run_script_background(script_path, "Summarize", background_tasks)

# === PIPELINE OPERATIONS ===

@router.post("/pipeline/full", 
            summary="🚀 Полный пайплайн",
            description="Запуск всех этапов обработки данных последовательно",
            tags=["🔄 Pipeline Operations"])
async def run_full_pipeline(background_tasks: BackgroundTasks):
    """Запуск полного пайплайна обработки данных"""
    
    def execute_pipeline():
        pipeline_results = []
        scripts = [
            ("scripts/1_collect_links_put_gbq.py", "🔗 Collect Links"),
            ("scripts/2_replay_ai_gbq.py", "📸 Replay Screenshots"), 
            ("scripts/3_collect_replay_screens.py", "📄 Extract Text"),
            ("scripts/4_get_clasters_gbq.py", "🧠 Clustering"),
            ("scripts/5_summarazing.py", "📝 Summarize")
        ]
        
        for script_path, script_name in scripts:
            result = run_script_safe(script_path, script_name)
            pipeline_results.append(result)
            
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
        "scripts": ["🔗 Collect Links", "📸 Replay Screenshots", "📄 Extract Text", "🧠 Clustering", "📝 Summarize"]
    }

# === MONITORING & STATUS ===

@router.get("/monitoring/scripts", 
           summary="📋 Статус скриптов",
           description="Проверка доступности и готовности всех скриптов",
           tags=["📊 Monitoring"])
async def get_scripts_status():
    """Статус доступных скриптов"""
    scripts = [
        {"name": "🔗 Collect Links", "path": "scripts/1_collect_links_put_gbq.py"},
        {"name": "📸 Replay Screenshots", "path": "scripts/2_replay_ai_gbq.py"},
        {"name": "📄 Extract Text", "path": "scripts/3_collect_replay_screens.py"},
        {"name": "🧠 Clustering", "path": "scripts/4_get_clasters_gbq.py"},
        {"name": "📝 Summarize", "path": "scripts/5_summarazing.py"}
    ]
    
    scripts_status = []
    for script in scripts:
        exists = os.path.exists(script["path"])
        scripts_status.append({
            "name": script["name"],
            "path": script["path"],
            "exists": exists,
            "status": "✅ ready" if exists else "❌ missing"
        })
    
    return {
        "scripts": scripts_status,
        "total_scripts": len(scripts),
        "ready_scripts": sum(1 for s in scripts_status if s["status"] == "✅ ready")
    }

# === LEGACY ENDPOINTS (для обратной совместимости) ===

@router.post("/collect-links", 
            summary="🔗 Сбор ссылок (legacy)",
            description="Устаревший endpoint, используйте /scripts/collect-links",
            deprecated=True,
            tags=["⚠️ Legacy"])
async def run_collect_links_legacy(background_tasks: BackgroundTasks, sync: bool = False):
    """Legacy endpoint - используйте /scripts/collect-links"""
    return await run_collect_links(background_tasks, sync)

@router.post("/replay-screenshots", 
            summary="📸 Скриншоты (legacy)",
            description="Устаревший endpoint, используйте /scripts/screenshots", 
            deprecated=True,
            tags=["⚠️ Legacy"])
async def run_replay_screenshots_legacy(background_tasks: BackgroundTasks, sync: bool = False):
    """Legacy endpoint - используйте /scripts/screenshots"""
    return await run_replay_screenshots(background_tasks, sync)

@router.post("/clustering", 
            summary="🧠 Кластеризация (legacy)",
            description="Устаревший endpoint, используйте /scripts/clustering",
            deprecated=True,
            tags=["⚠️ Legacy"])
async def run_clustering_legacy(background_tasks: BackgroundTasks, sync: bool = False):
    """Legacy endpoint - используйте /scripts/clustering"""
    return await run_clustering(background_tasks, sync)

@router.post("/summarize", 
            summary="📝 Саммари (legacy)",
            description="Устаревший endpoint, используйте /scripts/summarize",
            deprecated=True,
            tags=["⚠️ Legacy"])
async def run_summarize_legacy(background_tasks: BackgroundTasks, sync: bool = False):
    """Legacy endpoint - используйте /scripts/summarize"""
    return await run_summarize(background_tasks, sync)

@router.post("/full-pipeline", 
            summary="🚀 Полный пайплайн (legacy)",
            description="Устаревший endpoint, используйте /pipeline/full",
            deprecated=True,
            tags=["⚠️ Legacy"])
async def run_full_pipeline_legacy(background_tasks: BackgroundTasks):
    """Legacy endpoint - используйте /pipeline/full"""
    return await run_full_pipeline(background_tasks)

@router.get("/scripts/status", 
           summary="📋 Статус скриптов (legacy)",
           description="Устаревший endpoint, используйте /monitoring/scripts",
           deprecated=True,
           tags=["⚠️ Legacy"])
async def get_scripts_status_legacy():
    """Legacy endpoint - используйте /monitoring/scripts"""
    return await get_scripts_status()