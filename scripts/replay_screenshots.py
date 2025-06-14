import json
import os
import time
import random
import sys
from datetime import datetime
from playwright.sync_api import sync_playwright, Error as PlaywrightError
import tempfile
import shutil
from typing import Callable, Optional

class RenderScreenshotCollector:
    def __init__(self, status_callback: Optional[Callable[[str, int], None]] = None):
        self.status_callback = status_callback
        self.safety_settings = {'min_delay': 1, 'max_delay': 2}

    def _update_status(self, details: str, progress: int):
        if self.status_callback:
            self.status_callback(details, progress)
        print(f"[{progress}%] {details}")

    def process_test_url(self, page):
        try:
            self._update_status("Переход на google.com...", 25)
            page.goto("https://www.google.com", timeout=30000)
            
            self._update_status("Ожидание 5 секунд...", 50)
            time.sleep(5)
            
            self._update_status("Попытка сделать скриншот...", 75)
            
            # Используем временный файл для скриншота
            temp_dir = tempfile.mkdtemp()
            screenshot_path = os.path.join(temp_dir, "test_screenshot.png")
            page.screenshot(path=screenshot_path)
            
            self._update_status(f"✅ Тестовый скриншот успешно сделан в {screenshot_path}", 90)
            
            # Очистка
            shutil.rmtree(temp_dir)
            
            return True
        except PlaywrightError as e:
            self._update_status(f"❌ Критическая ошибка Playwright в простом тесте: {e}", 100)
            # Попытка сделать скриншот даже при ошибке, если это возможно
            try:
                page.screenshot(path="failure_test.png")
            except:
                pass
            return False

    def run(self):
        self._update_status("🚀 Запуск ЛАКМУСОВОГО ТЕСТА для Playwright...", 0)
        
        is_success = False
        with sync_playwright() as p:
            self._update_status("Запуск браузера Chromium...", 10)
            try:
                browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'])
                context = browser.new_context()
                page = context.new_page()
                
                is_success = self.process_test_url(page)
                
                browser.close()
            except Exception as e:
                self._update_status(f"❌ Не удалось даже запустить браузер: {e}", 100)
                is_success = False

        if is_success:
            self._update_status("✅ Лакмусовый тест пройден! Проблема не в окружении, а в логике работы с Amplitude.", 100)
            return {"status": "TEST_SUCCESS"}
        else:
            self._update_status("❌ Лакмусовый тест ПРОВАЛЕН! Проблема в окружении (Dockerfile или ресурсы Render).", 100)
            return {"status": "TEST_FAILED"}

if __name__ == "__main__":
    collector = RenderScreenshotCollector()
    collector.run()
