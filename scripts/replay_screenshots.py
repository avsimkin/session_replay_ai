import os
import sys
import time
import random
import json
from typing import Callable, Optional

from playwright.sync_api import sync_playwright, Error as PlaywrightError

USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"]
AMPLITUDE_URL = 'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D1247117195850?sessionReplayId=b04f4dad-3dea-4249-b9fe-78b689c822a5/1749812689447&sessionStartTime=1749812689447'

class RenderScreenshotCollector:
    def __init__(self, status_callback: Optional[Callable[[str, int], None]] = None):
        self.status_callback = status_callback
        self.cookies = self._load_cookies_from_secret_file()

    def _update_status(self, details: str, progress: int):
        if self.status_callback:
            self.status_callback(details, progress)
        print(f"[{progress}%] {details}")

    def _load_cookies_from_secret_file(self):
        secret_file_path = "/etc/secrets/cookies.json"
        if not os.path.exists(secret_file_path):
            self._update_status(f"Файл cookies не найден.", 2)
            return []
        try:
            with open(secret_file_path, 'r') as f:
                cookies = json.load(f)
            self._update_status(f"Cookies загружены ({len(cookies)} шт).", 2)
            return cookies
        except Exception as e:
            self._update_status(f"Ошибка чтения cookies: {e}", 2)
            return []

    def run(self):
        self._update_status("🚀 Запуск финального стресс-теста...", 0)
        
        with sync_playwright() as p:
            try:
                # ШАГ 1: Просто запустить браузер
                self._update_status("ШАГ 1: Запуск браузера...", 10)
                browser = p.firefox.launch(headless=True)
                context = browser.new_context(user_agent=random.choice(USER_AGENTS))
                if self.cookies:
                    context.add_cookies(self.cookies)
                page = context.new_page()
                self._update_status("✅ ШАГ 1 ПРОЙДЕН: Браузер запущен.", 33)
                time.sleep(5)

                # ШАГ 2: Открыть простую страницу
                self._update_status("ШАГ 2: Переход на google.com...", 40)
                page.goto("https://google.com", timeout=30000)
                self._update_status("✅ ШАГ 2 ПРОЙДЕН: Google.com открыт.", 66)
                time.sleep(5)

                # ШАГ 3: Открыть Amplitude
                self._update_status("ШАГ 3: Переход на Amplitude.com (самый сложный этап)...", 70)
                page.goto(AMPLITUDE_URL, timeout=90000, wait_until="domcontentloaded")
                self._update_status("✅ ШАГ 3 ПРОЙДЕН: Amplitude открыт без сбоя!", 95)
                
                browser.close()
                self._update_status("🏁 ТЕСТ УСПЕШНО ЗАВЕРШЕН! Окружение справляется с нагрузкой.", 100)
                return {"status": "TEST_SUCCESSFUL"}

            except Exception as e:
                self._update_status(f"❌ ТЕСТ ПРОВАЛЕН! Ошибка на одном из шагов: {e}", 100)
                return {"status": "TEST_FAILED", "error": str(e)}
