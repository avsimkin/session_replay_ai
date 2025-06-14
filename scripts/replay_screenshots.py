import os
import sys
import time
import random
from typing import Callable, Optional

from playwright.sync_api import sync_playwright, Error as PlaywrightError

# --- Настройки и Константы ---
# В этом тестовом скрипте мы не используем внешние настройки
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"]
TEST_URLS = [
    'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D1247117195850?sessionReplayId=b04f4dad-3dea-4249-b9fe-78b689c822a5/1749812689447&sessionStartTime=1749812689447',
    'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D1247144093674?sessionReplayId=09d7d9ec-9d2f-453b-83f5-5b403e45c202/1749823352686&sessionStartTime=1749823352686',
    'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D868026320025?sessionReplayId=03e5a484-6f63-4fb2-8964-2893e062ea27/1749825242509&sessionStartTime=1749825242509'
]

class RenderScreenshotCollector:
    def __init__(self, status_callback: Optional[Callable[[str, int], None]] = None):
        """Упрощенная инициализация только для отслеживания статуса."""
        self.status_callback = status_callback
        self.cookies = self._load_cookies_from_secret_file()

    def _update_status(self, details: str, progress: int):
        if self.status_callback:
            self.status_callback(details, progress)
        print(f"[{progress}%] {details}")

    def _load_cookies_from_secret_file(self):
        """Читает cookies из "Secret File", если он существует."""
        secret_file_path = "/etc/secrets/cookies.json"
        if not os.path.exists(secret_file_path):
            self._update_status(f"Файл cookies не найден, продолжаю без аутентификации.", 2)
            return []
        try:
            with open(secret_file_path, 'r') as f:
                cookies = json.load(f)
            self._update_status(f"Cookies загружены ({len(cookies)} шт).", 2)
            return cookies
        except Exception as e:
            self._update_status(f"Ошибка чтения cookies: {e}", 2)
            return []

    def process_single_url(self, page, url: str) -> bool:
        """Пытается открыть URL и сделать скриншот."""
        try:
            # Переходим на страницу
            self._update_status(f"Переход на URL...", -1)
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            
            # Ждем немного, чтобы страница могла что-то отрисовать
            self._update_status(f"Ожидание 10 секунд...", -1)
            time.sleep(10)

            # Просто делаем скриншот (он никуда не сохранится, важен сам факт выполнения)
            self._update_status(f"Попытка сделать скриншот...", -1)
            page.screenshot(timeout=20000)
            
            self._update_status(f"✅ Скриншот для URL успешно сделан.", -1)
            return True

        except PlaywrightError as e:
            self._update_status(f"❌ Ошибка Playwright: {e}", -1)
            return False
        except Exception as e:
            self._update_status(f"❌ Неизвестная ошибка: {e}", -1)
            return False

    def run(self):
        self._update_status("🚀 Запуск минимального теста на 3 ссылках...", 0)
        
        successful, failed = 0, 0
        
        with sync_playwright() as p:
            self._update_status("Запуск браузера Firefox...", 10)
            try:
                # Используем Firefox как более легковесную альтернативу
                browser = p.firefox.launch(headless=True)
                
                for i, url in enumerate(TEST_URLS, 1):
                    progress = 10 + int((i / len(TEST_URLS)) * 85)
                    self._update_status(f"▶️ [{i}/{len(TEST_URLS)}] Тестируем URL: {url[:70]}...", progress)
                    
                    context = browser.new_context(user_agent=random.choice(USER_AGENTS))
                    if self.cookies:
                        context.add_cookies(self.cookies)
                    page = context.new_page()

                    if self.process_single_url(page, url):
                        successful += 1
                    else:
                        failed += 1
                    
                    page.close()
                    context.close()
                
                browser.close()
            except Exception as e:
                self._update_status(f"❌ Критическая ошибка при запуске браузера: {e}", 100)
                failed = len(TEST_URLS) - successful

        self._update_status(f"🏁 Тест завершен. Успешно: {successful}, Ошибки: {failed}", 100)
        return {"status": "test_completed", "successful": successful, "failed": failed}

if __name__ == "__main__":
    # Для возможности прямого запуска из консоли
    collector = RenderScreenshotCollector()
    collector.run()
