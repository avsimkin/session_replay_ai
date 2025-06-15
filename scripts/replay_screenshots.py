import json
import os
import time
import hashlib
import random
import sys
from datetime import datetime
from playwright.sync_api import sync_playwright, Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError
import tempfile
import shutil
from typing import Callable, Optional

# Импорты для работы с Google API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.cloud import bigquery

# Добавляем путь к корню проекта для импорта config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config.settings import settings
except ImportError:
    class MockSettings:
        GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '/etc/secrets/bigquery-credentials.json')
        GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID', '1K8cbFU2gYpvP3PiHwOOHS1KREqdj6fQX')
    settings = MockSettings()

USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"]
TEST_URLS = [
    'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D1247117195850?sessionReplayId=b04f4dad-3dea-4249-b9fe-78b689c822a5/1749812689447&sessionStartTime=1749812689447',
    'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D1247144093674?sessionReplayId=09d7d9ec-9d2f-453b-83f5-5b403e45c202/1749823352686&sessionStartTime=1749823352686',
    'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D868026320025?sessionReplayId=03e5a484-6f63-4fb2-8964-2893e062ea27/1749825242509&sessionStartTime=1749825242509'
]

class RenderScreenshotCollector:
    # Адаптированный __init__ для сервера
    def __init__(self, status_callback: Optional[Callable[[str, int], None]] = None):
        self.status_callback = status_callback
        self.credentials_path = settings.GOOGLE_APPLICATION_CREDENTIALS
        self.gdrive_folder_id = settings.GDRIVE_FOLDER_ID
        
        self._update_status("Настройка подключений...", 1)
        self.cookies = self._load_cookies_from_secret_file()
        self._init_google_drive()

    def _update_status(self, details: str, progress: int):
        if self.status_callback: self.status_callback(details, progress)
        if progress != -1: print(f"[{progress}%] {details}")

    def _load_cookies_from_secret_file(self):
        secret_file_path = "/etc/secrets/cookies.json"
        self._update_status(f"Загрузка cookies из {secret_file_path}...", 2)
        if not os.path.exists(secret_file_path):
            self._update_status(f"❌ Файл cookies не найден!", 2)
            return []
        try:
            with open(secret_file_path, 'r') as f: cookies = json.load(f)
            self._update_status(f"✅ Cookies загружены ({len(cookies)} шт).", 2)
            return cookies
        except Exception as e:
            self._update_status(f"❌ Ошибка чтения cookies: {e}", 2)
            return []

    def _init_google_drive(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(self.credentials_path, scopes=['https://www.googleapis.com/auth/drive'])
            self.drive_service = build('drive', 'v3', credentials=credentials)
            self._update_status("Google Drive подключен", 4)
        except Exception as e:
            raise Exception(f"Ошибка подключения к Google Drive: {e}")

    # --- НАЧАЛО: ВСЯ ВАША ЛОКАЛЬНАЯ ЛОГИКА (СКОПИРОВАНА 1 В 1) ---

    def get_session_id_from_url(self, url):
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        if "sessionReplayId=" in url:
            parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
            session_replay_id = parts[0]
            session_start_time = parts[1] if len(parts) > 1 else "unknown"
            return f"{session_replay_id}_{session_start_time}_{url_hash}"
        return f"no_session_id_{url_hash}"

    def wait_for_content(self, page, selector, bad_texts=("Loading", "Loading summary"), timeout=10, min_text_length=10):
        self._update_status(f"⏳ Ждем загрузку контента (таймаут {timeout} сек)...", -1)
        start = time.time()
        while True:
            el = page.query_selector(selector)
            if el:
                txt = el.inner_text().strip()
                if txt and all(bad not in txt for bad in bad_texts) and len(txt) >= min_text_length:
                    self._update_status(f"✅ Контент загружен за {time.time() - start:.1f} сек", -1)
                    return el
            if time.time() - start > timeout:
                self._update_status(f"⚠️ Контент не загрузился за {timeout} сек", -1)
                return None
            time.sleep(0.5)
            
    def simulate_human_behavior(self, page):
        self._update_status("Имитация действий пользователя...", -1)
        try:
            for _ in range(random.randint(2, 4)):
                page.mouse.move(random.randint(200, 1200), random.randint(200, 700), steps=random.randint(5, 15))
                time.sleep(random.uniform(0.1, 0.3))
            if random.random() < 0.4:
                page.evaluate(f"window.scrollBy(0, {random.randint(100, 500) * random.choice([1, -1])})")
                time.sleep(random.uniform(0.5, 1.5))
        except Exception as e:
            self._update_status(f"Небольшая ошибка при имитации: {e}", -1)
    
    def screenshot_summary_flexible(self, page, session_id, base_dir, summary_el=None):
        self._update_status("📄 Ищем Summary блок...", -1)
        el = summary_el or self.wait_for_content(page, 'p.ltext-_uoww22', timeout=3)
        if not el:
            self._update_status("⚠️ Пробуем fallback селекторы для Summary...", -1)
            for selector in ['div[style*="min-width: 460px"]', '.ltext-_uoww22', 'div:has-text("Summary")', 'p:has-text("The user")']:
                el = page.query_selector(selector)
                if el and len(el.inner_text().strip()) > 20: break
            if not el:
                self._update_status("❌ Не удалось найти Summary блок.", -1)
                return None
        try:
            img_name = os.path.join(base_dir, f"{session_id}_summary.png")
            el.screenshot(path=img_name)
            self._update_status("✅ Summary скриншот сохранён", -1)
            return img_name
        except Exception as e:
            self._update_status(f"❌ Ошибка скриншота Summary: {e}", -1)
            return None

    def screenshot_by_title(self, page, block_title, session_id, base_dir):
        self._update_status(f"🔍 Ищем блок '{block_title}'...", -1)
        el = page.locator(f'h4:has-text("{block_title}")').locator('xpath=./ancestor::div[contains(@class, "cerulean-card")]').first
        try:
            el.wait_for(state='visible', timeout=15000)
            img_path = os.path.join(base_dir, f"{session_id}_{block_title.lower()}.png")
            el.screenshot(path=img_path)
            self._update_status(f"✅ {block_title} скриншот сохранён", -1)
            return img_path
        except Exception as e:
            self._update_status(f"❌ Ошибка скриншота {block_title}: {e}", -1)
            return None

    def screenshot_userinfo_block(self, page, session_id, base_dir):
        self._update_status("🔍 Ищем блок User Info...", -1)
        el = page.locator('.cerulean-cardbase.cerulean-alpha-general-card').first
        try:
            el.wait_for(state='visible', timeout=15000)
            img_path = os.path.join(base_dir, f"{session_id}_userinfo.png")
            el.screenshot(path=img_path)
            self._update_status("✅ User Info скриншот сохранён", -1)
            return img_path
        except Exception as e:
            self._update_status(f"❌ Ошибка скриншота User Info: {e}", -1)
            return None
    
    def process_single_url(self, page, url_data):
        url = url_data['session_replay_url']
        session_id = self.get_session_id_from_url(url)
        session_dir = tempfile.mkdtemp(prefix=f"session_{session_id}_")
        
        try:
            self.simulate_human_behavior(page)
            page.goto(url, timeout=90000, wait_until="networkidle")
            time.sleep(random.uniform(3, 5))
            
            summary_tab = page.query_selector("text=Summary")
            if not summary_tab: raise PlaywrightError("Вкладка Summary не найдена!")
            
            summary_tab.click()
            self._update_status("🖱️ Кликнули на Summary", -1)
            time.sleep(random.uniform(5, 8))
            self.simulate_human_behavior(page)
            
            summary_el = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=20)
            
            paths = {
                "userinfo": self.screenshot_userinfo_block(page, session_id, session_dir),
                "summary": self.screenshot_summary_flexible(page, session_id, session_dir, summary_el),
                "sentiment": self.screenshot_by_title(page, "Sentiment", session_id, session_dir),
                "actions": self.screenshot_by_title(page, "Actions", session_id, session_dir)
            }
            
            valid_screenshots = [p for p in paths.values() if p is not None]
            
            if len(valid_screenshots) < 3:
                 raise PlaywrightError(f"Сделано меньше 3 скриншотов ({len(valid_screenshots)}), сессия неудачная.")

            metadata = {"session_id": session_id, "url": url}
            with open(os.path.join(session_dir, "metadata.json"), 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            if not self.create_and_upload_archive(session_dir, session_id):
                raise PlaywrightError("Ошибка загрузки архива в Google Drive.")
            
            return True, len(valid_screenshots)

        except (PlaywrightError, PlaywrightTimeoutError) as e:
            self._update_status(f"❌ Ошибка Playwright: {e}", -1)
            failure_path = os.path.join(session_dir, f"FAILURE_screenshot.png")
            try: page.screenshot(path=failure_path, full_page=True, timeout=15000)
            except: pass
            self.create_and_upload_archive(session_dir, session_id, is_failure=True)
            return False, 0
        finally:
             shutil.rmtree(session_dir, ignore_errors=True)
             
    # --- КОНЕЦ: Ваша проверенная локальная логика ---

    def run(self):
        self._update_status("⚡️ ФИНАЛЬНАЯ ОТЛАДКА (с вашей логикой): 3 тестовые ссылки.", 5)
        urls_to_process = [{'session_replay_url': url} for url in TEST_URLS]
        total_urls = len(urls_to_process)
        self._update_status(f"🎯 Найдено {total_urls} URL для отладки.", 10)
        
        successful, failed = 0, 0
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'])
            try:
                for i, url_data in enumerate(urls_to_process, 1):
                    progress = 10 + int((i / total_urls) * 85)
                    self._update_status(f"▶️ [{i}/{total_urls}] URL: {url_data['session_replay_url'][:70]}...", progress)
                    context = browser.new_context(user_agent=random.choice(USER_AGENTS), viewport={'width': 1600, 'height': 1200})
                    if self.cookies: context.add_cookies(self.cookies)
                    page = context.new_page()
                    is_success, _ = self.process_single_url(page, url_data)
                    if is_success: successful += 1
                    else: failed += 1
                    page.close()
                    context.close()
            finally:
                browser.close()
        
        result = {"status": "completed", "processed": total_urls, "successful": successful, "failed": failed}
        self._update_status(f"🏁 Отладка завершена. Успешно: {successful}, Ошибки: {failed}", 100)
        return result

if __name__ == "__main__":
    collector = RenderScreenshotCollector()
    collector.run()
