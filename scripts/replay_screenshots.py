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

    def get_session_id_from_url(self, url):
        return hashlib.md5(url.encode()).hexdigest()

    def create_and_upload_archive(self, session_dir, session_id, is_failure=False):
        archive_path = None
        try:
            prefix = "FAILURE" if is_failure else "session_replay"
            archive_name_base = f"{prefix}_{session_id}_{int(time.time())}"
            archive_path = shutil.make_archive(archive_name_base, 'zip', session_dir)
            file_metadata = {'name': os.path.basename(archive_path), 'parents': [self.gdrive_folder_id]}
            media = MediaFileUpload(archive_path, resumable=True)
            uploaded_file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id, name').execute()
            self._update_status(f"☁️ Архив загружен. ID: {uploaded_file.get('id')}", -1)
            return uploaded_file
        finally:
            if archive_path and os.path.exists(archive_path):
                os.remove(archive_path)

    # --- НАЧАЛО: Ваша проверенная локальная логика ---

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

    def screenshot_userinfo_block(self, page, session_id, base_dir):
        self._update_status("Поиск блока 'User Info'...", -1)
        try:
            element = page.locator('.cerulean-cardbase.cerulean-alpha-general-card').first
            element.wait_for(state='visible', timeout=15000)
            img_path = os.path.join(base_dir, f"{session_id}_userinfo.png")
            element.screenshot(path=img_path)
            self._update_status("✅ Скриншот 'User Info' сделан.", -1)
            return img_path
        except Exception as e:
            self._update_status(f"❌ Ошибка скриншота 'User Info': {e}", -1)
            return None

    def screenshot_summary_flexible(self, page, session_id, base_dir):
        self._update_status("Поиск блока 'Summary'...", -1)
        try:
            # Ждем появления основного элемента
            page.wait_for_selector('p.ltext-_uoww22', state='visible', timeout=20000)
            element = page.locator('p.ltext-_uoww22').first
            # Проверяем, что в нем есть текст
            if len(element.inner_text()) < 20:
                self._update_status("Текст в Summary еще не загружен, ждем еще...", -1)
                time.sleep(10) # Дополнительное ожидание
            img_path = os.path.join(base_dir, f"{session_id}_summary.png")
            element.screenshot(path=img_path)
            self._update_status("✅ Скриншот 'Summary' сделан.", -1)
            return img_path
        except Exception as e:
            self._update_status(f"❌ Ошибка скриншота 'Summary': {e}", -1)
            return None
            
    def screenshot_by_title(self, page, block_title, session_id, base_dir):
        self._update_status(f"Поиск блока '{block_title}'...", -1)
        try:
            element = page.locator(f'h4:has-text("{block_title}")')
            parent_container = element.locator('xpath=./ancestor::div[contains(@class, "cerulean-card")]').first
            parent_container.wait_for(state='visible', timeout=10000)
            img_path = os.path.join(base_dir, f"{session_id}_{block_title.lower()}.png")
            parent_container.screenshot(path=img_path)
            self._update_status(f"✅ Скриншот '{block_title}' сделан.", -1)
            return img_path
        except Exception as e:
            self._update_status(f"❌ Ошибка скриншота '{block_title}': {e}", -1)
            return None

    def process_single_url(self, page, url_data):
        url = url_data['session_replay_url']
        session_id = self.get_session_id_from_url(url)
        session_dir = tempfile.mkdtemp(prefix=f"session_{session_id}_")
        
        try:
            page.goto(url, timeout=90000, wait_until='networkidle')
            self.simulate_human_behavior(page)
            
            summary_tab = page.locator("text=Summary").first
            summary_tab.click(timeout=10000)
            self._update_status("Клик на 'Summary', ожидание...", -1)
            time.sleep(10)
            self.simulate_human_behavior(page)

            paths = {
                "userinfo": self.screenshot_userinfo_block(page, session_id, session_dir),
                "summary": self.screenshot_summary_flexible(page, session_id, session_dir),
                "sentiment": self.screenshot_by_title(page, "Sentiment", session_id, session_dir),
                "actions": self.screenshot_by_title(page, "Actions", session_id, session_dir)
            }
            
            valid_screenshots = [p for p in paths.values() if p is not None]
            
            if len(valid_screenshots) < 3:
                 raise PlaywrightError(f"Сделано меньше 3 скриншотов ({len(valid_screenshots)}), сессия считается неудачной.")

            metadata = {"session_id": session_id, **url_data, "processed_at": datetime.now().isoformat()}
            with open(os.path.join(session_dir, "metadata.json"), 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            if not self.create_and_upload_archive(session_dir, session_id):
                raise PlaywrightError("Ошибка загрузки архива в Google Drive.")
            
            return True, len(valid_screenshots)

        except (PlaywrightError, PlaywrightTimeoutError) as e:
            self._update_status(f"❌ Ошибка Playwright: {e}", -1)
            failure_path = os.path.join(session_dir, f"FAILURE_screenshot.png")
            try:
                page.screenshot(path=failure_path, full_page=True, timeout=15000)
                self._update_status(f"📸 Сделан отладочный скриншот.", -1)
            except Exception as screenshot_error:
                self._update_status(f"❌ Не удалось сделать отладочный скриншот: {screenshot_error}", -1)
            
            self.create_and_upload_archive(session_dir, session_id, is_failure=True)
            return False, 0
        finally:
             shutil.rmtree(session_dir, ignore_errors=True)
             
    # --- КОНЕЦ: Ваша проверенная локальная логика ---

    def run(self):
        self._update_status("⚡️ ФИНАЛЬНАЯ ОТЛАДКА: Используются 3 тестовые ссылки.", 5)
        urls_to_process = [{'session_replay_url': url, 'amplitude_id': None, 'session_replay_id': None} for url in TEST_URLS]

        total_urls = len(urls_to_process)
        self._update_status(f"🎯 Найдено {total_urls} URL для отладки.", 10)
        
        successful, failed = 0, 0
        
        with sync_playwright() as p:
            # Используем Chromium, т.к. он более стабилен с вашими локаторами
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
