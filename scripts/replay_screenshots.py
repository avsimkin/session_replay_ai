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

# --- Настройки и Константы ---
# Этот блок без изменений
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config.settings import settings
except ImportError:
    class MockSettings:
        GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '/etc/secrets/bigquery-credentials.json')
        BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'codellon-dwh')
        BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'amplitude_session_replay')
        BQ_TABLE_URLS = os.environ.get('BQ_TABLE_URLS', 'session_replay_urls')
        GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID', '1K8cbFU2gYpvP3PiHwOOHS1KREqdj6fQX')
        PROCESSING_LIMIT = int(os.environ.get('PROCESSING_LIMIT', '10'))
        MIN_DURATION_SECONDS = int(os.environ.get('MIN_DURATION_SECONDS', '20'))
    settings = MockSettings()
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"]

class RenderScreenshotCollector:
    def __init__(self, status_callback: Optional[Callable[[str, int], None]] = None):
        self.status_callback = status_callback
        self.credentials_path = settings.GOOGLE_APPLICATION_CREDENTIALS
        self.gdrive_folder_id = settings.GDRIVE_FOLDER_ID
        self._update_status("Настройка подключений...", 1)
        self.cookies = self._load_cookies_from_secret_file()
        self._init_google_drive()

    def _update_status(self, details: str, progress: int):
        if self.status_callback:
            self.status_callback(details, progress)
        if progress != -1:
            print(f"[{progress}%] {details}")

    def _load_cookies_from_secret_file(self):
        secret_file_path = "/etc/secrets/cookies.json"
        self._update_status(f"Попытка загрузки cookies из {secret_file_path}...", 2)
        if not os.path.exists(secret_file_path):
            self._update_status(f"❌ Файл {secret_file_path} не найден! Аутентификация не удастся.", 2)
            return []
        try:
            with open(secret_file_path, 'r') as f:
                cookies = json.load(f)
            self._update_status(f"✅ Cookies успешно загружены из Secret File ({len(cookies)} записей).", 2)
            return cookies
        except Exception as e:
            self._update_status(f"❌ Не удалось прочитать или распарсить {secret_file_path}: {e}", 2)
            return []

    def _init_google_drive(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(self.credentials_path, scopes=['https://www.googleapis.com/auth/drive'])
            self.drive_service = build('drive', 'v3', credentials=credentials)
            self._update_status("Google Drive подключен", 4)
        except Exception as e:
            raise Exception(f"Ошибка подключения к Google Drive: {e}")

    def get_session_id_from_url(self, url):
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        if "sessionReplayId=" in url:
            parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
            return f"{parts[0]}_{parts[1] if len(parts) > 1 else 'unknown'}_{url_hash}"
        return f"no_session_id_{url_hash}"

    def create_and_upload_archive(self, session_dir, session_id, is_failure=False):
        archive_path = None
        try:
            prefix = "FAILURE" if is_failure else "session_replay"
            archive_name_base = f"{prefix}_{session_id}_{int(time.time())}"
            archive_path = shutil.make_archive(archive_name_base, 'zip', session_dir)
            self._update_status(f"📦 Создан архив: {os.path.basename(archive_path)}", -1)
            file_metadata = {'name': os.path.basename(archive_path), 'parents': [self.gdrive_folder_id]}
            media = MediaFileUpload(archive_path, resumable=True)
            uploaded_file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id, name').execute()
            self._update_status(f"☁️ Архив загружен в Google Drive. ID: {uploaded_file.get('id')}", -1)
            return uploaded_file
        finally:
            if archive_path and os.path.exists(archive_path):
                os.remove(archive_path)

    def process_single_url(self, page, url_data):
        url = url_data['session_replay_url']
        session_id = self.get_session_id_from_url(url)
        session_dir = tempfile.mkdtemp(prefix=f"session_{session_id}_")
        
        try:
            # УЛУЧШЕНИЕ: Ждем полной загрузки сети, таймаут 90 секунд
            self._update_status("Переход на страницу и ожидание полной загрузки...", -1)
            page.goto(url, timeout=90000, wait_until='networkidle')

            # УЛУЧШЕНИЕ: Даем дополнительное время на рендер JS-фреймворков
            self._update_status("Дополнительное ожидание рендера...", -1)
            time.sleep(5)
            
            # Проверяем на страницу логина
            if page.locator('input[type="email"]').is_visible():
                raise PlaywrightError("Обнаружена страница входа. Проверьте COOKIES.")

            # УЛУЧШЕНИЕ: Более надежный клик с увеличенным таймаутом
            summary_tab = page.locator("text=Summary").first
            self._update_status("Поиск вкладки 'Summary'...", -1)
            summary_tab.wait_for(state='visible', timeout=20000)
            summary_tab.click()
            self._update_status("Клик на 'Summary', ожидание загрузки контента...", -1)
            time.sleep(10) # Увеличим ожидание после клика

            screenshot_paths = []
            
            # Скриншот блока с информацией
            userinfo_element = page.locator('.cerulean-cardbase').first
            userinfo_element.wait_for(state='visible', timeout=15000)
            userinfo_path = os.path.join(session_dir, f"{session_id}_userinfo.png")
            userinfo_element.screenshot(path=userinfo_path)
            screenshot_paths.append(userinfo_path)
            self._update_status("✅ Скриншот 'User Info' сделан.", -1)

            # Скриншот блока Summary
            summary_element = page.locator('p.ltext-_uoww22').first
            summary_element.wait_for(state='visible', timeout=30000) # Даем 30 секунд
            summary_path = os.path.join(session_dir, f"{session_id}_summary.png")
            summary_element.screenshot(path=summary_path)
            screenshot_paths.append(summary_path)
            self._update_status("✅ Скриншот 'Summary' сделан.", -1)
            
            if not screenshot_paths:
                 raise PlaywrightError("Не удалось сделать ни одного скриншота.")

            metadata = {"session_id": session_id, **url_data, "processed_at": datetime.now().isoformat()}
            with open(os.path.join(session_dir, "metadata.json"), 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            if not self.create_and_upload_archive(session_dir, session_id):
                raise PlaywrightError("Ошибка загрузки архива в Google Drive.")
            
            return True, len(screenshot_paths)

        except (PlaywrightError, PlaywrightTimeoutError) as e:
            self._update_status(f"❌ Ошибка Playwright: {e}", -1)
            failure_path = os.path.join(session_dir, f"FAILURE_screenshot_{session_id}.png")
            try:
                page.screenshot(path=failure_path, full_page=True, timeout=15000)
                self._update_status(f"📸 Сделан отладочный скриншот.", -1)
            except Exception as screenshot_error:
                self._update_status(f"❌ Не удалось сделать даже отладочный скриншот: {screenshot_error}", -1)
            
            self.create_and_upload_archive(session_dir, session_id, is_failure=True)
            return False, 0
        finally:
             shutil.rmtree(session_dir, ignore_errors=True)

    def run(self):
        # Режим отладки на 3 ссылках
        self._update_status("⚡️ РЕЖИМ ОТЛАДКИ: Используются 3 тестовые ссылки.", 5)
        urls_to_process = [
            {'session_replay_url': 'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D1247117195850?sessionReplayId=b04f4dad-3dea-4249-b9fe-78b689c822a5/1749812689447&sessionStartTime=1749812689447'},
            {'session_replay_url': 'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D1247144093674?sessionReplayId=09d7d9ec-9d2f-453b-83f5-5b403e45c202/1749823352686&sessionStartTime=1749823352686'},
            {'session_replay_url': 'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D868026320025?sessionReplayId=03e5a484-6f63-4fb2-8964-2893e062ea27/1749825242509&sessionStartTime=1749825242509'}
        ]

        total_urls = len(urls_to_process)
        self._update_status(f"🎯 Найдено {total_urls} URL для отладки.", 10)
        
        successful, failed = 0, 0
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'])
            try:
                for i, url_data in enumerate(urls_to_process, 1):
                    progress = 10 + int((i / total_urls) * 85)
                    self._update_status(f"▶️ [{i}/{total_urls}] URL: {url_data['session_replay_url'][:70]}...", progress)
                    
                    context = browser.new_context(user_agent=random.choice(USER_AGENTS), viewport={'width': 1440, 'height': 900})
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
