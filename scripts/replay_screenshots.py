import json
import os
import time
import hashlib
import random
import sys
from datetime import datetime
from playwright.sync_api import sync_playwright, Error as PlaywrightError
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import zipfile
import tempfile
import shutil
from typing import Callable, Optional

# Добавляем путь к корню проекта для импорта config (на всякий случай)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config.settings import settings
except ImportError:
    # Заглушка, если файл настроек не найден
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
        self.bq_project_id = settings.BQ_PROJECT_ID
        self.bq_dataset_id = settings.BQ_DATASET_ID
        self.bq_table_id = settings.BQ_TABLE_URLS
        self.gdrive_folder_id = settings.GDRIVE_FOLDER_ID
        self.processing_limit = settings.PROCESSING_LIMIT
        self.min_duration = settings.MIN_DURATION_SECONDS
        self.full_table_name = f"`{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table_id}`"
        self.safety_settings = {'min_delay': 3, 'max_delay': 6, 'name': 'НАДЁЖНЫЙ (АВТО)'}
        
        self._update_status("Настройка подключений...", 1)
        self.cookies = self._load_cookies_from_secret_file()
        self._init_bigquery()
        self._init_google_drive()

    def _update_status(self, details: str, progress: int):
        if self.status_callback:
            self.status_callback(details, progress)
        if progress != -1: # Не печатаем служебные сообщения
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

    def _init_bigquery(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(self.credentials_path, scopes=["https://www.googleapis.com/auth/bigquery"])
            self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project_id)
            self._update_status("BigQuery подключен", 3)
        except Exception as e:
            raise Exception(f"Ошибка подключения к BigQuery: {e}")

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
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            
            self._update_status("Ожидание страницы (Summary или Login)...", -1)
            page.wait_for_selector('text=Summary, input[type="email"]', timeout=30000)

            if page.locator('input[type="email"]').is_visible():
                raise PlaywrightError("Обнаружена страница входа. Проверьте COOKIES.")

            summary_tab = page.locator("text=Summary").first
            summary_tab.click(timeout=5000)
            self._update_status("Клик на 'Summary', ожидание загрузки...", -1)
            time.sleep(random.uniform(7, 10))

            screenshot_paths = []
            
            userinfo_element = page.locator('.cerulean-cardbase').first
            userinfo_element.wait_for(state='visible', timeout=10000)
            userinfo_path = os.path.join(session_dir, f"{session_id}_userinfo.png")
            userinfo_element.screenshot(path=userinfo_path)
            screenshot_paths.append(userinfo_path)

            summary_element = page.locator('p.ltext-_uoww22').first
            summary_element.wait_for(state='visible', timeout=20000)
            summary_path = os.path.join(session_dir, f"{session_id}_summary.png")
            summary_element.screenshot(path=summary_path)
            screenshot_paths.append(summary_path)
            
            if not screenshot_paths:
                 raise PlaywrightError("Не удалось сделать ни одного скриншота.")

            metadata = {"session_id": session_id, **url_data, "processed_at": datetime.now().isoformat()}
            with open(os.path.join(session_dir, "metadata.json"), 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            if not self.create_and_upload_archive(session_dir, session_id):
                raise PlaywrightError("Ошибка загрузки архива в Google Drive.")
            
            return True, len(screenshot_paths)

        except PlaywrightError as e:
            self._update_status(f"❌ Ошибка Playwright: {e}", -1)
            failure_path = os.path.join(session_dir, f"FAILURE_screenshot_{session_id}.png")
            page.screenshot(path=failure_path, full_page=True)
            self._update_status(f"📸 Сделан отладочный скриншот: {os.path.basename(failure_path)}", -1)
            self.create_and_upload_archive(session_dir, session_id, is_failure=True)
            return False, 0
        finally:
             shutil.rmtree(session_dir, ignore_errors=True)

    def run(self):
        # --- НАЧАЛО: ИЗМЕНЕНИЯ ДЛЯ ОТЛАДКИ ---
        self._update_status("⚡️ РЕЖИМ ОТЛАДКИ: Используются 3 тестовые ссылки.", 5)
        
        # Задаем ссылки жестко, вместо вызова get_unprocessed_urls()
        urls_to_process = [
            {
                'session_replay_url': 'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D1247117195850?sessionReplayId=b04f4dad-3dea-4249-b9fe-78b689c822a5/1749812689447&sessionStartTime=1749812689447',
                'amplitude_id': 1247117195850, 'session_replay_id': 'b04f4dad-3dea-4249-b9fe-78b689c822a5/1749812689447'
            },
            {
                'session_replay_url': 'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D1247144093674?sessionReplayId=09d7d9ec-9d2f-453b-83f5-5b403e45c202/1749823352686&sessionStartTime=1749823352686',
                'amplitude_id': 1247144093674, 'session_replay_id': '09d7d9ec-9d2f-453b-83f5-5b403e45c202/1749823352686'
            },
            {
                'session_replay_url': 'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D868026320025?sessionReplayId=03e5a484-6f63-4fb2-8964-2893e062ea27/1749825242509&sessionStartTime=1749825242509',
                'amplitude_id': 868026320025, 'session_replay_id': '03e5a484-6f63-4fb2-8964-2893e062ea27/1749825242509'
            }
        ]
        # --- КОНЕЦ: ИЗМЕНЕНИЯ ДЛЯ ОТЛАДКИ ---

        total_urls = len(urls_to_process)
        self._update_status(f"🎯 Найдено {total_urls} URL для отладки. Режим: {self.safety_settings['name']}", 10)
        
        successful, failed, total_screenshots = 0, 0, 0
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'])
            try:
                for i, url_data in enumerate(urls_to_process, 1):
                    progress = 10 + int((i / total_urls) * 85)
                    log_url = url_data.get('session_replay_url', 'N/A')
                    self._update_status(f"▶️ [{i}/{total_urls}] URL: {log_url[:70]}...", progress)
                    
                    context = browser.new_context(user_agent=random.choice(USER_AGENTS), viewport={'width': 1440, 'height': 900})
                    if self.cookies: context.add_cookies(self.cookies)
                    page = context.new_page()

                    is_success, screenshots_count = self.process_single_url(page, url_data)
                    
                    # Временно отключаем обновление статуса в БД, чтобы не портить данные
                    self._update_status(f"Отладка: пропуск обновления статуса в БД для {url_data['session_replay_url']}", -1)
                    
                    if is_success:
                        successful += 1
                        total_screenshots += screenshots_count
                    else:
                        failed += 1
                    
                    page.close()
                    context.close()
                    if i < total_urls:
                         time.sleep(random.uniform(self.safety_settings['min_delay'], self.safety_settings['max_delay']))
            finally:
                browser.close()
        
        result = {"status": "completed", "processed": total_urls, "successful": successful, "failed": failed}
        self._update_status(f"🏁 Отладка завершена. Успешно: {successful}, Ошибки: {failed}", 100)
        return result
