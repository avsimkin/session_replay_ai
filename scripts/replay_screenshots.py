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
        BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'codellon-dwh')
        BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'amplitude_session_replay')
        BQ_TABLE_URLS = os.environ.get('BQ_TABLE_URLS', 'session_replay_urls')
        GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID', '1K8cbFU2gYpvP3PiHwOOHS1KREqdj6fQX')
        PROCESSING_LIMIT = int(os.environ.get('PROCESSING_LIMIT', '20'))
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
        
        self._update_status("Настройка подключений...", 1)
        self.cookies = self._load_cookies_from_secret_file()
        self._init_bigquery()
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

    def _init_bigquery(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
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

    def get_unprocessed_urls(self):
        query = f"""SELECT * FROM {self.full_table_name} WHERE is_processed = FALSE AND duration_seconds >= {self.min_duration} ORDER BY record_date DESC LIMIT {self.processing_limit}"""
        return [dict(row) for row in self.bq_client.query(query).result()]

    def mark_url_as_processed(self, url, screenshots_count=0, drive_folder_id=None, success=True):
        status_text = "успешно" if success else "с ошибкой"
        self._update_status(f"Обновление статуса URL ({status_text})", -1)
        try:
            update_query = "UPDATE `{}` SET is_processed = TRUE, processed_datetime = CURRENT_TIMESTAMP(), screenshots_count = @screenshots_count, drive_folder_id = @drive_folder_id WHERE session_replay_url = @url".format(self.full_table_name.replace('`',''))
            params = [bigquery.ScalarQueryParameter("screenshots_count", "INTEGER", screenshots_count), bigquery.ScalarQueryParameter("drive_folder_id", "STRING", drive_folder_id), bigquery.ScalarQueryParameter("url", "STRING", url)]
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            self.bq_client.query(update_query, job_config=job_config).result()
        except Exception as e:
            self._update_status(f"❌ Ошибка обновления статуса URL: {e}", -1)
            
    def get_session_id_from_url(self, url):
        # ВОССТАНОВЛЕНА ВАША ЛОГИКА ИМЕНОВАНИЯ
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        if "sessionReplayId=" in url:
            parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
            session_replay_id = parts[0]
            session_start_time = parts[1] if len(parts) > 1 else "unknown"
            return f"{session_replay_id}_{session_start_time}_{url_hash}"
        return f"no_session_id_{url_hash}"

    def create_and_upload_archive(self, session_dir, session_id, is_failure=False):
        archive_path = None
        try:
            prefix = "FAILURE" if is_failure else "session_replay"
            archive_name_base = f"{prefix}_{session_id}" # Убрал timestamp для чистоты
            archive_path = shutil.make_archive(archive_name_base, 'zip', session_dir)
            file_metadata = {'name': os.path.basename(archive_path), 'parents': [self.gdrive_folder_id]}
            media = MediaFileUpload(archive_path, resumable=True)
            uploaded_file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id, name').execute()
            self._update_status(f"☁️ Архив загружен. ID: {uploaded_file.get('id')}", -1)
            return uploaded_file
        finally:
            if archive_path and os.path.exists(archive_path):
                os.remove(archive_path)

    # --- ИНТЕГРАЦИЯ ВАШЕЙ ЛОКАЛЬНОЙ ЛОГИКИ ---
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
        # Логика из вашего скрипта
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
        # Логика из вашего скрипта
        self._update_status("Поиск блока 'Summary'...", -1)
        try:
            element = page.wait_for_selector('p.ltext-_uoww22', state='visible', timeout=30000)
            if len(element.inner_text()) < 20:
                self._update_status("Текст в Summary еще не загружен, ждем еще...", -1)
                time.sleep(10)
            img_path = os.path.join(base_dir, f"{session_id}_summary.png")
            element.screenshot(path=img_path)
            self._update_status("✅ Скриншот 'Summary' сделан.", -1)
            return img_path
        except Exception as e:
            self._update_status(f"❌ Ошибка скриншота 'Summary': {e}", -1)
            return None
            
    def screenshot_by_title(self, page, block_title, session_id, base_dir):
        # Логика из вашего скрипта
        self._update_status(f"Поиск блока '{block_title}'...", -1)
        try:
            element = page.locator(f'h4:has-text("{block_title}")')
            parent_container = element.locator('xpath=./ancestor::div[contains(@class, "cerulean-card")]').first
            parent_container.wait_for(state='visible', timeout=15000)
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

            metadata = {"session_id": session_id, **url_data}
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
            except Exception as screenshot_error:
                self._update_status(f"❌ Не удалось сделать отладочный скриншот: {screenshot_error}", -1)
            self.create_and_upload_archive(session_dir, session_id, is_failure=True)
            return False, 0
        finally:
             shutil.rmtree(session_dir, ignore_errors=True)

    def run(self):
        self._update_status("🚀 Запуск сборщика скриншотов", 0)
        self._update_status(f"🔍 Получение URL (лимит: {self.processing_limit})", 5)
        urls_to_process = self.get_unprocessed_urls()
        
        if not urls_to_process:
            self._update_status("🎉 Все URL обработаны.", 100)
            return {"status": "success", "message": "No new URLs."}

        total_urls = len(urls_to_process)
        self._update_status(f"🎯 Найдено {total_urls} URL. Режим: Боевой", 10)
        
        successful, failed, total_screenshots = 0, 0, 0
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'])
            try:
                for i, url_data in enumerate(urls_to_process, 1):
                    progress = 10 + int((i / total_urls) * 85)
                    self._update_status(f"▶️ [{i}/{total_urls}] URL: {url_data['session_replay_url'][:70]}...", progress)
                    
                    context = browser.new_context(user_agent=random.choice(USER_AGENTS), viewport={'width': 1600, 'height': 1200})
                    if self.cookies: context.add_cookies(self.cookies)
                    page = context.new_page()

                    is_success, screenshots_count = self.process_single_url(page, url_data)
                    
                    self.mark_url_as_processed(url_data['session_replay_url'], screenshots_count, self.gdrive_folder_id if is_success else None, success=is_success)
                    
                    if is_success:
                        successful += 1
                        total_screenshots += screenshots_count
                    else:
                        failed += 1
                    
                    page.close()
                    context.close()
            finally:
                browser.close()
        
        result = {"status": "completed", "processed": total_urls, "successful": successful, "failed": failed}
        self._update_status(f"🏁 Завершено. Успешно: {successful}, Ошибки: {failed}", 100)
        return result
