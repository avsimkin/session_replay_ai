import json
import os
import time
import hashlib
import random
import sys
from datetime import datetime
from playwright.sync_api import sync_playwright
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
        COOKIES = os.environ.get('COOKIES', '[]')
        PROCESSING_LIMIT = int(os.environ.get('PROCESSING_LIMIT', '10'))
        MIN_DURATION_SECONDS = int(os.environ.get('MIN_DURATION_SECONDS', '20'))
    settings = MockSettings()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

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
        self.safety_settings = {'min_delay': 2, 'max_delay': 5, 'name': 'ОБЫЧНЫЙ (АВТО)'}
        
        self._update_status("Настройка подключений...", 1)
        self.cookies = self._load_cookies_from_env()
        self._init_bigquery()
        self._init_google_drive()

    def _update_status(self, details: str, progress: int):
        if self.status_callback:
            self.status_callback(details, progress)
        if progress != -1: # Не печатаем служебные сообщения
            print(f"[{progress}%] {details}")

    def _load_cookies_from_env(self):
        try:
            cookies = json.loads(settings.COOKIES)
            self._update_status(f"Cookies загружены ({len(cookies)} записей)", 2)
            return cookies
        except Exception as e:
            self._update_status(f"Ошибка загрузки cookies: {e}", 2)
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

    def get_unprocessed_urls(self):
        query = f"""
        SELECT session_replay_url, amplitude_id, session_replay_id, duration_seconds, events_count, record_date
        FROM {self.full_table_name}
        WHERE is_processed = FALSE AND duration_seconds >= {self.min_duration}
        ORDER BY record_date DESC LIMIT {self.processing_limit}
        """
        try:
            result = self.bq_client.query(query).result()
            return [dict(row) for row in result]
        except Exception as e:
            self._update_status(f"Ошибка получения URL: {e}", 10)
            raise

    def mark_url_as_processed(self, url, screenshots_count=0, drive_folder_id=None, success=True):
        status_text = "успешно" if success else "с ошибкой"
        self._update_status(f"Обновление статуса URL ({status_text})", -1)
        try:
            update_query = f"""
            UPDATE {self.full_table_name}
            SET is_processed = TRUE, processed_datetime = CURRENT_TIMESTAMP(),
                screenshots_count = @screenshots_count, drive_folder_id = @drive_folder_id
            WHERE session_replay_url = @url
            """
            job_config = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("url", "STRING", url),
                bigquery.ScalarQueryParameter("screenshots_count", "INTEGER", screenshots_count),
                bigquery.ScalarQueryParameter("drive_folder_id", "STRING", drive_folder_id)])
            self.bq_client.query(update_query, job_config=job_config).result()
        except Exception as e:
            self._update_status(f"❌ Ошибка обновления статуса URL: {e}", -1)

    def get_session_id_from_url(self, url):
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        if "sessionReplayId=" in url:
            parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
            return f"{parts[0]}_{parts[1] if len(parts) > 1 else 'unknown'}_{url_hash}"
        return f"no_session_id_{url_hash}"

    def wait_for_content(self, page, selector, timeout=10):
        self._update_status(f"Ожидание контента (селектор: {selector}, таймаут: {timeout}с)", -1)
        try:
            element = page.wait_for_selector(selector, state='visible', timeout=timeout*1000)
            self._update_status("Контент загружен", -1)
            return element
        except Exception:
            self._update_status(f"Контент не загрузился за {timeout}с", -1)
            return None

    def screenshot_by_title(self, page, block_title, session_id, base_dir):
        self._update_status(f"Поиск блока '{block_title}'...", -1)
        try:
            parent = page.locator(f'h4:has-text("{block_title}")').locator('..')
            img_path = os.path.join(base_dir, f"{session_id}_{block_title.lower()}.png")
            parent.screenshot(path=img_path)
            self._update_status(f"✅ Скриншот '{block_title}' сохранён", -1)
            return img_path
        except Exception as e:
            self._update_status(f"❌ Не удалось сделать скриншот '{block_title}': {e}", -1)
            return None

    def create_and_upload_session_archive(self, session_dir, session_id):
        archive_path = None
        try:
            archive_name_base = f"session_replay_{session_id}_{int(time.time())}"
            archive_path = shutil.make_archive(archive_name_base, 'zip', session_dir)
            self._update_status(f"📦 Создан архив: {os.path.basename(archive_path)}", -1)
            
            file_metadata = {'name': os.path.basename(archive_path), 'parents': [self.gdrive_folder_id]}
            media = MediaFileUpload(archive_path, resumable=True)
            uploaded_file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id, name').execute()
            self._update_status(f"☁️ Архив загружен в Google Drive. ID: {uploaded_file.get('id')}", -1)
            return uploaded_file
        except Exception as e:
            self._update_status(f"❌ Ошибка создания/загрузки архива: {e}", -1)
            return None
        finally:
            if archive_path and os.path.exists(archive_path):
                os.remove(archive_path)

    def process_single_url(self, page, url_data):
        # ИСПРАВЛЕНИЕ: Используем правильный ключ 'session_replay_url'
        url = url_data['session_replay_url']
        session_id = self.get_session_id_from_url(url)
        
        session_dir = tempfile.mkdtemp(prefix=f"session_{session_id}_")
        screenshot_paths = []
        try:
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            page.locator("text=Summary").first.click()
            time.sleep(random.uniform(5, 8)) # Даем больше времени на прогрузку

            # Делаем скриншоты
            userinfo_path = page.locator('.cerulean-cardbase').first.screenshot(path=os.path.join(session_dir, f"{session_id}_userinfo.png"))
            screenshot_paths.append(userinfo_path)

            summary_element = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=20)
            if summary_element:
                summary_path = os.path.join(session_dir, f"{session_id}_summary.png")
                summary_element.screenshot(path=summary_path)
                screenshot_paths.append(summary_path)

            if len(screenshot_paths) < 1:
                 raise Exception("Не удалось сделать ни одного скриншота.")
            
            # Создаем metadata.json
            metadata = {"session_id": session_id, **url_data, "processed_at": datetime.now().isoformat()}
            with open(os.path.join(session_dir, "metadata.json"), 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            if not self.create_and_upload_session_archive(session_dir, session_id):
                raise Exception("Ошибка загрузки архива.")
            
            return True, len(screenshot_paths)
        except Exception as e:
            self._update_status(f"❌ Ошибка при обработке сессии {session_id}: {e}", -1)
            return False, len(screenshot_paths)
        finally:
             shutil.rmtree(session_dir, ignore_errors=True)

    def run(self):
        self._update_status("🚀 Запуск сборщика скриншотов", 0)
        self._update_status(f"🔍 Получение необработанных URL (лимит: {self.processing_limit})", 5)
        urls_to_process = self.get_unprocessed_urls()
        
        if not urls_to_process:
            self._update_status("🎉 Все подходящие URL уже обработаны.", 100)
            return {"status": "success", "message": "No new URLs to process."}

        total_urls = len(urls_to_process)
        self._update_status(f"🎯 Найдено {total_urls} URL. Режим: {self.safety_settings['name']}", 10)
        
        successful, failed, total_screenshots = 0, 0, 0
        start_time = time.time()
        
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
                    
                    url_to_mark = url_data['session_replay_url']
                    self.mark_url_as_processed(url_to_mark, screenshots_count, self.gdrive_folder_id if is_success else None, success=is_success)
                    
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
        
        total_time = time.time() - start_time
        result = {"status": "completed", "processed": total_urls, "successful": successful, "failed": failed, "time_minutes": round(total_time / 60, 1)}
        self._update_status(f"🏁 Завершено. Успешно: {successful}, Ошибки: {failed}", 100)
        return result

if __name__ == "__main__":
    collector = RenderScreenshotCollector()
    collector.run()
