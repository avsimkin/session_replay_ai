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
from typing import Callable, Optional

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
        """Инициализация с callback-функцией для статуса."""
        self.credentials_path = settings.GOOGLE_APPLICATION_CREDENTIALS
        self.bq_project_id = settings.BQ_PROJECT_ID
        self.bq_dataset_id = settings.BQ_DATASET_ID
        self.bq_table_id = settings.BQ_TABLE_URLS
        self.gdrive_folder_id = settings.GDRIVE_FOLDER_ID
        self.processing_limit = settings.PROCESSING_LIMIT
        self.min_duration = settings.MIN_DURATION_SECONDS
        self.cookies = self._load_cookies_from_env()
        self.safety_settings = {
            'min_delay': 2, 'max_delay': 4, 'batch_size': 5,
            'batch_pause_min': 30, 'batch_pause_max': 60, 'name': 'RENDER_AUTO'
        }
        self.full_table_name = f"`{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table_id}`"
        self.status_callback = status_callback
        
        self._update_status("Настройка подключений...", 1)
        self._init_bigquery()
        self._init_google_drive()

    def _update_status(self, details: str, progress: int):
        """Вызывает callback для обновления статуса."""
        if self.status_callback:
            self.status_callback(details, progress)
        else:
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
        SELECT session_replay_url, amplitude_id, session_replay_id, duration_seconds
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

    def mark_url_as_processed(self, url, screenshots_count=0, drive_folder_id=None):
        update_query = f"""
        UPDATE {self.full_table_name}
        SET is_processed = TRUE, processed_datetime = CURRENT_TIMESTAMP(),
            screenshots_count = @screenshots_count, drive_folder_id = @drive_folder_id
        WHERE session_replay_url = @url
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("url", "STRING", url),
                bigquery.ScalarQueryParameter("screenshots_count", "INTEGER", screenshots_count),
                bigquery.ScalarQueryParameter("drive_folder_id", "STRING", drive_folder_id)
            ]
        )
        self.bq_client.query(update_query, job_config=job_config).result()

    def run(self):
        """Основной метод запуска."""
        self._update_status("🚀 Запуск сборщика скриншотов", 0)
        
        self._update_status(f"🔍 Получение необработанных URL (лимит: {self.processing_limit})", 5)
        urls_data = self.get_unprocessed_urls()
        
        if not urls_data:
            self._update_status("🎉 Все подходящие URL уже обработаны.", 100)
            return {"status": "success", "message": "No new URLs to process."}

        total_urls = len(urls_data)
        self._update_status(f"🎯 Найдено {total_urls} URL для обработки.", 10)
        
        successful, failed, total_screenshots = 0, 0, 0
        start_time = time.time()
        
        with sync_playwright() as p:
            browser_args = ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
            browser = p.chromium.launch(headless=True, args=browser_args)
            
            try:
                for i, url_data in enumerate(urls_data, 1):
                    progress = 10 + int((i / total_urls) * 85)
                    self._update_status(f"▶️ [{i}/{total_urls}] Обработка URL: {url_data['session_replay_url'][:50]}...", progress)
                    
                    context = browser.new_context(user_agent=random.choice(USER_AGENTS))
                    if self.cookies:
                        context.add_cookies(self.cookies)
                    page = context.new_page()

                    # Здесь должна быть ваша логика обработки одной ссылки, например:
                    # success, screenshots_count = self.process_single_url(page, url_data)
                    # Для примера, имитируем работу:
                    time.sleep(random.uniform(2, 4)) 
                    is_success_mock = random.random() > 0.1 # 90% success rate
                    screenshots_count_mock = random.randint(2, 5) if is_success_mock else 0
                    
                    self.mark_url_as_processed(url_data['session_replay_url'], screenshots_count_mock, self.gdrive_folder_id if is_success_mock else None)
                    
                    if is_success_mock:
                        successful += 1
                        total_screenshots += screenshots_count_mock
                    else:
                        failed += 1
                    
                    page.close()
                    context.close()
                    time.sleep(random.uniform(self.safety_settings['min_delay'], self.safety_settings['max_delay']))

            finally:
                browser.close()
        
        total_time = time.time() - start_time
        result = {
            "status": "success", "processed_urls": total_urls, "successful": successful,
            "failed": failed, "total_screenshots": total_screenshots,
            "processing_time_minutes": round(total_time / 60, 1)
        }
        
        self._update_status(f"🏁 Обработка завершена. Успешно: {successful}, Ошибки: {failed}", 100)
        return result

# if __name__ == "__main__":
#     collector = RenderScreenshotCollector()
#     collector.run()