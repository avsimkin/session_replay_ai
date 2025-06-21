import io
import re
import zipfile
import pytesseract
from PIL import Image
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pandas as pd
from datetime import datetime
import os
import sys
from typing import Callable, Optional

# Добавляем путь к корню проекта для импорта config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config.settings import settings
except ImportError:
    class MockSettings:
        GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '/etc/secrets/bigquery-credentials.json')
        GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID', '1K8cbFU2gYpvP3PiHwOOHS1KREqdj6fQX')
        BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'codellon-dwh')
        BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'amplitude_session_replay')
        BQ_SOURCE_TABLE = os.environ.get('BQ_SOURCE_TABLE', 'session_replay_urls')
        BQ_TARGET_TABLE = os.environ.get('BQ_TARGET_TABLE', 'replay_text_complete')
    settings = MockSettings()

class TextExtractionProcessor:
    def __init__(self, status_callback: Optional[Callable[[str, int], None]] = None):
        self.status_callback = status_callback
        self.credentials_path = settings.GOOGLE_APPLICATION_CREDENTIALS
        self.gdrive_folder_id = settings.GDRIVE_FOLDER_ID
        self.bq_project_id = settings.BQ_PROJECT_ID
        self.bq_dataset_id = settings.BQ_DATASET_ID
        self.bq_source_table = settings.BQ_SOURCE_TABLE
        self.bq_target_table = settings.BQ_TARGET_TABLE
        
        self.batch_size = int(os.environ.get('OCR_BATCH_SIZE', '20'))
        self.max_runtime_minutes = int(os.environ.get('OCR_MAX_RUNTIME_MINUTES', '25'))
        self.save_frequency = int(os.environ.get('OCR_SAVE_FREQUENCY', '5'))
        
        # --- ИСПРАВЛЕНИЕ: Добавлены недостающие атрибуты ---
        self.start_time = None
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        self.tesseract_available = True # По умолчанию считаем, что Tesseract доступен
        # ----------------------------------------------------
        
        self._update_status("🔐 Настраиваем подключения...", 1)
        self._init_clients()

    def _update_status(self, details: str, progress: int):
        if self.status_callback: 
            self.status_callback(details, progress)
        timestamp = datetime.now().strftime("%H:%M:%S")
        if progress != -1: 
            print(f"[{timestamp}][{progress}%] {details}")
        else:
            print(f"[{timestamp}] {details}")

    def _init_clients(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive"]
            )
            self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project_id)
            self.drive_service = build('drive', 'v3', credentials=credentials)
            self._setup_tesseract()
            self._update_status("✅ Google Cloud подключен", 5)
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к Google Cloud: {e}")

    def _setup_tesseract(self):
        try:
            tesseract_cmd = os.environ.get('TESSERACT_CMD')
            if tesseract_cmd and os.path.exists(tesseract_cmd):
                pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
                self._update_status(f"✅ Tesseract найден: {tesseract_cmd}", -1)
                return
            
            # Проверяем стандартные пути
            for path in ['/usr/bin/tesseract', '/usr/local/bin/tesseract', '/opt/homebrew/bin/tesseract', 'tesseract']:
                try:
                    if path == 'tesseract':
                        pytesseract.get_tesseract_version()
                        self._update_status("✅ Tesseract найден в PATH", -1)
                        return
                    elif os.path.exists(path):
                        pytesseract.pytesseract.tesseract_cmd = path
                        pytesseract.get_tesseract_version()
                        self._update_status(f"✅ Tesseract найден: {path}", -1)
                        return
                except:
                    continue
            
            self._update_status("⚠️ Tesseract не найден - OCR будет пропущен", -1)
            self.tesseract_available = False
            
        except Exception as e:
            self._update_status(f"⚠️ Ошибка настройки Tesseract: {e}", -1)
            self.tesseract_available = False

    def get_processed_sessions(self, limit=None):
        if limit is None: limit = self.batch_size * 10
        query = f"""
        SELECT s.session_replay_url, s.amplitude_id, s.session_replay_id, s.duration_seconds, s.events_count, s.record_date
        FROM `{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_source_table}` s
        LEFT JOIN `{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_target_table}` t ON s.session_replay_id = t.session_id
        WHERE s.is_processed = TRUE AND t.session_id IS NULL
        ORDER BY s.record_date DESC LIMIT {limit}"""
        self._update_status("🔍 Получаем необработанные сессии из BigQuery...", 10)
        try:
            sessions = [dict(row) for row in self.bq_client.query(query).result()]
            self._update_status(f"📊 Найдено НЕобработанных OCR сессий: {len(sessions)}", 15)
            return sessions
        except Exception as e:
            self._update_status(f"❌ Ошибка получения сессий: {e}", -1)
            raise

    def find_zip_for_session(self, session_id):
        search_patterns = [f"name contains '{session_id}' and name contains '.zip' and '{self.gdrive_folder_id}' in parents"]
        for i, query in enumerate(search_patterns):
            try:
                results = self.drive_service.files().list(q=query, fields="files(id, name)", pageSize=1).execute()
                files = results.get('files', [])
                if files:
                    self._update_status(f"  🔎 Найден архив (попытка {i+1}): {files[0]['name']}", -1)
                    return files[0]
            except Exception as e:
                self._update_status(f"  ⚠️ Ошибка поиска (попытка {i+1}): {e}", -1)
        self._update_status("  ❌ Архив не найден!", -1)
        return None

    def get_zipfile_from_drive(self, file_id):
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done: status, done = downloader.next_chunk()
            fh.seek(0)
            return zipfile.ZipFile(fh)
        except Exception as e:
            self._update_status(f"❌ Ошибка скачивания архива: {e}", -1)
            raise

    def process_zip_session(self, session, zip_file):
        data = {'session_id': session['session_replay_id'], 'amplitude_id': session['amplitude_id'], **session}
        screenshots_count = 0
        try:
            for fname in zip_file.namelist():
                if fname.lower().endswith('.png'):
                    screenshots_count += 1
                    if not self.tesseract_available: continue
                    with zip_file.open(fname) as file:
                        img = Image.open(file)
                        text = pytesseract.image_to_string(img, lang='eng')
                        if 'summary' in fname: data['summary'] = text
                        elif 'sentiment' in fname: data['sentiment'] = text
                        elif 'actions' in fname: data['actions'] = text
        except Exception as e:
            self._update_status(f"❌ Ошибка обработки архива: {e}", -1)
        return data, screenshots_count

    def update_session_status_in_bq(self, session_replay_url, screenshots_count, drive_file_id):
        table_id = f"{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_source_table}"
        update_query = f"UPDATE `{table_id}` SET screenshots_count = @screenshots_count, drive_folder_id = @drive_folder_id WHERE session_replay_url = @session_replay_url"
        try:
            job_config = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("screenshots_count", "INT64", screenshots_count),
                bigquery.ScalarQueryParameter("drive_folder_id", "STRING", drive_file_id),
                bigquery.ScalarQueryParameter("session_replay_url", "STRING", session_replay_url)])
            self.bq_client.query(update_query, job_config=job_config).result()
        except Exception as e:
            self._update_status(f"❌ Ошибка обновления статуса: {e}", -1)
        
    def check_runtime_limit(self):
        if self.start_time:
            elapsed_minutes = (datetime.now() - self.start_time).total_seconds() / 60
            if elapsed_minutes >= self.max_runtime_minutes:
                self._update_status(f"⏰ Достигнут лимит времени работы ({self.max_runtime_minutes} мин)", -1)
                return True
        return False

    def upload_to_bigquery(self, rows):
        if not rows:
            return
        try:
            # --- ДИАГНОСТИЧЕСКИЙ PRINT ---
            # Проверяем первые 5 записей на наличие и тип record_date
            print("--- Проверка данных перед загрузкой в BigQuery ---")
            for i, r in enumerate(rows[:5]): # Смотрим только первые 5, чтобы не засорять лог
                if 'record_date' in r:
                    print(f"Запись {i}: record_date={r['record_date']}, тип={type(r['record_date'])}")
                else:
                    print(f"Запись {i}: КЛЮЧ 'record_date' ОТСУТСТВУЕТ!")
            print("-------------------------------------------------")
            # -----------------------------

            df = pd.DataFrame(rows)
            if 'record_date' in df.columns:
                df['record_date'] = pd.to_datetime(df['record_date'], errors='coerce')
                df.dropna(subset=['record_date'], inplace=True)

            if df.empty:
                self._update_status("ℹ️ Нет корректных данных для загрузки после очистки дат.", -1)
                return

            table_id = f"{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_target_table}"
            job = self.bq_client.load_table_from_dataframe(df, table_id)
            job.result()
            self._update_status(f"💾 Сохранен батч из {len(df)} сессий в {table_id}", -1)
        except Exception as e:
            self._update_status(f"❌ Ошибка загрузки в BigQuery: {e}", -1)

    def run(self):
        self.start_time = datetime.now()
        self._update_status("🔄 ЗАПУСК ОБРАБОТКИ OCR ТЕКСТА", 20)
        sessions = self.get_processed_sessions()
        
        if not sessions:
            self._update_status("✅ Все сессии уже обработаны OCR!", 100)
            return {"status": "no_sessions", "message": "Нет сессий для OCR обработки"}

        self._update_status(f"📋 Начинаем обработку {len(sessions)} сессий (макс. {self.max_runtime_minutes} мин)", 25)
        all_data = []
        
        for i, session in enumerate(sessions, 1):
            if self.check_runtime_limit():
                self._update_status(f"⏰ Остановка по лимиту времени. Обработано: {i-1}/{len(sessions)}", -1)
                break
            
            progress = 25 + int((i / len(sessions)) * 70)
            self._update_status(f"▶️ [{i}/{len(sessions)}] Сессия: {session['session_replay_id']}", progress)

            zip_file_info = self.find_zip_for_session(session['session_replay_id'])
            if not zip_file_info:
                self.total_failed += 1
                continue

            try:
                zip_file = self.get_zipfile_from_drive(zip_file_info['id'])
                row, screenshots_count = self.process_zip_session(session, zip_file)
                all_data.append(row)
                self.update_session_status_in_bq(session['session_replay_url'], screenshots_count, zip_file_info['id'])
                self.total_successful += 1
            except Exception as e:
                self.total_failed += 1
                self._update_status(f"❌ Ошибка обработки сессии {session['session_replay_id']}: {e}", -1)
            
            self.total_processed += 1
            if len(all_data) >= self.save_frequency:
                self.upload_to_bigquery(all_data)
                all_data = []

        if all_data: self.upload_to_bigquery(all_data)

        total_time = datetime.now() - self.start_time
        result = {"status": "completed", "total_processed": self.total_processed, "successful": self.total_successful, "failed": self.total_failed}
        self._update_status(f"🏁 OCR ОБРАБОТКА ЗАВЕРШЕНА! Успешно: {self.total_successful}, Ошибки: {self.total_failed}", 100)
        return result

def main():
    try:
        processor = TextExtractionProcessor()
        processor.run()
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
