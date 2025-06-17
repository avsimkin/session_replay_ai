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
        
        # Статистика работы
        self.start_time = None
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        
        self._update_status("🔐 Настраиваем подключения...", 1)
        self._init_clients()

    def _update_status(self, details: str, progress: int):
        if self.status_callback: 
            self.status_callback(details, progress)
        if progress != -1: 
            print(f"[{progress}%] {details}")
        else:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] {details}")

    def _init_clients(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=[
                    "https://www.googleapis.com/auth/bigquery",
                    "https://www.googleapis.com/auth/drive"
                ]
            )
            self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project_id)
            self.drive_service = build('drive', 'v3', credentials=credentials)
            self._update_status("✅ Google Cloud подключен", 5)
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к Google Cloud: {e}")

    def get_processed_sessions(self):
        """Получить список обработанных сессий из BigQuery, которые еще НЕ обработаны OCR"""
        query = f"""
        SELECT 
            s.session_replay_url, 
            s.amplitude_id, 
            s.session_replay_id, 
            s.duration_seconds, 
            s.events_count, 
            s.record_date
        FROM `{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_source_table}` s
        LEFT JOIN `{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_target_table}` t 
            ON s.session_replay_id = t.session_id
        WHERE s.is_processed = TRUE 
            AND t.session_id IS NULL  -- Исключаем уже обработанные OCR
        """
        
        self._update_status("🔍 Получаем необработанные сессии из BigQuery...", 10)
        
        try:
            result = self.bq_client.query(query).result()
            sessions = []
            for row in result:
                sessions.append({
                    'session_replay_url': row.session_replay_url,
                    'amplitude_id': row.amplitude_id,
                    'session_replay_id': row.session_replay_id,
                    'duration_seconds': row.duration_seconds,
                    'events_count': row.events_count,
                    'record_date': row.record_date.strftime('%Y-%m-%d')
                })
            self._update_status(f"📊 Найдено НЕобработанных OCR сессий: {len(sessions)}", 15)
            return sessions
        except Exception as e:
            self._update_status(f"❌ Ошибка получения сессий: {e}", -1)
            raise

    def find_zip_for_session(self, session_id):
        """Найти ZIP-архив для session_id в папке Google Drive"""
        query = f"'{self.gdrive_folder_id}' in parents and name contains '{session_id}' and name contains '.zip'"
        
        try:
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name)",
                pageSize=10
            ).execute()
            files = results.get('files', [])
            
            if files:
                self._update_status(f"  🔎 Найден архив: {files[0]['name']}", -1)
                return files[0]
            
            self._update_status("  ❌ Архив не найден!", -1)
            return None
        except Exception as e:
            self._update_status(f"  ❌ Ошибка поиска архива: {e}", -1)
            return None

    def get_zipfile_from_drive(self, file_id):
        """Скачать ZIP-архив с Google Drive в память"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            fh.seek(0)
            return zipfile.ZipFile(fh)
        except Exception as e:
            self._update_status(f"❌ Ошибка скачивания архива: {e}", -1)
            raise

    def parse_userinfo_text(self, text):
        """Парсинг userinfo из OCR текста"""
        res = {
            'user_id': '',
            'country': '',
            'session_length': '',
            'event_total': '',
            'device_type': ''
        }
        
        # Паттерны для user_id
        user_id_patterns = [
            r'\b([A-Z]{2}[0-9]{6})\b',
            r'\b([A-Z]{2,3}[0-9]{5,7})\b',
            r'\b([A-Z]{4}[0-9]{4,8})\b',
            r'\b([0-9]{10,15})\b',
            r'\b([A-Z]+[0-9]+)\b',
            r'^([A-Z0-9]{6,15})$',
        ]
        
        for pattern in user_id_patterns:
            matches = re.findall(pattern, text, re.MULTILINE)
            valid_matches = [m for m in matches if m.lower() not in ['android', 'session', 'length', 'total', 'event', 'device']]
            if valid_matches:
                res['user_id'] = valid_matches[0]
                break
        
        # Fallback для user_id
        if not res['user_id']:
            lines = text.split('\n')
            for line in lines:
                line = line.strip()
                if (6 <= len(line) <= 15 and
                        re.match(r'^[A-Z0-9]+$', line) and
                        any(c.isdigit() for c in line) and
                        any(c.isalpha() for c in line)):
                    res['user_id'] = line
                    break
        
        # Поиск стран
        country_patterns = {
            'Spain': ['spain', 'españa'],
            'Costa Rica': ['costa rica', 'costa'],
            'Russia': ['russia', 'russian', 'рф'],
            'Peru': ['peru', 'perú'],
            'Bolivia': ['bolivia'],
            'Ecuador': ['ecuador'],
            'Netherlands': ['netherlands', 'holland'],
            'Germany': ['germany', 'deutschland'],
            'France': ['france', 'francia']
        }
        
        combined_lower = text.lower()
        for country, patterns in country_patterns.items():
            for pattern in patterns:
                if pattern in combined_lower:
                    res['country'] = country
                    break
            if res['country']:
                break
        
        # Поиск длительности сессии
        session_patterns = [
            r'(\d+h\s*\d+m)',
            r'(\d+m\s*\d+s)',
            r'(\d+h)',
            r'(\d+m)',
            r'(\d+s)'
        ]
        for pattern in session_patterns:
            match = re.search(pattern, text)
            if match:
                res['session_length'] = match.group(1)
                break
        
        # Поиск количества событий
        event_match = re.search(r'(?:event\s*total|total)\s*:?\s*(\d+)', text, re.IGNORECASE)
        if event_match:
            res['event_total'] = event_match.group(1)
        else:
            numbers = re.findall(r'\b(\d{1,2})\b', text)
            for num in numbers:
                if 0 < int(num) < 100:
                    res['event_total'] = num
                    break
        
        # Поиск типа устройства
        devices = ['android', 'iphone', 'ios', 'windows', 'mac', 'linux']
        for device in devices:
            if device in combined_lower:
                res['device_type'] = device.capitalize()
                break
        
        return res

    def process_zip_session(self, session, zip_file):
        """Обработка одной сессии: OCR всех PNG в архиве"""
        data = {
            'session_id': session['session_replay_id'],
            'amplitude_id': session['amplitude_id'],
            'session_replay_url': session['session_replay_url'],
            'duration_seconds': session['duration_seconds'],
            'events_count': session['events_count'],
            'record_date': session['record_date'],
            'user_id': '',
            'country': '',
            'session_length': '',
            'event_total': '',
            'device_type': '',
            'summary': '',
            'sentiment': '',
            'actions': ''
        }
        
        screenshots_count = 0
        
        try:
            for fname in zip_file.namelist():
                if fname.lower().endswith('.png'):
                    screenshots_count += 1
                    
                    with zip_file.open(fname) as file:
                        img = Image.open(file)
                        
                        # OCR с обработкой ошибок
                        try:
                            # Проверяем доступность tesseract
                            if not hasattr(pytesseract, '_tesseract_cmd_exists'):
                                try:
                                    pytesseract.get_tesseract_version()
                                    pytesseract._tesseract_cmd_exists = True
                                except:
                                    pytesseract._tesseract_cmd_exists = False
                            
                            if not pytesseract._tesseract_cmd_exists:
                                self._update_status("⚠️ Tesseract недоступен, пропускаем OCR", -1)
                                continue
                                
                            if 'userinfo' in fname:
                                text = pytesseract.image_to_string(img, lang='eng')
                                userinfo = self.parse_userinfo_text(text)
                                data.update(userinfo)
                            elif 'summary' in fname:
                                data['summary'] = pytesseract.image_to_string(img, lang='eng')
                            elif 'sentiment' in fname:
                                data['sentiment'] = pytesseract.image_to_string(img, lang='eng')
                            elif 'actions' in fname:
                                data['actions'] = pytesseract.image_to_string(img, lang='eng')
                        except Exception as ocr_error:
                            self._update_status(f"⚠️ OCR ошибка для {fname}: {ocr_error}", -1)
                            continue
                            
        except Exception as e:
            self._update_status(f"❌ Ошибка обработки архива: {e}", -1)
            raise
            
        return data, screenshots_count

    def update_session_status_in_bq(self, session_replay_url, processed_datetime, screenshots_count, drive_folder_id):
        """Обновить статус сессии в BigQuery"""
        table_id = f"{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_source_table}"
        update_query = f"""
        UPDATE `{table_id}`
        SET
            processed_datetime = @processed_datetime,
            screenshots_count = @screenshots_count,
            drive_folder_id = @drive_folder_id
        WHERE session_replay_url = @session_replay_url
        """
        
        try:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("processed_datetime", "STRING", processed_datetime),
                    bigquery.ScalarQueryParameter("screenshots_count", "INT64", screenshots_count),
                    bigquery.ScalarQueryParameter("drive_folder_id", "STRING", drive_folder_id),
                    bigquery.ScalarQueryParameter("session_replay_url", "STRING", session_replay_url)
                ]
            )
            self.bq_client.query(update_query, job_config=job_config).result()
            self._update_status(f"✅ Обновлен статус для {session_replay_url}", -1)
        except Exception as e:
            self._update_status(f"❌ Ошибка обновления статуса: {e}", -1)

    def upload_to_bigquery(self, rows):
        """Загрузить данные в целевую таблицу BigQuery"""
        if not rows:
            self._update_status("⚠️ Нет данных для загрузки", -1)
            return

        try:
            df = pd.DataFrame(rows)
            table_id = f"{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_target_table}"
            
            job = self.bq_client.load_table_from_dataframe(df, table_id)
            job.result()
            self._update_status(f"✅ Загружено {len(df)} строк в {table_id}", -1)
        except Exception as e:
            self._update_status(f"❌ Ошибка загрузки в BigQuery: {e}", -1)
            raise

    def run(self):
        """Основной метод обработки OCR"""
        self.start_time = datetime.now()
        
        self._update_status("🔄 ЗАПУСК ОБРАБОТКИ OCR ТЕКСТА", 20)
        
        # Получаем сессии для обработки
        sessions = self.get_processed_sessions()
        
        if not sessions:
            self._update_status("✅ Все сессии уже обработаны OCR!", 100)
            return {"status": "no_sessions", "message": "Нет сессий для OCR обработки"}

        self._update_status(f"📋 Начинаем обработку {len(sessions)} сессий", 25)
        
        all_data = []
        batch_size = 50
        
        try:
            for i, session in enumerate(sessions, 1):
                progress = 25 + int((i / len(sessions)) * 70)
                self._update_status(f"▶️ [{i}/{len(sessions)}] Обрабатываем сессию: {session['session_replay_id']}", progress)

                # Поиск архива
                zip_file_info = self.find_zip_for_session(session['session_replay_id'])
                if not zip_file_info:
                    self.total_failed += 1
                    continue

                try:
                    # Скачивание и обработка
                    zip_file = self.get_zipfile_from_drive(zip_file_info['id'])
                    row, screenshots_count = self.process_zip_session(session, zip_file)
                    all_data.append(row)

                    # Обновление статуса в источнике
                    processed_datetime = datetime.utcnow().isoformat()
                    self.update_session_status_in_bq(
                        session['session_replay_url'],
                        processed_datetime,
                        screenshots_count,
                        zip_file_info['id']
                    )

                    self.total_successful += 1
                    self._update_status(f"✅ Сессия {session['session_replay_id']} обработана OCR", -1)

                    # Сохранение батчами
                    if len(all_data) >= batch_size:
                        self.upload_to_bigquery(all_data)
                        self._update_status(f"💾 Сохранен батч из {len(all_data)} сессий", -1)
                        all_data = []

                except Exception as e:
                    self.total_failed += 1
                    self._update_status(f"❌ Ошибка обработки сессии {session['session_replay_id']}: {e}", -1)
                    continue
                
                self.total_processed += 1

            # Сохранение остатка
            if all_data:
                self.upload_to_bigquery(all_data)
                self._update_status(f"💾 Сохранен финальный батч из {len(all_data)} сессий", -1)

        except Exception as e:
            self._update_status(f"❌ Критическая ошибка в OCR обработке: {e}", -1)
            raise

        # Финальная статистика
        total_time = datetime.now() - self.start_time
        result = {
            "status": "completed",
            "total_processed": self.total_processed,
            "total_successful": self.total_successful,
            "total_failed": self.total_failed,
            "success_rate": f"{(self.total_successful/self.total_processed*100):.1f}%" if self.total_processed > 0 else "0%",
            "total_time_minutes": round(total_time.total_seconds() / 60, 1)
        }

        self._update_status(f"🏁 OCR ОБРАБОТКА ЗАВЕРШЕНА!", 100)
        self._update_status(f"📊 Обработано: {self.total_processed}, Успешно: {self.total_successful}, Ошибок: {self.total_failed}", 100)
        
        return result


def main():
    """Основная функция для запуска OCR обработки"""
    try:
        def console_status_callback(details: str, progress: int):
            if progress != -1:
                print(f"[{progress}%] {details}")
            else:
                print(f"[INFO] {details}")

        processor = TextExtractionProcessor(status_callback=console_status_callback)
        result = processor.run()
        
        print(f"\n🏁 Финальный результат: {result}")
        return result

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    main()