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
import pandas_gbq
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


def clean_summary(text):
    """Очищает summary от технического мусора"""
    if pd.isna(text) or text == 'null' or not text:
        return ''

    lines = text.split('\n')
    clean_lines = []
    found_content = False

    for line in lines:
        line = line.strip()
        if not line:
            continue

        technical_patterns = [
            r'^[A-Za-z0-9]{6,20}$', r'^[0-9,.\s<an>]+$', r'^[0-9]+\.$',
            r'Session Length', r'Device Type', r'Event Total', r'Amplitude AI',
            r'ReplaySummary', r'Events \+\}', r'Powered by',
            r'^[0-9]+[hms]\s*[0-9]*\s*(Android|iOS|Windows|Apple iPhone)$',
            r'^[0-9]+\s+(Russia|France|Spain|Germany)', r'^\d+[hms]\s+\d+$',
            r'^.*Russia.*<[an]$', r'^.*Android$', r'^.*Windows$', r'^.*Apple iPhone$',
            r'^Summary\s*$', r'^[A-Z]{2,5}\s*$', r'^\d{1,3}$', r'^[*%+\}\s]+$',
            r'^Recent\s*v\s*Favorites', r'^Q\s*Search',
        ]

        is_technical = any(re.search(pattern, line, re.IGNORECASE) for pattern in technical_patterns)

        if not is_technical:
            if len(line) > 20 and re.search(r'[a-zA-Z].*[a-zA-Z]', line):
                found_content = True
                clean_lines.append(line)
            elif found_content:
                clean_lines.append(line)

    if not clean_lines:
        return ''

    content = ' '.join(clean_lines)
    highlights_match = re.search(r'(.+?)(?:\n\s*Highlights|\s+Highlights)', content, re.DOTALL)
    if highlights_match:
        clean_text = highlights_match.group(1).strip()
    else:
        clean_text = content.strip()

    artifacts_to_remove = [r'\s*\*%\s*$', r'\s*\+\}\s*$', r'\s*GJ!\s*$', r'\s*iG!\s*$', 
                          r'\s*AT\s*$', r'\s*AC\s*$', r'\s*NS\s*$', r'\s*[*%+\}]{1,3}\s*$']
    
    for pattern in artifacts_to_remove:
        clean_text = re.sub(pattern, '', clean_text)

    clean_text = re.sub(r'\s+', ' ', clean_text)
    return clean_text.strip() if clean_text.strip() else ''


def clean_sentiment(text):
    """Очищает sentiment от технического мусора"""
    if pd.isna(text) or text == 'null' or not text:
        return ''

    text = re.sub(r'^\s*Sentiment\s*\n*', '', text, flags=re.IGNORECASE)
    lines = text.split('\n')
    clean_lines = []
    found_content = False

    for line in lines:
        line = line.strip()
        if not line:
            continue

        technical_patterns = [r'^[A-Za-z0-9]{6,20}$', r'^Sentiment\s*$', r'^[A-Z]{2,5}\s*$', 
                            r'^\d{1,3}$', r'^[*%+\}\s]+$']
        is_technical = any(re.search(pattern, line, re.IGNORECASE) for pattern in technical_patterns)

        if not is_technical:
            if (len(line) > 15 and 
                re.search(r'(user|demonstrated|appeared|showed|felt|experienced)', line, re.IGNORECASE)):
                found_content = True
                clean_lines.append(line)
            elif found_content and len(line) > 10:
                clean_lines.append(line)

    if clean_lines:
        content = ' '.join(clean_lines)
    else:
        content = text.strip()

    for pattern in [r'\s*\*%\s*$', r'\s*\+\}\s*$', r'\s*[*%+\}]{1,3}\s*$']:
        content = re.sub(pattern, '', content)
    
    content = re.sub(r'\s+', ' ', content)
    return content.strip()


def clean_actions(text):
    """Очищает actions от технического мусора"""
    if pd.isna(text) or text == 'null' or not text:
        return ''

    text = re.sub(r'^\s*(?:Actions?|ActIONS?|AC)\s*\n*', '', text, flags=re.IGNORECASE)
    lines = text.split('\n')
    action_lines = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        technical_patterns = [r'^[A-Za-z0-9]{6,20}$', r'^Actions?\s*$', r'^ActIONS?\s*$', 
                            r'^AC\s*$', r'^[A-Z]{2,5}\s*$', r'^\d{1,3}$', r'^[*%+\}\s]+$',
                            r'^Recent\s*v\s*Favorites', r'^Q\s*Search']
        is_technical = any(re.search(pattern, line, re.IGNORECASE) for pattern in technical_patterns)

        if not is_technical:
            if (line.startswith(('-', '+', '•')) or 
                len(line) > 15 and
                re.search(r'(consider|provide|add|improve|investigate|enhance|create|implement)', line, re.IGNORECASE)):
                action_lines.append(line)

    if action_lines:
        content = '\n'.join(action_lines)
    else:
        content = text.strip()
        for pattern in [r'^Recent.*?\n', r'^Q\s*Search.*?\n']:
            content = re.sub(pattern, '', content, flags=re.MULTILINE)

    for pattern in [r'\s*\*%\s*$', r'\s*\+\}\s*$', r'\s*[*%+\}]{1,3}\s*$']:
        content = re.sub(pattern, '', content)

    content = re.sub(r'[ \t]+', ' ', content)
    content = re.sub(r'\n\s*\n', '\n', content)
    return content.strip()


def parse_userinfo_text(text):
    """Парсинг userinfo из текста"""
    res = {'user_id': '', 'country': '', 'session_length': '', 'event_total': '', 'device_type': ''}
    
    # Парсинг user_id
    user_id_patterns = [r'\b([A-Z]{2}[0-9]{6})\b', r'\b([A-Z]{2,3}[0-9]{5,7})\b', 
                       r'\b([A-Z]{4}[0-9]{4,8})\b', r'\b([0-9]{10,15})\b', r'\b([A-Z]+[0-9]+)\b']
    for pattern in user_id_patterns:
        matches = re.findall(pattern, text, re.MULTILINE)
        valid_matches = [m for m in matches if m.lower() not in ['android', 'session', 'length', 'total', 'event', 'device']]
        if valid_matches:
            res['user_id'] = valid_matches[0]
            break
    
    # Парсинг country
    country_patterns = {
        'Spain': ['spain', 'españa'], 'Costa Rica': ['costa rica', 'costa'],
        'Russia': ['russia', 'russian', 'рф'], 'Peru': ['peru', 'perú'],
        'Bolivia': ['bolivia'], 'Ecuador': ['ecuador'], 'Netherlands': ['netherlands', 'holland'],
        'Germany': ['germany', 'deutschland'], 'France': ['france', 'francia']
    }
    combined_lower = text.lower()
    for country, patterns in country_patterns.items():
        for pattern in patterns:
            if pattern in combined_lower:
                res['country'] = country
                break
        if res['country']:
            break
    
    # Парсинг session_length
    for pattern in [r'(\d+h\s*\d+m)', r'(\d+m\s*\d+s)', r'(\d+h)', r'(\d+m)', r'(\d+s)']:
        match = re.search(pattern, text)
        if match:
            res['session_length'] = match.group(1)
            break
    
    # Парсинг event_total
    event_match = re.search(r'(?:event\s*total|total)\s*:?\s*(\d+)', text, re.IGNORECASE)
    if event_match:
        res['event_total'] = event_match.group(1)
    
    # Парсинг device_type
    devices = ['android', 'iphone', 'ios', 'windows', 'mac', 'linux']
    for device in devices:
        if device in combined_lower:
            res['device_type'] = device.capitalize()
            break
    
    return res


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
        
        # Добавлены недостающие атрибуты
        self.start_time = None
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        self.tesseract_available = True
        
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
        """ОБНОВЛЕНО: Полная схема + очистка + userinfo"""
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
        raw_data = {'summary': '', 'sentiment': '', 'actions': ''}
        
        try:
            for fname in zip_file.namelist():
                if fname.lower().endswith('.png'):
                    screenshots_count += 1
                    if not self.tesseract_available: 
                        continue
                    with zip_file.open(fname) as file:
                        img = Image.open(file)
                        text = pytesseract.image_to_string(img, lang='eng')
                        
                        if 'userinfo' in fname.lower():
                            userinfo = parse_userinfo_text(text)
                            data.update(userinfo)
                        elif 'summary' in fname.lower():
                            raw_data['summary'] = text
                            data['summary'] = clean_summary(text)
                        elif 'sentiment' in fname.lower():
                            raw_data['sentiment'] = text
                            data['sentiment'] = clean_sentiment(text)
                        elif 'actions' in fname.lower():
                            raw_data['actions'] = text
                            data['actions'] = clean_actions(text)
        except Exception as e:
            self._update_status(f"❌ Ошибка обработки архива: {e}", -1)
        
        # Логирование очистки
        cleaning_stats = []
        for field in ['summary', 'sentiment', 'actions']:
            if raw_data[field] and data[field] != raw_data[field]:
                original_len = len(raw_data[field])
                cleaned_len = len(data[field])
                compression = round((1 - cleaned_len / original_len) * 100) if original_len > 0 else 0
                cleaning_stats.append(f"{field.title()}: {original_len}→{cleaned_len} (-{compression}%)")
        
        if cleaning_stats:
            self._update_status(f"🧹 Очищено: {', '.join(cleaning_stats)}", -1)
        
        return data, screenshots_count

    def update_session_status_in_bq(self, session_replay_url, screenshots_count, drive_file_id):
        """ОБНОВЛЕНО: Добавляем processed_datetime"""
        processed_datetime = datetime.utcnow().isoformat()
        
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
            job_config = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("processed_datetime", "STRING", processed_datetime),
                bigquery.ScalarQueryParameter("screenshots_count", "INT64", screenshots_count),
                bigquery.ScalarQueryParameter("drive_folder_id", "STRING", drive_file_id),
                bigquery.ScalarQueryParameter("session_replay_url", "STRING", session_replay_url)
            ])
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
        """ОБНОВЛЕНО: MERGE вместо append + дедупликация"""
        if not rows:
            return
        
        # Дедупликация по session_id
        seen_sessions = set()
        unique_rows = []
        duplicates_found = 0
        
        for row in rows:
            session_id = row.get('session_id')
            if session_id not in seen_sessions:
                seen_sessions.add(session_id)
                unique_rows.append(row)
            else:
                duplicates_found += 1
        
        if duplicates_found > 0:
            self._update_status(f"🧹 Убрано дубликатов: {duplicates_found}", -1)
        
        try:
            df = pd.DataFrame(unique_rows)
            
            if 'record_date' in df.columns:
                df['record_date'] = pd.to_datetime(df['record_date'], errors='coerce')
                df.dropna(subset=['record_date'], inplace=True)

            if df.empty:
                self._update_status("ℹ️ Нет корректных данных для загрузки", -1)
                return

            # Используем MERGE вместо простого append
            temp_table_id = f"{self.bq_dataset_id}.temp_ocr_batch_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
            target_table_id = f"{self.bq_dataset_id}.{self.bq_target_table}"
            
            # Загружаем во временную таблицу
            job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            job = self.bq_client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
            job.result()
            
            # MERGE запрос
            merge_query = f"""
            MERGE `{target_table_id}` T
            USING `{temp_table_id}` S
            ON T.session_id = S.session_id
            WHEN MATCHED THEN
              UPDATE SET
                amplitude_id = S.amplitude_id,
                session_replay_url = S.session_replay_url,
                duration_seconds = S.duration_seconds,
                events_count = S.events_count,
                record_date = S.record_date,
                user_id = S.user_id,
                country = S.country,
                session_length = S.session_length,
                event_total = S.event_total,
                device_type = S.device_type,
                summary = S.summary,
                sentiment = S.sentiment,
                actions = S.actions
            WHEN NOT MATCHED THEN
              INSERT (
                session_id, amplitude_id, session_replay_url, duration_seconds, 
                events_count, record_date, user_id, country, session_length, 
                event_total, device_type, summary, sentiment, actions
              )
              VALUES (
                S.session_id, S.amplitude_id, S.session_replay_url, S.duration_seconds,
                S.events_count, S.record_date, S.user_id, S.country, S.session_length,
                S.event_total, S.device_type, S.summary, S.sentiment, S.actions
              )
            """
            
            merge_job = self.bq_client.query(merge_query)
            merge_job.result()
            
            # Удаляем временную таблицу
            self.bq_client.delete_table(temp_table_id)
            
            self._update_status(f"💾 MERGE завершен: {len(df)} записей обработано", -1)

        except Exception as e:
            import traceback
            self._update_status(f"❌ Ошибка загрузки в BigQuery: {e}", -1)
            print(f"🔍 Трейсбек: {traceback.format_exc()}")

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
                # ✅ ДОБАВЛЕНО: Обновляем статус в session_replay_urls
                self.update_session_status_in_bq(session['session_replay_url'], screenshots_count, zip_file_info['id'])
                self.total_successful += 1
            except Exception as e:
                self.total_failed += 1
                self._update_status(f"❌ Ошибка обработки сессии {session['session_replay_id']}: {e}", -1)
            
            self.total_processed += 1
            if len(all_data) >= self.save_frequency:
                self.upload_to_bigquery(all_data)
                all_data = []

        if all_data: 
            self.upload_to_bigquery(all_data)

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