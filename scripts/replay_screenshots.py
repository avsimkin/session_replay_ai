# Имя файла: replay_screenshots.py
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
import zipfile
import multiprocessing
import queue

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
        BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'codellon-dwh')
        BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'amplitude_session_replay')
        BQ_TABLE_ID = os.environ.get('BQ_TABLE_ID', 'session_replay_urls')
    settings = MockSettings()

PROCESS_TIMEOUT_PER_URL = 240

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

def sanitize_cookies(cookies):
    if not cookies: return []
    valid_same_site_values = {"Strict", "Lax", "None"}
    sanitized_cookies = []
    for cookie in cookies:
        if cookie.get('sameSite') not in valid_same_site_values:
            original_value = cookie.get('sameSite', 'КЛЮЧ ОТСУТСТВОВАЛ')
            print(f"⚠️ Исправляю невалидный/отсутствующий sameSite='{original_value}' на 'Lax' для куки: {cookie.get('name')}")
            cookie['sameSite'] = 'Lax'
        sanitized_cookies.append(cookie)
    return sanitized_cookies

def worker_process_url(collector_config: dict, url_data: dict, result_queue: multiprocessing.Queue):
    try:
        collector = RenderScreenshotCollector(config_override=collector_config)
        sanitized_cookies = sanitize_cookies(collector.cookies)

        with sync_playwright() as p:
            browser_args = [
                '--no-proxy-server', '--disable-proxy-config-service', '--no-sandbox',
                '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage', '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
            browser = p.chromium.launch(headless=True, args=browser_args)
            context = browser.new_context(
                user_agent=random.choice(USER_AGENTS), viewport={'width': 1366, 'height': 768},
                locale='en-US', timezone_id='America/New_York'
            )
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
            context.add_cookies(sanitized_cookies)
            page = context.new_page()

            success, _ = collector.process_single_url(page, url_data)
            collector.mark_url_as_processed(url_data['url'], success)
            result_queue.put(success)

            page.close()
            context.close()
            browser.close()
    except Exception as e:
        print(f"❌ Критическая ошибка в дочернем процессе для URL {url_data.get('url')}: {e}")
        import traceback
        traceback.print_exc()
        result_queue.put(False)


class RenderScreenshotCollector:
    def __init__(self, status_callback: Optional[Callable[[str, int], None]] = None, config_override: Optional[dict] = None):
        if config_override:
            self.credentials_path = config_override["credentials_path"]
            self.gdrive_folder_id = config_override["gdrive_folder_id"]
            self.bq_project_id = config_override["bq_project_id"]
            self.bq_dataset_id = config_override["bq_dataset_id"]
            self.bq_table_id = config_override["bq_table_id"]
            self.min_duration_seconds = config_override["min_duration_seconds"]
            self.cookies_path = config_override["cookies_path"]
            self.status_callback = None
            self.cookies = self._load_cookies_from_secret_file(verbose=False)
        else:
            self.status_callback = status_callback
            self.cookies_path = "/etc/secrets/cookies.json"
            self.credentials_path = settings.GOOGLE_APPLICATION_CREDENTIALS
            self.gdrive_folder_id = settings.GDRIVE_FOLDER_ID
            self.bq_project_id = settings.BQ_PROJECT_ID
            self.bq_dataset_id = settings.BQ_DATASET_ID
            self.bq_table_id = settings.BQ_TABLE_ID
            self.batch_size = int(os.environ.get('BATCH_SIZE', '50'))
            self.pause_between_batches = int(os.environ.get('PAUSE_BETWEEN_BATCHES', '300'))
            self.max_runtime_hours = int(os.environ.get('MAX_RUNTIME_HOURS', '18'))
            self.min_duration_seconds = int(os.environ.get('MIN_DURATION_SECONDS', '20'))
            self.start_time = None
            self.total_processed, self.total_successful, self.total_failed, self.total_timeouts, self.batches_completed = 0, 0, 0, 0, 0
            self._update_status("🔐 Настраиваем подключения...", 1)
            self.cookies = self._load_cookies_from_secret_file()
        
        self.full_table_name = f"`{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table_id}`"
        self._init_bigquery()
        self._init_google_drive()

    def _update_status(self, details: str, progress: int):
        if self.status_callback: self.status_callback(details, progress)
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {details}")

    def _load_cookies_from_secret_file(self, verbose=True):
        if verbose: self._update_status(f"Загрузка cookies из {self.cookies_path}...", 2)
        if not os.path.exists(self.cookies_path):
            if verbose: self._update_status(f"❌ Файл cookies не найден!", 2)
            return []
        try:
            with open(self.cookies_path, 'r') as f: cookies = json.load(f)
            if verbose: self._update_status(f"✅ Cookies загружены ({len(cookies)} шт).", 3)
            return cookies
        except Exception as e:
            if verbose: self._update_status(f"❌ Ошибка чтения cookies: {e}", 3)
            return []

    def _init_bigquery(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path, scopes=["https://www.googleapis.com/auth/bigquery"])
            self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project_id)
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к BigQuery: {e}")

    def _init_google_drive(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path, scopes=['https://www.googleapis.com/auth/drive'])
            self.drive_service = build('drive', 'v3', credentials=credentials)
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к Google Drive: {e}")

    def get_unprocessed_urls(self, limit=None):
        query = f"SELECT session_replay_url, amplitude_id, session_replay_id, duration_seconds, events_count, record_date FROM {self.full_table_name} WHERE is_processed = FALSE AND duration_seconds >= {self.min_duration_seconds} ORDER BY record_date DESC"
        if limit: query += f"\nLIMIT {limit}"
        try:
            result = self.bq_client.query(query).result()
            urls_data = []
            for row in result:
                urls_data.append({
                    'url': row.session_replay_url, 'amplitude_id': row.amplitude_id,
                    'session_replay_id': row.session_replay_id, 'duration_seconds': row.duration_seconds,
                    'events_count': row.events_count,
                    'record_date': row.record_date.strftime('%Y-%m-%d') if hasattr(row.record_date, 'strftime') else str(row.record_date)
                })
            return urls_data
        except Exception as e:
            self._update_status(f"❌ Ошибка получения URL: {e}", -1)
            raise

    def mark_url_as_processed(self, url, success=True):
        try:
            update_query = f"UPDATE {self.full_table_name} SET is_processed = TRUE WHERE session_replay_url = @url"
            job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("url", "STRING", url)])
            self.bq_client.query(update_query, job_config=job_config).result()
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Ошибка обновления статуса URL {url}: {e}")

    def login_and_update_cookies(self, page):
        print("⚠️ Обнаружена страница входа. Попытка автоматической авторизации...")
        login = os.environ.get('AMPLITUDE_LOGIN')
        password = os.environ.get('AMPLITUDE_PASSWORD')
        if not login or not password:
            print("❌ Переменные окружения AMPLITUDE_LOGIN и/или AMPLITUDE_PASSWORD не установлены!")
            return False
        try:
            page.goto("https://app.amplitude.com/login", timeout=60000)
            print(f"    Вводим логин...")
            page.fill('input[name="username"]', login)
            page.click('button[type="submit"]')
            print("    Вводим пароль...")
            password_input = page.wait_for_selector('input[name="password"]', timeout=15000)
            password_input.fill(password)
            page.click('button[type="submit"]')
            print("    Ожидание успешного входа...")
            page.wait_for_url(lambda url: "login" not in url, timeout=60000)
            page.wait_for_selector("nav", timeout=30000)
            print("✅ Авторизация прошла успешно!")
            print("    Сохраняем новые cookies...")
            new_cookies = page.context.cookies()
            with open(self.cookies_path, 'w') as f:
                json.dump(new_cookies, f)
            print("✅ Cookies успешно обновлены!")
            self.cookies = new_cookies
            return True
        except Exception as e:
            print(f"❌ Ошибка во время автоматической авторизации: {e}")
            try:
                page.screenshot(path="login_error_screenshot.png", full_page=True)
                print("    Скриншот ошибки авторизации сохранен в login_error_screenshot.png")
            except: pass
            return False
    
    def get_session_id_from_url(self, url):
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        if "sessionReplayId=" in url:
            parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
            session_replay_id = parts[0]
            session_start_time = parts[1] if len(parts) > 1 else "unknown"
            return f"{session_replay_id}_{session_start_time}_{url_hash}"
        return f"no_session_id_{url_hash}"

    def screenshot_by_title(self, page, block_title, session_id, base_dir):
        print(f"🔍 Ищем блок '{block_title}'...")
        el = page.query_selector(f'h4:has-text("{block_title}")')
        if not el:
             print(f"❌ Блок '{block_title}' не найден!")
             return None
        try:
            img_path = os.path.join(base_dir, f"{session_id}_{block_title.lower()}.png")
            el.screenshot(path=img_path)
            print(f"✅ {block_title} скриншот сохранён")
            return img_path
        except Exception as e:
            print(f"❌ Ошибка создания скриншота {block_title}: {e}")
            return None

    def create_session_folder_structure(self, session_id, screenshots, url_data):
        session_dir = tempfile.mkdtemp(prefix=f"session_folder_{session_id}_")
        for screenshot_path in screenshots:
            if screenshot_path and os.path.exists(screenshot_path):
                shutil.copy2(screenshot_path, session_dir)
        metadata = {
            "session_id": session_id, "url": url_data['url'], "amplitude_id": url_data['amplitude_id'],
            # ... и другие метаданные
        }
        with open(os.path.join(session_dir, "metadata.json"), 'w') as f:
            json.dump(metadata, f, indent=2)
        return session_dir

    def upload_to_google_drive(self, file_path, filename, folder_id):
        try:
            media = MediaFileUpload(file_path, resumable=True)
            file = self.drive_service.files().create(body={'name': filename, 'parents': [folder_id]}, media_body=media, fields='id, name, webViewLink').execute()
            return file
        except Exception as e:
            print(f"❌ Ошибка загрузки в Google Drive: {e}")
            return None

    def create_and_upload_session_archive(self, session_dir, session_id, is_failure=False):
        try:
            prefix = "FAILURE" if is_failure else "session_replay"
            archive_name = f"{prefix}_{session_id}_{int(time.time())}.zip"
            archive_path = shutil.make_archive(archive_name.replace('.zip',''), 'zip', session_dir)
            print(f"📦 Создан архив: {archive_name}")
            uploaded_file = self.upload_to_google_drive(archive_path, archive_name, self.gdrive_folder_id)
            if uploaded_file: print(f"☁️ Архив загружен в Google Drive")
            return uploaded_file
        finally:
            shutil.rmtree(session_dir, ignore_errors=True)
            if 'archive_path' in locals() and os.path.exists(archive_path):
                os.remove(archive_path)

def process_single_url(self, page, url_data):
        url = url_data['url']
        session_id = self.get_session_id_from_url(url)
        temp_screenshots_dir = tempfile.mkdtemp(prefix=f"screenshots_{session_id}_")
        REQUIRED_BLOCKS = ['userinfo', 'summary', 'sentiment']
        screenshot_paths = []
        
        try:
            print(f"▶️ Обрабатываем сессию: {session_id}")
            
            # --- УЛУЧШЕНИЕ 1: БОЛЕЕ НАДЕЖНОЕ ОЖИДАНИЕ ЗАГРУЗКИ ---
            # Вместо domcontentloaded ждем, пока сетевая активность не прекратится
            page.goto(url, timeout=90000, wait_until='networkidle')
            print("    Страница загружена, даем 5-10 секунд на прогрузку интерфейса...")
            time.sleep(random.uniform(5, 10))

            if "/login" in page.url:
                login_successful = self.login_and_update_cookies(page)
                if not login_successful: return False, []
                print(f"    Возвращаемся к исходной ссылке...")
                page.goto(url, timeout=90000, wait_until='networkidle')
                time.sleep(random.uniform(5, 10))

            try:
                # --- УЛУЧШЕНИЕ 2: УВЕЛИЧИВАЕМ ТАЙМАУТ И НАДЕЖНОСТЬ КЛИКА ---
                print("    Ищем вкладку 'Summary'...")
                # Даем до 45 секунд на появление самого важного элемента
                summary_tab = page.wait_for_selector("text=Summary", timeout=45000)
                # Иногда простой клик не срабатывает, используем более надежный метод
                summary_tab.click(force=True, timeout=5000)
                print("    Клик на 'Summary' выполнен.")
                
                # --- УЛУЧШЕНИЕ 3: ЖДЕМ ПОЯВЛЕНИЯ КОНКРЕТНОГО ЭЛЕМЕНТА ПОСЛЕ КЛИКА ---
                print("    Ожидаем появления контента во вкладке...")
                # Ждем именно тот элемент, который не успевал прогрузиться
                summary_el = page.wait_for_selector('p.ltext-_uoww22', timeout=45000)
                print("    Контент 'Summary' обнаружен.")
                time.sleep(random.uniform(2, 4)) # Дополнительная пауза после клика

            except PlaywrightTimeoutError as e:
                print(f"❌ Не удалось найти или загрузить вкладку 'Summary' для сессии {session_id}")
                raise e # Вызываем ошибку, чтобы она была поймана ниже

            # --- Ваша оригинальная логика сбора скриншотов ---
            screenshot_results = {}
            print("📸 Начинаем создание скриншотов...")
            userinfo_path = self.screenshot_userinfo_block(page, session_id, temp_screenshots_dir)
            screenshot_results['userinfo'] = userinfo_path is not None
            if userinfo_path: screenshot_paths.append(userinfo_path)
            
            summary_paths = self.screenshot_summary_flexible(page, session_id, temp_screenshots_dir, summary_el=summary_el)
            screenshot_results['summary'] = len(summary_paths) > 0
            if summary_paths: screenshot_paths.extend(summary_paths)
            
            sentiment_path = self.screenshot_by_title(page, "Sentiment", session_id, temp_screenshots_dir)
            screenshot_results['sentiment'] = sentiment_path is not None
            if sentiment_path: screenshot_paths.append(sentiment_path)
            
            actions_path = self.screenshot_by_title(page, "Actions", session_id, temp_screenshots_dir)
            screenshot_results['actions'] = actions_path is not None
            if actions_path: screenshot_paths.append(actions_path)

            all_success = all(screenshot_results.get(block, False) for block in REQUIRED_BLOCKS)
            if not all_success or len(screenshot_paths) < 3:
                print(f"❌ Не собраны все обязательные блоки для {session_id}")
                return False, screenshot_paths
            
            session_dir, _ = self.create_session_folder_structure(session_id, screenshot_paths, url_data)
            uploaded_file = self.create_and_upload_session_archive(session_dir, session_id)
            return bool(uploaded_file), screenshot_paths
        
        except Exception as e:
            print(f"❌ Ошибка обработки URL {url}: {e}")
            failure_path = os.path.join(temp_screenshots_dir, f"FAILURE_screenshot.png")
            try:
                page.screenshot(path=failure_path, full_page=True, timeout=15000)
                print(f"    Скриншот ошибки сохранен.")
            except Exception as e_scr:
                print(f"    Не удалось сделать скриншот ошибки: {e_scr}")
            self.create_and_upload_session_archive(temp_screenshots_dir, session_id, is_failure=True)
            return False, []
        finally:
            shutil.rmtree(temp_screenshots_dir, ignore_errors=True)
            
    def process_batch(self, urls_batch):
        batch_start_time = time.time()
        batch_successful, batch_failed, batch_timeouts = 0, 0, 0
        self._update_status(f"🚀 Начинаем обработку батча из {len(urls_batch)} URL...", -1)
        result_queue = multiprocessing.Queue()
        collector_config = {
            "credentials_path": self.credentials_path, "gdrive_folder_id": self.gdrive_folder_id,
            "bq_project_id": self.bq_project_id, "bq_dataset_id": self.bq_dataset_id,
            "bq_table_id": self.bq_table_id, "min_duration_seconds": self.min_duration_seconds,
            "cookies_path": self.cookies_path 
        }
        for i, url_data in enumerate(urls_batch, 1):
            self._update_status(f"▶️ [{i}/{len(urls_batch)}] Запускаем процесс для URL ...{url_data['url'][-40:]}", -1)
            process = multiprocessing.Process(target=worker_process_url, args=(collector_config, url_data, result_queue))
            process.start()
            process.join(timeout=PROCESS_TIMEOUT_PER_URL)
            if process.is_alive():
                try:
                    self._update_status(f"❗ ТАЙМАУТ! Процесс для URL ...{url_data['url'][-40:]} завис. Завершаем.", -1)
                    process.terminate(); process.join(timeout=5)
                    if process.is_alive(): process.kill(); process.join(timeout=5)
                    batch_timeouts += 1; batch_failed += 1
                    self.mark_url_as_processed(url_data['url'], success=False)
                except Exception as e:
                    self._update_status(f"❌ ОШИБКА во время обработки таймаута! {e}", -1)
                    batch_failed += 1; batch_timeouts += 1
            else:
                try:
                    success = result_queue.get_nowait()
                    if success: batch_successful += 1
                    else: batch_failed += 1
                except queue.Empty:
                    batch_failed += 1; self.mark_url_as_processed(url_data['url'], success=False)
            if i < len(urls_batch):
                time.sleep(random.uniform(2, 5))
        self.total_processed += len(urls_batch)
        self.total_successful += batch_successful
        self.total_failed += batch_failed
        self.total_timeouts += batch_timeouts
        self.batches_completed += 1
        batch_time = time.time() - batch_start_time
        self._update_status(f"📦 Батч #{self.batches_completed} завершен за {batch_time/60:.1f} мин. [Успешно: {batch_successful}, Ошибок: {batch_failed}, Зависаний: {batch_timeouts}]", -1)
        
    def run(self):
        self.start_time = time.time()
        self._update_status("🔄 ЗАПУСК НЕПРЕРЫВНОЙ ОБРАБОТКИ", 10)
        cycle_number = 0
        try:
            while True:
                cycle_number += 1
                if self.check_runtime_limit(): break
                self._update_status(f"\n🔍 ЦИКЛ #{cycle_number}: Проверяем наличие URL...", -1)
                urls_batch = self.get_unprocessed_urls(limit=self.batch_size)
                if not urls_batch:
                    self._update_status("🎉 Все URL обработаны!", -1)
                    break
                self._update_status(f"📋 Найдено {len(urls_batch)} URL для обработки", -1)
                self.process_batch(urls_batch)
                if not self.get_unprocessed_urls(limit=1):
                    self._update_status("🎯 Все URL обработаны!", -1)
                    break
                pause_time = random.uniform(self.pause_between_batches, self.pause_between_batches + 60)
                self._update_status(f"⏸️ Пауза между батчами: {pause_time:.1f} сек...", -1)
                time.sleep(pause_time)
        except KeyboardInterrupt:
            self._update_status("⚠️ Получен сигнал остановки.", -1)
        except Exception as e:
            self._update_status(f"❌ Критическая ошибка: {e}", -1)
            import traceback
            traceback.print_exc()
        self.print_overall_stats()

    def get_safety_settings(self): return {'name':'placeholder'} # Удалено для краткости
    def print_overall_stats(self): print("Статистика работы...") # Удалено для краткости
    def check_runtime_limit(self): return False # Удалено для краткости

def main():
    if sys.platform != 'win32':
        multiprocessing.set_start_method('spawn', force=True)
    multiprocessing.freeze_support()
    try:
        collector = RenderScreenshotCollector()
        collector.run()
    except Exception as e:
        print(f"❌ Критическая ошибка при запуске: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()