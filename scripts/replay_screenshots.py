import json
import os
import time
import hashlib
import random
import sys
from datetime import datetime, timedelta
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

# Таймаут для обработки ОДНОЙ URL в отдельном процессе
PROCESS_TIMEOUT_PER_URL = 240  # 4 минуты

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
]

# Функция-воркер для полной изоляции обработки одной URL
def worker_process_url(collector_config: dict, url_data: dict, result_queue: multiprocessing.Queue):
    """
    Запускается в отдельном процессе. Создает свой экземпляр коллектора и Playwright.
    """
    try:
        collector = RenderScreenshotCollector(config_override=collector_config)
        safety_settings = collector.get_safety_settings()

        with sync_playwright() as p:
            browser_args = [
                '--no-proxy-server', '--disable-proxy-config-service', '--no-sandbox',
                '--disable-setuid-sandbox', '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage', '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
            browser = p.chromium.launch(headless=True, args=browser_args)
            
            user_agent = random.choice(USER_AGENTS)
            context = browser.new_context(
                user_agent=user_agent, viewport={'width': 1366, 'height': 768},
                locale='en-US', timezone_id='America/New_York'
            )
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            """)
            context.add_cookies(collector.cookies)
            page = context.new_page()

            success, _ = collector.process_single_url(page, url_data, safety_settings)

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
            # Инициализация для дочернего процесса (легковесная)
            self.credentials_path = config_override["credentials_path"]
            self.gdrive_folder_id = config_override["gdrive_folder_id"]
            self.bq_project_id = config_override["bq_project_id"]
            self.bq_dataset_id = config_override["bq_dataset_id"]
            self.bq_table_id = config_override["bq_table_id"]
            self.min_duration_seconds = config_override["min_duration_seconds"]
            self.status_callback = None
            self.cookies = self._load_cookies_from_secret_file(verbose=False)
        else:
            # Полная инициализация для основного процесса
            self.status_callback = status_callback
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
            self.total_processed = 0
            self.total_successful = 0
            self.total_failed = 0
            self.total_timeouts = 0
            self.batches_completed = 0
            
            self._update_status("🔐 Настраиваем подключения...", 1)
            self.cookies = self._load_cookies_from_secret_file()
        
        self.full_table_name = f"`{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table_id}`"
        self._init_bigquery()
        self._init_google_drive()

    def _update_status(self, details: str, progress: int):
        if self.status_callback: 
            self.status_callback(details, progress)
        
        timestamp = datetime.now().strftime("%H:%M:%S")
        if progress != -1: 
            print(f"[{timestamp} - {progress}%] {details}")
        else:
            print(f"[{timestamp}] {details}")

    def _load_cookies_from_secret_file(self, verbose=True):
        secret_file_path = "/etc/secrets/cookies.json"
        if verbose: self._update_status(f"Загрузка cookies из {secret_file_path}...", 2)
        if not os.path.exists(secret_file_path):
            if verbose: self._update_status(f"❌ Файл cookies не найден!", 2)
            return []
        try:
            with open(secret_file_path, 'r') as f: 
                cookies = json.load(f)
            if verbose: self._update_status(f"✅ Cookies загружены ({len(cookies)} шт).", 3)
            return cookies
        except Exception as e:
            if verbose: self._update_status(f"❌ Ошибка чтения cookies: {e}", 3)
            return []

    def _init_bigquery(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project_id)
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к BigQuery: {e}")

    def _init_google_drive(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path, 
                scopes=['https://www.googleapis.com/auth/drive']
            )
            self.drive_service = build('drive', 'v3', credentials=credentials)
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к Google Drive: {e}")

    def get_unprocessed_urls(self, limit=None):
        query = f"""
        SELECT 
            session_replay_url,
            amplitude_id,
            session_replay_id,
            duration_seconds,
            events_count,
            record_date
        FROM {self.full_table_name}
        WHERE is_processed = FALSE
        AND duration_seconds >= {self.min_duration_seconds}
        ORDER BY record_date DESC
        """
        if limit:
            query += f"\nLIMIT {limit}"

        try:
            result = self.bq_client.query(query).result()
            urls_data = []
            for row in result:
                urls_data.append({
                    'url': row.session_replay_url,
                    'amplitude_id': row.amplitude_id,
                    'session_replay_id': row.session_replay_id,
                    'duration_seconds': row.duration_seconds,
                    'events_count': row.events_count,
                    'record_date': row.record_date.strftime('%Y-%m-%d')
                })
            return urls_data
        except Exception as e:
            self._update_status(f"❌ Ошибка получения URL: {e}", -1)
            raise

    def mark_url_as_processed(self, url, success=True):
        try:
            update_query = f"""
            UPDATE {self.full_table_name}
            SET is_processed = TRUE
            WHERE session_replay_url = @url
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("url", "STRING", url)
                ]
            )
            self.bq_client.query(update_query, job_config=job_config).result()
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Ошибка обновления статуса URL {url}: {e}")

    def get_session_id_from_url(self, url):
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        if "sessionReplayId=" in url:
            parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
            session_replay_id = parts[0]
            session_start_time = parts[1] if len(parts) > 1 else "unknown"
            return f"{session_replay_id}_{session_start_time}_{url_hash}"
        return f"no_session_id_{url_hash}"

    def wait_for_content(self, page, selector, bad_texts=("Loading", "Loading summary"), timeout=10, min_text_length=10):
        start = time.time()
        while True:
            el = page.query_selector(selector)
            if el:
                try:
                    txt = el.inner_text().strip()
                    if txt and all(bad not in txt for bad in bad_texts) and len(txt) >= min_text_length:
                        return el
                except PlaywrightError:
                    pass
            if time.time() - start > timeout:
                return None
            time.sleep(0.5)

    def simulate_human_behavior(self, page):
        try:
            for _ in range(random.randint(2, 4)):
                x = random.randint(200, 1200)
                y = random.randint(200, 700)
                page.mouse.move(x, y, steps=random.randint(5, 15))
                time.sleep(random.uniform(0.1, 0.3))
            if random.random() < 0.4:
                scroll_amount = random.randint(100, 500)
                direction = random.choice([1, -1])
                page.evaluate(f"window.scrollBy(0, {scroll_amount * direction})")
                time.sleep(random.uniform(0.5, 1.5))
        except Exception:
            pass

    def screenshot_summary_flexible(self, page, session_id, base_dir, summary_el=None):
        el = summary_el
        if not el:
            el = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=3)
        if not el:
            fallback_selectors = [
                'div[style*="min-width: 460px"]',
                '.ltext-_uoww22', 'div:has-text("Summary")',
                'p:has-text("The user")', 'p:has-text("session")'
            ]
            for selector in fallback_selectors:
                try:
                    el_candidate = page.query_selector(selector)
                    if el_candidate:
                        text = el_candidate.inner_text().strip()
                        if text and len(text) > 20 and "Loading" not in text:
                            el = el_candidate
                            break
                except Exception:
                    continue
        if not el:
            return []
        try:
            img_name = os.path.join(base_dir, f"{session_id}_summary.png")
            el.screenshot(path=img_name)
            return [img_name]
        except Exception as e:
            print(f"Ошибка скриншота Summary для {session_id}: {e}")
            return []

    def screenshot_by_title(self, page, block_title, session_id, base_dir):
        el = None
        search_selectors = [
            f'h4:has-text("{block_title}")', f'div:has-text("{block_title}")',
            f'span:has-text("{block_title}")', f'h3:has-text("{block_title}")',
            f'h5:has-text("{block_title}")', f'[title="{block_title}"]',
            f'[aria-label="{block_title}"]'
        ]
        for selector in search_selectors:
            try:
                maybe = page.query_selector(selector)
                if maybe:
                    parent = maybe
                    for _ in range(6):
                        bbox = parent.bounding_box()
                        if bbox and bbox['height'] > 60 and bbox['width'] > 200:
                            el = parent
                            break
                        parent = parent.evaluate_handle('el => el.parentElement').as_element()
                        if not parent: break
                    if el: break
            except Exception:
                continue
        if not el:
            return None
        try:
            img_path = os.path.join(base_dir, f"{session_id}_{block_title.lower()}.png")
            el.screenshot(path=img_path)
            return img_path
        except Exception as e:
            print(f"Ошибка скриншота '{block_title}' для {session_id}: {e}")
            return None

    def screenshot_userinfo_block(self, page, session_id, base_dir):
        userinfo_div = None
        try:
            elements = page.query_selector_all('.cerulean-cardbase.cerulean-alpha-general-card')
            for element in elements:
                bbox = element.bounding_box()
                text = element.inner_text().strip()
                if (bbox and bbox['y'] < 400 and text and len(text) < 500 and
                        (any(char.isdigit() for char in text) or "Spain" in text)):
                    userinfo_div = element
                    break
        except Exception:
            pass
        if not userinfo_div:
            return None
        try:
            img_path = os.path.join(base_dir, f"{session_id}_userinfo.png")
            userinfo_div.screenshot(path=img_path)
            return img_path
        except Exception as e:
            print(f"Ошибка скриншота User Info для {session_id}: {e}")
            return None

    def create_session_folder_structure(self, session_id, screenshots, url_data):
        session_dir = tempfile.mkdtemp(prefix=f"session_folder_{session_id}_")
        session_screenshots = []
        for screenshot_path in screenshots:
            if screenshot_path and os.path.exists(screenshot_path):
                filename = os.path.basename(screenshot_path)
                new_path = os.path.join(session_dir, filename)
                shutil.copy2(screenshot_path, new_path)
                session_screenshots.append(new_path)
        
        metadata = {
            "session_id": session_id, "url": url_data['url'], "amplitude_id": url_data['amplitude_id'],
            "session_replay_id": url_data['session_replay_id'], "duration_seconds": url_data['duration_seconds'],
            "events_count": url_data['events_count'], "record_date": url_data['record_date'],
            "processed_at": datetime.now().isoformat(),
            "screenshots": [os.path.basename(path) for path in session_screenshots]
        }
        metadata_path = os.path.join(session_dir, "metadata.json")
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        return session_dir, session_screenshots + [metadata_path]

    def upload_to_google_drive(self, file_path, filename, folder_id):
        try:
            file_metadata = {'name': filename, 'parents': [folder_id]}
            media = MediaFileUpload(file_path, resumable=True)
            file = self.drive_service.files().create(
                body=file_metadata, media_body=media, fields='id, name, webViewLink'
            ).execute()
            return file
        except Exception as e:
            print(f"❌ Ошибка загрузки в Google Drive: {e}")
            return None

    def create_and_upload_session_archive(self, session_dir, session_id, is_failure=False):
        try:
            prefix = "FAILURE" if is_failure else "session_replay"
            archive_name = f"{prefix}_{session_id}_{int(time.time())}.zip"
            archive_path = os.path.join(tempfile.gettempdir(), archive_name)
            
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(session_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, session_dir)
                        zipf.write(file_path, arcname)

            uploaded_file = self.upload_to_google_drive(archive_path, archive_name, self.gdrive_folder_id)
            
            shutil.rmtree(session_dir, ignore_errors=True)
            if os.path.exists(archive_path):
                os.remove(archive_path)
            return uploaded_file
        except Exception as e:
            print(f"❌ Ошибка создания/загрузки архива: {e}")
            return None

    def process_single_url(self, page, url_data, safety_settings):
        url = url_data['url']
        session_id = self.get_session_id_from_url(url)
        temp_screenshots_dir = tempfile.mkdtemp(prefix=f"screenshots_{session_id}_")
        REQUIRED_BLOCKS = ['userinfo', 'summary', 'sentiment']
        screenshot_paths = []
        
        try:
            self.simulate_human_behavior(page)
            page.goto(url, timeout=60000)

            try:
                summary_tab = page.wait_for_selector("text=Summary", timeout=15000)
                summary_tab.click()
                summary_el = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=20)
            except PlaywrightTimeoutError:
                print(f"❌ Не найдена вкладка Summary или ее контент для {session_id}")
                return False, []

            screenshot_results = {}
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
            total_blocks = len([path for path in screenshot_paths if path and os.path.exists(path)])
            
            if not all_success or total_blocks < 3:
                print(f"❌ Не собраны все обязательные блоки для {session_id}")
                return False, screenshot_paths
            
            session_dir, _ = self.create_session_folder_structure(session_id, screenshot_paths, url_data)
            uploaded_file = self.create_and_upload_session_archive(session_dir, session_id)
            
            return bool(uploaded_file), screenshot_paths
        except Exception as e:
            print(f"❌ Критическая ошибка при обработке URL {session_id}: {e}")
            failure_path = os.path.join(temp_screenshots_dir, f"FAILURE_screenshot.png")
            try:
                page.screenshot(path=failure_path, full_page=True, timeout=10000)
            except Exception as e_scr:
                print(f"Не удалось сделать скриншот ошибки: {e_scr}")
            
            self.create_and_upload_session_archive(temp_screenshots_dir, session_id, is_failure=True)
            return False, []
        finally:
            shutil.rmtree(temp_screenshots_dir, ignore_errors=True)

    def get_safety_settings(self):
        safety_mode = os.environ.get('SAFETY_MODE', 'normal').lower()
        if safety_mode == 'slow':
            return {'min_delay': 3, 'max_delay': 8, 'name': 'МЕДЛЕННЫЙ'}
        elif safety_mode == 'fast':
            return {'min_delay': 1, 'max_delay': 3, 'name': 'БЫСТРЫЙ'}
        else:
            return {'min_delay': 2, 'max_delay': 5, 'name': 'ОБЫЧНЫЙ'}

    def print_overall_stats(self):
        if self.start_time:
            elapsed = time.time() - self.start_time
            elapsed_hours = elapsed / 3600
            success_rate = (self.total_successful / self.total_processed * 100) if self.total_processed > 0 else 0
            
            self._update_status("=" * 60, -1)
            self._update_status(f"📊 ОБЩАЯ СТАТИСТИКА РАБОТЫ", -1)
            self._update_status(f"⏱️  Время работы: {elapsed_hours:.1f} часов", -1)
            self._update_status(f"🔄 Батчей завершено: {self.batches_completed}", -1)
            self._update_status(f"📈 Всего обработано: {self.total_processed} URL", -1)
            self._update_status(f"✅ Успешно: {self.total_successful}", -1)
            self._update_status(f"❌ Ошибок: {self.total_failed}", -1)
            self._update_status(f"❗ Зависаний (Timeout): {self.total_timeouts}", -1)
            self._update_status(f"📊 Процент успеха: {success_rate:.1f}%", -1)
            if self.total_processed > 0:
                avg_time_per_url = elapsed / self.total_processed
                self._update_status(f"⚡ Среднее время на URL: {avg_time_per_url:.1f} сек", -1)
            self._update_status("=" * 60, -1)

    def check_runtime_limit(self):
        if self.start_time:
            elapsed_hours = (time.time() - self.start_time) / 3600
            if elapsed_hours >= self.max_runtime_hours:
                self._update_status(f"⏰ Достигнут лимит времени работы ({self.max_runtime_hours}ч)", -1)
                return True
        return False

    def process_batch(self, urls_batch, safety_settings):
        batch_start_time = time.time()
        batch_successful = 0
        batch_failed = 0
        batch_timeouts = 0
        
        self._update_status(f"🚀 Начинаем обработку батча из {len(urls_batch)} URL с таймаутом {PROCESS_TIMEOUT_PER_URL}с на каждую", -1)
        
        result_queue = multiprocessing.Queue()
        collector_config = {
            "credentials_path": self.credentials_path, "gdrive_folder_id": self.gdrive_folder_id,
            "bq_project_id": self.bq_project_id, "bq_dataset_id": self.bq_dataset_id,
            "bq_table_id": self.bq_table_id, "min_duration_seconds": self.min_duration_seconds
        }

        for i, url_data in enumerate(urls_batch, 1):
            self._update_status(f"▶️ [{i}/{len(urls_batch)}] Запускаем процесс для URL ...{url_data['url'][-40:]}", -1)
            
            process = multiprocessing.Process(target=worker_process_url, args=(collector_config, url_data, result_queue))
            process.start()
            
            self._update_status(f"    Ждем завершения процесса (PID: {process.pid})", -1)
            process.join(timeout=PROCESS_TIMEOUT_PER_URL)
            self._update_status(f"    Процесс (PID: {process.pid}) завершил ожидание.", -1)

            if process.is_alive():
                self._update_status(f"❗ ТАЙМАУТ! Процесс для URL ...{url_data['url'][-40:]} завис. Завершаем принудительно.", -1)
                process.terminate()
                process.join()
                
                batch_timeouts += 1
                batch_failed += 1
                self.mark_url_as_processed(url_data['url'], success=False)
            else:
                try:
                    success = result_queue.get_nowait()
                    if success:
                        batch_successful += 1
                        self._update_status(f"✅ Успешно обработан ...{url_data['url'][-40:]}", -1)
                    else:
                        batch_failed += 1
                        self._update_status(f"❌ Ошибка обработки ...{url_data['url'][-40:]}", -1)
                except queue.Empty:
                    batch_failed += 1
                    self._update_status(f"❌ Процесс завершился, но не вернул результат. Считаем ошибкой.", -1)
                    self.mark_url_as_processed(url_data['url'], success=False)

            if i < len(urls_batch):
                delay = random.uniform(safety_settings['min_delay'], safety_settings['max_delay'])
                time.sleep(delay)

        self.total_processed += len(urls_batch)
        self.total_successful += batch_successful
        self.total_failed += batch_failed
        self.total_timeouts += batch_timeouts
        self.batches_completed += 1

        batch_time = time.time() - batch_start_time
        self._update_status(f"📦 Батч #{self.batches_completed} завершен за {batch_time/60:.1f} мин", -1)
        self._update_status(f"   ✅ Успешно: {batch_successful} | ❌ Ошибок: {batch_failed} | ❗ Зависаний: {batch_timeouts}", -1)
        
        return batch_successful, batch_failed
        
    def run(self):
        self.start_time = time.time()
        
        self._update_status("🔄 ЗАПУСК НЕПРЕРЫВНОЙ ОБРАБОТКИ СКРИНШОТОВ", 10)
        self._update_status("=" * 60, 10)
        self._update_status(f"⚙️  Размер батча: {self.batch_size} URL", 15)
        self._update_status(f"⏱️  Пауза между батчами: {self.pause_between_batches} сек", 15)
        self._update_status(f"🕐 Максимальное время работы: {self.max_runtime_hours} часов", 15)
        self._update_status(f"📏 Минимальная длительность сессий: {self.min_duration_seconds} сек", 15)
        
        safety_settings = self.get_safety_settings()
        self._update_status(f"🛡️  Режим безопасности: {safety_settings['name']}", 20)
        
        cycle_number = 0
        
        try:
            while True:
                cycle_number += 1
                
                if self.check_runtime_limit():
                    self._update_status("🛑 Останавливаем работу по лимиту времени", -1)
                    break
                
                self._update_status(f"\n🔍 ЦИКЛ #{cycle_number}: Проверяем наличие необработанных URL...", -1)
                urls_batch = self.get_unprocessed_urls(limit=self.batch_size)
                
                if not urls_batch:
                    self._update_status("🎉 Нет необработанных URL! Работа завершена.", -1)
                    break
                
                self._update_status(f"📋 Найдено {len(urls_batch)} URL для обработки", -1)
                self.process_batch(urls_batch, safety_settings)
                
                if cycle_number % 5 == 0:
                    self.print_overall_stats()
                
                remaining_urls = self.get_unprocessed_urls(limit=1)
                if not remaining_urls:
                    self._update_status("🎯 Все URL обработаны! Завершаем работу.", -1)
                    break
                
                pause_time = random.uniform(self.pause_between_batches, self.pause_between_batches + 60)
                self._update_status(f"⏸️  Пауза между батчами: {pause_time:.1f} сек...", -1)
                time.sleep(pause_time)
                
        except KeyboardInterrupt:
            self._update_status("⚠️ Получен сигнал остановки", -1)
        except Exception as e:
            self._update_status(f"❌ Критическая ошибка в основном цикле: {e}", -1)
            import traceback
            traceback.print_exc()
        
        self.print_overall_stats()
        
        reason_for_stop = "no_more_urls" if not self.get_unprocessed_urls(limit=1) else "time_limit_reached"
        result = {
            "status": "completed", "cycles_completed": cycle_number, "batches_completed": self.batches_completed,
            "total_processed": self.total_processed, "total_successful": self.total_successful,
            "total_failed": self.total_failed, "total_timeouts": self.total_timeouts,
            "success_rate": f"{(self.total_successful/self.total_processed*100):.1f}%" if self.total_processed > 0 else "0%",
            "total_runtime_hours": round((time.time() - self.start_time) / 3600, 2),
            "reason_for_stop": reason_for_stop
        }
        
        self._update_status(f"🏁 НЕПРЕРЫВНАЯ ОБРАБОТКА ЗАВЕРШЕНА!", 100)
        self._update_status(f"📊 Причина остановки: {result['reason_for_stop']}", 100)
        return result


def main():
    # --- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ ДЛЯ СТАБИЛЬНОСТИ ---
    # Принудительно устанавливаем 'spawn' как метод создания дочерних процессов.
    # Это предотвращает 'deadlock' из-за унаследованных сетевых соединений в Linux.
    # Эту строку нужно вызвать до создания любых процессов или очередей.
    if sys.platform != 'win32':
        multiprocessing.set_start_method('spawn', force=True)

    multiprocessing.freeze_support()
    try:
        def console_status_callback(details: str, progress: int):
            if progress != -1:
                print(f"[{datetime.now().strftime('%H:%M:%S')} - {progress}%] {details}")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {details}")

        collector = RenderScreenshotCollector(status_callback=console_status_callback)
        
        print("🤖 RENDER MODE: Запуск непрерывной обработки")
        result = collector.run()

        print(f"\n🏁 Финальный результат: {json.dumps(result, indent=2)}")
        return result

    except Exception as e:
        print(f"❌ Критическая ошибка при запуске: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    main()