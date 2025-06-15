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

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
]

class ContinuousScreenshotCollector:
    def __init__(self, status_callback: Optional[Callable[[str, int], None]] = None):
        self.status_callback = status_callback
        self.credentials_path = settings.GOOGLE_APPLICATION_CREDENTIALS
        self.gdrive_folder_id = settings.GDRIVE_FOLDER_ID
        
        # BigQuery настройки
        self.bq_project_id = settings.BQ_PROJECT_ID
        self.bq_dataset_id = settings.BQ_DATASET_ID
        self.bq_table_id = settings.BQ_TABLE_ID
        self.full_table_name = f"`{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table_id}`"
        
        # Настройки для непрерывной работы
        self.batch_size = int(os.environ.get('BATCH_SIZE', '50'))
        self.pause_between_batches = int(os.environ.get('PAUSE_BETWEEN_BATCHES', '300'))  # 5 минут
        self.max_runtime_hours = int(os.environ.get('MAX_RUNTIME_HOURS', '18'))  # 18 часов
        self.min_duration_seconds = int(os.environ.get('MIN_DURATION_SECONDS', '20'))
        
        # Статистика работы
        self.start_time = None
        self.total_processed = 0
        self.total_successful = 0
        self.total_failed = 0
        self.batches_completed = 0
        
        self._update_status("🔐 Настраиваем подключения...", 1)
        self.cookies = self._load_cookies_from_secret_file()
        self._init_bigquery()
        self._init_google_drive()

    def _update_status(self, details: str, progress: int):
        if self.status_callback: 
            self.status_callback(details, progress)
        if progress != -1: 
            print(f"[{progress}%] {details}")
        else:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] {details}")

    def _load_cookies_from_secret_file(self):
        secret_file_path = "/etc/secrets/cookies.json"
        self._update_status(f"Загрузка cookies из {secret_file_path}...", 2)
        if not os.path.exists(secret_file_path):
            self._update_status(f"❌ Файл cookies не найден!", 2)
            return []
        try:
            with open(secret_file_path, 'r') as f: 
                cookies = json.load(f)
            self._update_status(f"✅ Cookies загружены ({len(cookies)} шт).", 3)
            return cookies
        except Exception as e:
            self._update_status(f"❌ Ошибка чтения cookies: {e}", 3)
            return []

    def _init_bigquery(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project_id)
            self._update_status("✅ BigQuery подключен", 4)
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к BigQuery: {e}")

    def _init_google_drive(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path, 
                scopes=['https://www.googleapis.com/auth/drive']
            )
            self.drive_service = build('drive', 'v3', credentials=credentials)
            self._update_status("✅ Google Drive подключен", 5)
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
            if success:
                self._update_status("✅ URL отмечен как обработанный", -1)
            else:
                self._update_status("⚠️ URL отмечен как обработанный (с ошибкой)", -1)
        except Exception as e:
            self._update_status(f"❌ Ошибка обновления статуса URL: {e}", -1)

    def get_session_id_from_url(self, url):
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        if "sessionReplayId=" in url:
            parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
            session_replay_id = parts[0]
            session_start_time = parts[1] if len(parts) > 1 else "unknown"
            return f"{session_replay_id}_{session_start_time}_{url_hash}"
        return f"no_session_id_{url_hash}"

    def wait_for_content(self, page, selector, bad_texts=("Loading", "Loading summary"), timeout=10, min_text_length=10):
        """
        Ждём появления контента не дольше timeout секунд.
        Проверяем каждые 0.5 сек, логируем каждые 2 сек.
        Как только появился валидный текст — сразу возвращаем элемент.
        Если за timeout секунд не появился — возвращаем None.
        """
        start = time.time()
        last_log = 0
        while True:
            el = page.query_selector(selector)
            if el:
                txt = el.inner_text().strip()
                if txt and all(bad not in txt for bad in bad_texts) and len(txt) >= min_text_length:
                    return el
            elapsed = time.time() - start
            if elapsed - last_log >= 3:
                last_log = elapsed
            if elapsed > timeout:
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
            if random.random() < 0.2:
                safe_x = random.randint(50, 1300)
                safe_y = random.randint(50, 150)
                page.mouse.click(safe_x, safe_y)
                time.sleep(random.uniform(0.3, 0.8))
            if random.random() < 0.3:
                page.keyboard.press('Tab')
                time.sleep(random.uniform(0.2, 0.5))
        except Exception:
            pass

    def screenshot_summary_flexible(self, page, session_id, base_dir, summary_el=None):
        self._update_status("📄 Ищем Summary блок...", -1)

        el = summary_el
        if not el:
            el = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=3)

        if el:
            text_content = el.inner_text().strip()
            if len(text_content) > 20:
                self._update_status(f"✅ Summary загружен (длина: {len(text_content)} символов)", -1)
            else:
                self._update_status(f"⚠️ Summary слишком короткий ({len(text_content)} символов), пробуем fallback", -1)
                el = None
        if not el:
            self._update_status("⚠️ Пробуем fallback селекторы для Summary...", -1)
            fallback_selectors = [
                'div[style*="min-width: 460px"]',
                '.ltext-_uoww22',
                'div:has-text("Summary")',
                'p:has-text("The user")',
                'p:has-text("session")'
            ]
            for selector in fallback_selectors:
                try:
                    el = page.query_selector(selector)
                    if el:
                        text = el.inner_text().strip()
                        if text and len(text) > 20 and "Loading" not in text:
                            self._update_status(f"✅ Fallback сработал с селектором: {selector}", -1)
                            break
                        else:
                            el = None
                except Exception:
                    continue
            if not el:
                self._update_status("❌ Не удалось найти Summary блок ни одним способом", -1)
                return []
        try:
            img_name = os.path.join(base_dir, f"{session_id}_summary.png")
            el.screenshot(path=img_name)
            self._update_status("✅ Summary скриншот сохранён", -1)
            return [img_name]
        except Exception as e:
            self._update_status(f"❌ Ошибка создания скриншота Summary: {e}", -1)
            return []

    def screenshot_by_title(self, page, block_title, session_id, base_dir):
        self._update_status(f"🔍 Ищем блок '{block_title}'...", -1)
        el = None
        search_selectors = [
            f'h4:has-text("{block_title}")',
            f'div:has-text("{block_title}")',
            f'span:has-text("{block_title}")',
            f'h3:has-text("{block_title}")',
            f'h5:has-text("{block_title}")',
            f'[title="{block_title}"]',
            f'[aria-label="{block_title}"]'
        ]
        for selector in search_selectors:
            try:
                maybe = page.query_selector(selector)
                if maybe:
                    self._update_status(f"📍 Найден элемент с '{block_title}' через селектор: {selector}", -1)
                    parent = maybe
                    for level in range(6):
                        try:
                            bbox = parent.bounding_box()
                            if bbox and bbox['height'] > 60 and bbox['width'] > 200:
                                text_content = parent.inner_text().strip()
                                if text_content and len(text_content) > 10:
                                    el = parent
                                    self._update_status(f"✅ Найден подходящий контейнер на уровне {level}", -1)
                                    break
                        except Exception:
                            pass
                        try:
                            parent = parent.evaluate_handle('el => el.parentElement').as_element()
                            if not parent:
                                break
                        except Exception:
                            break
                    if el:
                        break
            except Exception as e:
                continue
        if not el:
            self._update_status(f"🔄 Пробуем поиск по частичному содержимому '{block_title}'...", -1)
            try:
                all_elements = page.query_selector_all('div, span, h1, h2, h3, h4, h5, h6')
                for element in all_elements:
                    try:
                        text = element.inner_text().strip()
                        if block_title.lower() in text.lower() and len(text) < 100:
                            parent = element
                            for _ in range(4):
                                try:
                                    parent = parent.evaluate_handle('el => el.parentElement').as_element()
                                    if parent:
                                        bbox = parent.bounding_box()
                                        parent_text = parent.inner_text().strip()
                                        if (bbox and bbox['height'] > 60 and
                                                len(parent_text) > len(text) and len(parent_text) < 1000):
                                            el = parent
                                            self._update_status(f"✅ Найден через поиск по содержимому", -1)
                                            break
                                except Exception:
                                    break
                            if el:
                                break
                    except Exception:
                        continue
            except Exception:
                pass
        if el:
            content_loaded = False
            self._update_status(f"⏳ Ждем загрузку контента блока '{block_title}'...", -1)
            for attempt in range(30):
                try:
                    txt = el.inner_text().strip()
                    if txt and "Loading" not in txt and len(txt) > 10:
                        content_loaded = True
                        self._update_status(f"✅ Контент блока '{block_title}' загружен", -1)
                        break
                except Exception:
                    pass
                time.sleep(0.5)
            if not content_loaded:
                self._update_status(f"⚠️ {block_title} — Не дождались полной загрузки, скриню как есть", -1)
        else:
            self._update_status(f"❌ Блок '{block_title}' не найден!", -1)
            return None
        try:
            img_path = os.path.join(base_dir, f"{session_id}_{block_title.lower()}.png")
            el.screenshot(path=img_path)
            self._update_status(f"✅ {block_title} скриншот сохранён", -1)
            return img_path
        except Exception as e:
            self._update_status(f"❌ Ошибка создания скриншота {block_title}: {e}", -1)
            return None

    def screenshot_userinfo_block(self, page, session_id, base_dir):
        self._update_status("🔍 Ищем блок User Info...", -1)
        userinfo_div = None
        try:
            css_selector = '.cerulean-cardbase.cerulean-alpha-general-card'
            elements = page.query_selector_all(css_selector)
            for element in elements:
                try:
                    text = element.inner_text().strip()
                    bbox = element.bounding_box()
                    if (bbox and
                            bbox['y'] < 400 and
                            text and
                            len(text) > 10 and len(text) < 500 and
                            (any(char.isdigit() for char in text) or
                             any(country in text for country in
                                 ["Spain", "Peru", "Bolivia", "Ecuador", "Netherlands", "Costa Rica", "Russia"]))):
                        userinfo_div = element
                        break
                except Exception:
                    continue
        except Exception:
            pass
        if not userinfo_div:
            try:
                session_selectors = [
                    'text=Session Length',
                    'text=Event Total',
                    'text=Device Type'
                ]
                for selector in session_selectors:
                    element = page.query_selector(selector)
                    if element:
                        parent = element
                        for _ in range(5):
                            try:
                                parent = parent.evaluate_handle('el => el.parentElement').as_element()
                                if parent:
                                    bbox = parent.bounding_box()
                                    text = parent.inner_text().strip()
                                    if (bbox and bbox['y'] < 400 and
                                            bbox['width'] > 200 and bbox['height'] > 80 and
                                            len(text) > 20 and len(text) < 500):
                                        userinfo_div = parent
                                        break
                            except Exception:
                                break
                        if userinfo_div:
                            break
            except Exception:
                pass
        if not userinfo_div:
            self._update_status("⚠️ User info не найден", -1)
            return None
        try:
            img_path = os.path.join(base_dir, f"{session_id}_userinfo.png")
            userinfo_div.screenshot(path=img_path)
            self._update_status("✅ User info сохранён", -1)
            return img_path
        except Exception:
            self._update_status("❌ Ошибка создания скриншота user info", -1)
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
            "session_id": session_id,
            "url": url_data['url'],
            "amplitude_id": url_data['amplitude_id'],
            "session_replay_id": url_data['session_replay_id'],
            "duration_seconds": url_data['duration_seconds'],
            "events_count": url_data['events_count'],
            "record_date": url_data['record_date'],
            "processed_at": datetime.now().isoformat(),
            "screenshots": [os.path.basename(path) for path in session_screenshots]
        }
        metadata_path = os.path.join(session_dir, "metadata.json")
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        return session_dir, session_screenshots + [metadata_path]

    def upload_to_google_drive(self, file_path, filename, folder_id):
        try:
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            media = MediaFileUpload(file_path, resumable=True)
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink'
            ).execute()
            return file
        except Exception as e:
            self._update_status(f"❌ Ошибка загрузки в Google Drive: {e}", -1)
            return None

    def create_and_upload_session_archive(self, session_dir, session_id, is_failure=False):
        try:
            prefix = "FAILURE" if is_failure else "session_replay"
            archive_name = f"{prefix}_{session_id}_{int(time.time())}.zip"
            archive_path = archive_name
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(session_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, session_dir)
                        zipf.write(file_path, arcname)
            self._update_status(f"📦 Создан архив: {archive_name}", -1)
            uploaded_file = self.upload_to_google_drive(
                archive_path,
                archive_name,
                self.gdrive_folder_id
            )
            if uploaded_file:
                self._update_status(f"☁️ Архив загружен в Google Drive", -1)
                shutil.rmtree(session_dir, ignore_errors=True)
                os.remove(archive_path)
                return uploaded_file
            else:
                self._update_status("❌ Не удалось загрузить архив", -1)
                return None
        except Exception as e:
            self._update_status(f"❌ Ошибка создания архива: {e}", -1)
            return None

    def process_single_url(self, page, url_data, safety_settings):
        url = url_data['url']
        session_id = self.get_session_id_from_url(url)
        temp_screenshots_dir = tempfile.mkdtemp(prefix=f"screenshots_{session_id}_")
        REQUIRED_BLOCKS = ['userinfo', 'summary', 'sentiment']
        OPTIONAL_BLOCKS = ['actions']
        try:
            self.simulate_human_behavior(page)
            page.goto(url, timeout=30000)
            time.sleep(random.uniform(2, 5))
            summary_tab = page.query_selector("text=Summary")
            if summary_tab:
                self.simulate_human_behavior(page)
                summary_tab.click()
                self._update_status("🖱️ Кликнули на Summary", -1)
                time.sleep(random.uniform(3, 6))
                summary_el = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=10)
            else:
                self._update_status("❌ Вкладка Summary не найдена!", -1)
                return False, []
            screenshot_results = {}
            self._update_status("📸 Начинаем создание скриншотов...", -1)
            userinfo_path = self.screenshot_userinfo_block(page, session_id, temp_screenshots_dir)
            screenshot_results['userinfo'] = userinfo_path is not None
            screenshot_paths = [userinfo_path] if userinfo_path else []
            time.sleep(random.uniform(1, 2))
            summary_paths = self.screenshot_summary_flexible(page, session_id, temp_screenshots_dir, summary_el=summary_el)
            screenshot_results['summary'] = len(summary_paths) > 0
            if summary_paths:
                screenshot_paths += summary_paths
            time.sleep(random.uniform(1, 2))
            sentiment_path = self.screenshot_by_title(page, "Sentiment", session_id, temp_screenshots_dir)
            screenshot_results['sentiment'] = sentiment_path is not None
            if sentiment_path:
                screenshot_paths.append(sentiment_path)
            time.sleep(random.uniform(1, 2))
            actions_path = self.screenshot_by_title(page, "Actions", session_id, temp_screenshots_dir)
            screenshot_results['actions'] = actions_path is not None
            if actions_path:
                screenshot_paths.append(actions_path)
            all_success = all(screenshot_results.get(block, False) for block in REQUIRED_BLOCKS)
            total_blocks = len([path for path in screenshot_paths if path and os.path.exists(path)])
            if not all_success:
                self._update_status("❌ Не получены все обязательные блоки", -1)
                return False, screenshot_paths
            if total_blocks < 3:
                self._update_status("❌ Получено меньше 3 скриншотов", -1)
                return False, screenshot_paths
            session_dir, all_files = self.create_session_folder_structure(
                session_id, screenshot_paths, url_data
            )
            quality_info = {
                "screenshot_results": screenshot_results,
                "total_screenshots": total_blocks,
                "required_blocks_success": all_success,
                "success_rate_percent": 100.0,
                "processing_quality": "high"
            }
            metadata_path = os.path.join(session_dir, "metadata.json")
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                metadata['quality_analysis'] = quality_info
                with open(metadata_path, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
            uploaded_file = self.create_and_upload_session_archive(session_dir, session_id)
            if uploaded_file:
                for path in screenshot_paths:
                    if path and os.path.exists(path):
                        os.remove(path)
                return True, screenshot_paths
            else:
                self._update_status("❌ Не удалось загрузить в Google Drive", -1)
                return False, screenshot_paths
        except Exception as e:
            self._update_status(f"❌ Ошибка обработки URL: {e}", -1)
            failure_path = os.path.join(temp_screenshots_dir, f"FAILURE_screenshot.png")
            try: 
                page.screenshot(path=failure_path, full_page=True, timeout=15000)
            except: 
                pass
            self.create_and_upload_session_archive(temp_screenshots_dir, session_id, is_failure=True)
            return False, []
        finally:
            shutil.rmtree(temp_screenshots_dir, ignore_errors=True)

def choose_safety_mode(self):
    self._update_status("\n🛡️ Выберите режим безопасности:", -1)
    self._update_status("1. 🐌 МЕДЛЕННЫЙ (3-8 сек между URL, батчи по 10)", -1)
    self._update_status("2. ⚡ ОБЫЧНЫЙ (2-5 сек между URL, батчи по 20)", -1)
    self._update_status("3. 🚀 БЫСТРЫЙ (1-3 сек между URL, батчи по 30)", -1)
    while True:
        try:
            choice = input("\nВведите номер (1-3): ").strip()
            if choice == "1":
                return {
                    'min_delay': 3, 
                    'max_delay': 8, 
                    'batch_size': 10,
                    'batch_pause_min': 60, 
                    'batch_pause_max': 120, 
                    'name': 'МЕДЛЕННЫЙ'
                }
            elif choice == "2":
                return {
                    'min_delay': 2, 
                    'max_delay': 5, 
                    'batch_size': 20,
                    'batch_pause_min': 30, 
                    'batch_pause_max': 60, 
                    'name': 'ОБЫЧНЫЙ'
                }
            elif choice == "3":
                return {
                    'min_delay': 1, 
                    'max_delay': 3, 
                    'batch_size': 30,
                    'batch_pause_min': 15, 
                    'batch_pause_max': 30, 
                    'name': 'БЫСТРЫЙ'
                }
            else:
                self._update_status("❌ Введите число от 1 до 3", -1)
        except ValueError:
            self._update_status("❌ Введите корректное число", -1)