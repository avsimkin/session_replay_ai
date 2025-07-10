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
import io
import psutil

# Импорты для работы с Google API
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.cloud import bigquery
from google.oauth2 import service_account

# Конфигурация пути к корню проекта для импорта настроек
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

# Константы
PROCESS_TIMEOUT_PER_URL = 180  # Снижено до 180 секунд, как в локальном коде
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
]

class DriveOAuthClient:
    """Клиент для работы с Google Drive через OAuth"""
    
    def __init__(self):
        self.service = None
        self.scopes = ['https://www.googleapis.com/auth/drive.file']
        
    def authenticate(self):
        """Авторизация через сохраненные токены"""
        try:
            refresh_token = os.environ.get('GOOGLE_REFRESH_TOKEN')
            client_id = os.environ.get('GOOGLE_CLIENT_ID')
            client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
            
            if not all([refresh_token, client_id, client_secret]):
                raise ValueError("Не все переменные окружения настроены")
            
            creds = Credentials(
                token=None,
                refresh_token=refresh_token,
                token_uri='https://oauth2.googleapis.com/token',
                client_id=client_id,
                client_secret=client_secret,
                scopes=self.scopes
            )
            
            creds.refresh(Request())
            self.service = build('drive', 'v3', credentials=creds)
            print("✅ OAuth авторизация в Google Drive успешна")
            return True
        except Exception as e:
            print(f"❌ Ошибка OAuth авторизации: {e}")
            return False
    
    def upload_file(self, file_path, file_name=None, folder_id=None):
        """Загрузить файл в Google Drive"""
        try:
            if not self.service:
                if not self.authenticate():
                    return None
            
            if not file_name:
                file_name = os.path.basename(file_path)
            
            file_metadata = {'name': file_name}
            if folder_id:
                file_metadata['parents'] = [folder_id]
            
            with open(file_path, 'rb') as file_data:
                media = MediaIoBaseUpload(
                    io.BytesIO(file_data.read()),
                    mimetype='application/octet-stream',
                    resumable=True
                )
            
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,webViewLink'
            ).execute()
            
            print(f"✅ Файл загружен: {file.get('name')}")
            print(f"🔗 Ссылка: {file.get('webViewLink')}")
            
            return {
                'id': file.get('id'),
                'name': file.get('name'),
                'webViewLink': file.get('webViewLink')
            }
        except Exception as e:
            print(f"❌ Ошибка загрузки файла: {e}")
            return None

def sanitize_cookies(cookies):
    """Проверяет и исправляет cookies для соответствия формату Playwright"""
    if not cookies:
        return []
    valid_same_site_values = {"Strict", "Lax", "None"}
    sanitized_cookies = []
    for cookie in cookies:
        if cookie.get('sameSite') not in valid_same_site_values:
            original_value = cookie.get('sameSite', 'КЛЮЧ ОТСУТСТВОВАЛ')
            print(f"⚠️ Исправляю sameSite='{original_value}' на 'Lax' для куки: {cookie.get('name')}")
            cookie['sameSite'] = 'Lax'
        sanitized_cookies.append(cookie)
    return sanitized_cookies

def worker_process_url(collector_config: dict, url_data: dict, result_queue: multiprocessing.Queue):
    """Обработка URL в отдельном процессе"""
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
            user_agent = random.choice(USER_AGENTS)
            context = browser.new_context(
                user_agent=user_agent, viewport={'width': 1366, 'height': 768},
                locale='en-US', timezone_id='America/New_York'
            )
            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                window.navigator.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            """)
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
            self.min_duration_seconds = int(os.environ.get('MIN_DURATION_SECONDS', '20'))
            self.start_time = None
            self.total_processed, self.total_successful, self.total_failed, self.total_timeouts = 0, 0, 0, 0
            self._update_status("🔐 Настраиваем подключения...", 1)
            self.cookies = self._load_cookies_from_secret_file()
        
        self.full_table_name = f"`{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table_id}`"
        self._init_bigquery()
        self._init_google_drive_oauth()

    def _update_status(self, details: str, progress: int):
        """Обновление статуса с форматированным выводом"""
        if self.status_callback:
            self.status_callback(details, progress)
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"\n{'=' * 20} [{timestamp}] {'=' * 20}\n{details}\n{'=' * 50}")

    def _load_cookies_from_secret_file(self, verbose=True):
        """Загрузка cookies из файла"""
        if verbose:
            self._update_status(f"Загрузка cookies из {self.cookies_path}...", 2)
        if not os.path.exists(self.cookies_path):
            if verbose:
                self._update_status(f"❌ Файл cookies не найден!", 2)
            return []
        try:
            with open(self.cookies_path, 'r') as f:
                cookies = json.load(f)
            if verbose:
                self._update_status(f"✅ Cookies загружены ({len(cookies)} шт).", 3)
            return cookies
        except Exception as e:
            if verbose:
                self._update_status(f"❌ Ошибка чтения cookies: {e}", 3)
            return []

    def _init_bigquery(self):
        """Инициализация BigQuery"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path, scopes=["https://www.googleapis.com/auth/bigquery"])
            self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project_id)
            self._update_status("✅ BigQuery подключен", 4)
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к BigQuery: {e}")

    def _init_google_drive_oauth(self):
        """Инициализация Google Drive через OAuth"""
        try:
            self.drive_client = DriveOAuthClient()
            if not self.drive_client.authenticate():
                raise Exception("Не удалось авторизоваться в Google Drive")
            self._update_status("✅ Google Drive OAuth подключен", 5)
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к Google Drive: {e}")

    def get_unprocessed_urls(self, limit=None):
        """Получение необработанных URL из BigQuery"""
        query = f"""
        SELECT session_replay_url, amplitude_id, session_replay_id, duration_seconds, events_count, record_date 
        FROM {self.full_table_name} 
        WHERE is_processed = FALSE AND duration_seconds >= {self.min_duration_seconds} 
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
                    'record_date': row.record_date.strftime('%Y-%m-%d') if hasattr(row.record_date, 'strftime') else str(row.record_date)
                })
            self._update_status(f"📊 Найдено {len(urls_data)} необработанных URL", -1)
            return urls_data
        except Exception as e:
            self._update_status(f"❌ Ошибка получения URL: {e}", -1)
            raise

    def mark_url_as_processed(self, url, success=True):
        """Отметка URL как обработанного в BigQuery"""
        try:
            update_query = f"""
            UPDATE {self.full_table_name} 
            SET is_processed = TRUE 
            WHERE session_replay_url = @url
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("url", "STRING", url)]
            )
            self.bq_client.query(update_query, job_config=job_config).result()
            status = "✅" if success else "❌"
            self._update_status(f"{status} URL отмечен как обработанный", -1)
        except Exception as e:
            self._update_status(f"❌ Ошибка обновления статуса URL {url}: {e}", -1)

    def get_session_id_from_url(self, url):
        """Извлечение ID сессии из URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        if "sessionReplayId=" in url:
            parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
            session_replay_id = parts[0]
            session_start_time = parts[1] if len(parts) > 1 else "unknown"
            return f"{session_replay_id}_{session_start_time}_{url_hash}"
        return f"no_session_id_{url_hash}"

    def login_and_update_cookies(self, page, max_retries=3):
        """Автоматическая авторизация с повторными попытками"""
        login = os.environ.get('AMPLITUDE_LOGIN')
        password = os.environ.get('AMPLITUDE_PASSWORD')
        if not login or not password:
            self._update_status("❌ Переменные AMPLITUDE_LOGIN и/или AMPLITUDE_PASSWORD не установлены!", -1)
            return False
        for attempt in range(max_retries):
            self._update_status(f"⚠️ Попытка авторизации {attempt + 1}/{max_retries}...", -1)
            try:
                page.goto("https://app.amplitude.com/login", timeout=60000)
                self._update_status("    Вводим логин...", -1)
                page.fill('input[name="username"]', login)
                page.click('button[type="submit"]')
                self._update_status("    Вводим пароль...", -1)
                password_input = page.wait_for_selector('input[name="password"]', timeout=15000)
                password_input.fill(password)
                page.click('button[type="submit"]')
                self._update_status("    Ожидание успешного входа...", -1)
                page.wait_for_url(lambda url: "login" not in url, timeout=60000)
                page.wait_for_selector("nav", timeout=30000)
                self._update_status("✅ Авторизация прошла успешно!", -1)
                self._update_status("    Сохраняем новые cookies...", -1)
                new_cookies = page.context.cookies()
                with open(self.cookies_path, 'w') as f:
                    json.dump(new_cookies, f)
                self.cookies = new_cookies
                return True
            except Exception as e:
                self._update_status(f"❌ Ошибка во время авторизации: {e}", -1)
                try:
                    page.screenshot(path="login_error_screenshot.png", full_page=True)
                    self._update_status("    Скриншот ошибки авторизации сохранен", -1)
                except:
                    pass
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(5, 10))
        self._update_status("❌ Не удалось авторизоваться после всех попыток", -1)
        return False

    def simulate_human_behavior(self, page):
        """Имитация человеческого поведения"""
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

    def wait_for_content(self, page, selector, bad_texts=("Loading", "Loading summary"), timeout=10, min_text_length=10):
        """Ожидание загрузки контента"""
        self._update_status(f"⏳ Ждем загрузку контента (таймаут {timeout} сек)...", -1)
        start = time.time()
        last_log = 0
        while True:
            el = page.query_selector(selector)
            if el:
                txt = el.inner_text().strip()
                if txt and all(bad not in txt for bad in bad_texts) and len(txt) >= min_text_length:
                    self._update_status(f"✅ Контент загружен за {time.time() - start:.1f} сек", -1)
                    return el
            elapsed = time.time() - start
            if elapsed - last_log >= 3:
                self._update_status(f"⏳ Ожидание... {elapsed:.1f}/{timeout} сек", -1)
                last_log = elapsed
            if elapsed > timeout:
                self._update_status(f"⚠️ Контент не загрузился за {timeout} сек", -1)
                return None
            time.sleep(0.5)

    def screenshot_userinfo_block(self, page, session_id, base_dir):
        """Скриншот блока с информацией о пользователе"""
        os.makedirs(base_dir, exist_ok=True)
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

    def screenshot_summary_flexible(self, page, session_id, base_dir, summary_el=None):
        """Гибкий скриншот блока Summary"""
        os.makedirs(base_dir, exist_ok=True)
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
                'p:has-text("session")',
                '[data-testid*="summary"]',
                'div[class*="summary"] p'
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
        """Универсальный скриншот блока по заголовку"""
        os.makedirs(base_dir, exist_ok=True)
        self._update_status(f"🔍 Ищем блок '{block_title}'...", -1)
        el = None
        
        search_selectors = [
            f'h4:has-text("{block_title}")',
            f'h3:has-text("{block_title}")',
            f'h2:has-text("{block_title}")',
            f'div:has-text("{block_title}")',
            f'span:has-text("{block_title}")',
            f'h5:has-text("{block_title}")',
            f'[title="{block_title}"]',
            f'[aria-label="{block_title}"]',
            f'[data-testid*="{block_title.lower()}"]'
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
            except Exception:
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

    def create_session_folder_structure(self, session_id, screenshots, url_data):
        """Создание структуры папки сессии"""
        session_dir = tempfile.mkdtemp(prefix=f"session_folder_{session_id}_")
        for screenshot_path in screenshots:
            if screenshot_path and os.path.exists(screenshot_path):
                shutil.copy2(screenshot_path, session_dir)
        metadata = {
            "session_id": session_id,
            "url": url_data['url'],
            "amplitude_id": url_data['amplitude_id'],
            "session_replay_id": url_data['session_replay_id'],
            "duration_seconds": url_data['duration_seconds'],
            "events_count": url_data['events_count'],
            "record_date": url_data.get('record_date', ''),
            "processed_at": datetime.now().isoformat(),
            "screenshots": [os.path.basename(path) for path in screenshots if path]
        }
        with open(os.path.join(session_dir, "metadata.json"), 'w') as f:
            json.dump(metadata, f, indent=2)
        return session_dir

    def upload_to_google_drive(self, file_path, filename, folder_id):
        """Загрузка файла в Google Drive"""
        try:
            return self.drive_client.upload_file(file_path, filename, folder_id)
        except Exception as e:
            self._update_status(f"❌ Ошибка загрузки в Google Drive: {e}", -1)
            return None

    def create_and_upload_session_archive(self, session_dir, session_id, is_failure=False):
        """Создание и загрузка архива сессии"""
        try:
            prefix = "FAILURE" if is_failure else "session_replay"
            archive_name = f"{prefix}_{session_id}_{int(time.time())}.zip"
            archive_path_base = os.path.join(tempfile.gettempdir(), archive_name.replace('.zip',''))
            archive_path = shutil.make_archive(archive_path_base, 'zip', session_dir)
            
            self._update_status(f"📦 Создан архив: {archive_name}", -1)
            uploaded_file = self.upload_to_google_drive(archive_path, archive_name, self.gdrive_folder_id)
            if uploaded_file:
                self._update_status(f"☁️ Архив загружен в Google Drive", -1)
                self._update_status(f"🔗 Ссылка: {uploaded_file.get('webViewLink', 'N/A')}", -1)
            return uploaded_file
        except Exception as e:
            self._update_status(f"❌ Ошибка создания/загрузки архива: {e}", -1)
            return None
        finally:
            if 'session_dir' in locals() and os.path.exists(session_dir):
                shutil.rmtree(session_dir, ignore_errors=True)
            if 'archive_path' in locals() and os.path.exists(archive_path):
                os.remove(archive_path)

    def process_single_url(self, page, url_data):
        """Обработка одного URL"""
        url = url_data['url']
        session_id = self.get_session_id_from_url(url)
        temp_screenshots_dir = tempfile.mkdtemp(prefix=f"screenshots_{session_id}_")
        REQUIRED_BLOCKS = ['userinfo', 'summary', 'sentiment']
        screenshot_paths = []
        
        try:
            self._update_status(f"▶️ Обрабатываем сессию: {session_id}", -1)
            self.simulate_human_behavior(page)
            
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            time.sleep(random.uniform(2, 5))

            if "/login" in page.url:
                login_successful = self.login_and_update_cookies(page)
                if not login_successful:
                    return False, []
                self._update_status("    Возвращаемся к исходной ссылке...", -1)
                page.goto(url, timeout=60000, wait_until='domcontentloaded')
                time.sleep(random.uniform(2, 5))

            summary_tab = page.query_selector("text=Summary")
            if summary_tab:
                try:
                    self.simulate_human_behavior(page)
                    summary_tab.click()
                    self._update_status("🖱️ Кликнули на Summary", -1)
                    time.sleep(random.uniform(3, 6))
                    summary_el = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=20)
                except PlaywrightError as e:
                    self._update_status(f"⚠️ Ошибка клика на Summary: {e}", -1)
                    try:
                        summary_tab = page.wait_for_selector("text=Summary", timeout=5000)
                        summary_tab.click(force=True)
                        self._update_status("🖱️ Кликнули на Summary (force)", -1)
                        time.sleep(random.uniform(3, 6))
                        summary_el = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=20)
                    except Exception as e2:
                        self._update_status(f"❌ Не удалось кликнуть на Summary: {e2}", -1)
                        return False, []
            else:
                self._update_status("❌ Вкладка Summary не найдена!", -1)
                return False, []

            screenshot_results = {}
            self._update_status("\n📸 Начинаем создание скриншотов...", -1)

            self._update_status("\n1️⃣ User Info блок:", -1)
            userinfo_path = self.screenshot_userinfo_block(page, session_id, temp_screenshots_dir)
            screenshot_results['userinfo'] = userinfo_path is not None
            if userinfo_path:
                screenshot_paths.append(userinfo_path)
            time.sleep(random.uniform(1, 2))

            self._update_status("\n2️⃣ Summary блок:", -1)
            summary_paths = self.screenshot_summary_flexible(page, session_id, temp_screenshots_dir, summary_el=summary_el)
            screenshot_results['summary'] = len(summary_paths) > 0
            if summary_paths:
                screenshot_paths.extend(summary_paths)
            time.sleep(random.uniform(1, 2))

            self._update_status("\n3️⃣ Sentiment блок:", -1)
            sentiment_path = self.screenshot_by_title(page, "Sentiment", session_id, temp_screenshots_dir)
            screenshot_results['sentiment'] = sentiment_path is not None
            if sentiment_path:
                screenshot_paths.append(sentiment_path)
            time.sleep(random.uniform(1, 2))

            self._update_status("\n4️⃣ Actions блок:", -1)
            actions_path = self.screenshot_by_title(page, "Actions", session_id, temp_screenshots_dir)
            screenshot_results['actions'] = actions_path is not None
            if actions_path:
                screenshot_paths.append(actions_path)

            self._update_status("\n📊 Результаты скриншотов:", -1)
            for block, success in screenshot_results.items():
                status = "✅" if success else "❌"
                self._update_status(f"   {status} {block.capitalize()}", -1)

            all_required_success = all(screenshot_results.get(block, False) for block in REQUIRED_BLOCKS)
            total_blocks = len([path for path in screenshot_paths if path and os.path.exists(path)])

            self._update_status("\n🎯 Анализ качества:", -1)
            self._update_status(f"   📋 Все обязательные блоки: {'✅' if all_required_success else '❌'}", -1)
            self._update_status(f"   📸 Всего скриншотов: {total_blocks}", -1)

            if not all_required_success or total_blocks < 3:
                self._update_status(f"❌ Не получены все обязательные блоки или меньше 3 скриншотов ({total_blocks}).", -1)
                return False, screenshot_paths

            session_dir = self.create_session_folder_structure(session_id, screenshot_paths, url_data)
            uploaded_file = self.create_and_upload_session_archive(session_dir, session_id)

            if uploaded_file:
                for path in screenshot_paths:
                    if path and os.path.exists(path):
                        os.remove(path)
                return True, screenshot_paths
            else:
                self._update_status("❌ Не удалось загрузить архив", -1)
                return False, screenshot_paths

        except Exception as e:
            self._update_status(f"❌ Ошибка при обработке URL {url}: {e}", -1)
            import traceback
            traceback.print_exc()
            
            failure_path = os.path.join(temp_screenshots_dir, f"FAILURE_screenshot.png")
            try:
                page.screenshot(path=failure_path, full_page=True, timeout=15000)
                self._update_status("    Скриншот ошибки сохранен.", -1)
                screenshot_paths.append(failure_path)
            except Exception as e_scr:
                self._update_status(f"    Не удалось сделать скриншот ошибки: {e_scr}", -1)
            
            try:
                html_path = os.path.join(temp_screenshots_dir, f"FAILURE_page_content.html")
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(page.content())
                self._update_status("    HTML контент сохранен для диагностики.", -1)
            except Exception as e_html:
                self._update_status(f"    Не удалось сохранить HTML: {e_html}", -1)
            
            self.create_and_upload_session_archive(temp_screenshots_dir, session_id, is_failure=True)
            return False, screenshot_paths
        finally:
            if 'temp_screenshots_dir' in locals() and os.path.exists(temp_screenshots_dir):
                shutil.rmtree(temp_screenshots_dir, ignore_errors=True)
            if 'session_dir' in locals() and os.path.exists(session_dir):
                shutil.rmtree(session_dir, ignore_errors=True)

    def get_safety_settings(self):
        """Получение настроек режима безопасности"""
        safety_mode = os.environ.get('SAFETY_MODE', 'normal').lower()
        settings = {
            'slow': {'min_delay': 3, 'max_delay': 8, 'batch_size': 10, 'batch_pause_min': 60, 'batch_pause_max': 120, 'name': 'МЕДЛЕННЫЙ'},
            'normal': {'min_delay': 2, 'max_delay': 5, 'batch_size': 20, 'batch_pause_min': 30, 'batch_pause_max': 60, 'name': 'ОБЫЧНЫЙ'},
            'fast': {'min_delay': 1, 'max_delay': 3, 'batch_size': 30, 'batch_pause_min': 15, 'batch_pause_max': 30, 'name': 'БЫСТРЫЙ'}
        }
        return settings.get(safety_mode, settings['normal'])

    def get_url_count(self, total_urls):
        """Получение количества URL для обработки"""
        try:
            count = int(os.environ.get('URL_COUNT', total_urls))
            return min(count, total_urls)
        except ValueError:
            return total_urls

    def print_progress(self, current, total, start_time, successful, failed, timeouts):
        """Вывод прогресса с ETA"""
        elapsed = time.time() - start_time
        percent = (current / total) * 100
        eta = "неизвестно"
        if current > 0:
            avg_time = elapsed / current
            remaining = (total - current) * avg_time
            remaining_min = remaining / 60
            eta = f"{remaining_min / 60:.1f}ч" if remaining_min > 60 else f"{remaining_min:.1f}мин"
        self._update_status(f"📊 Обработано: {current}/{total} ({percent:.1f}%) | ⏳ Осталось: ~{eta}", -1)
        self._update_status(f"✅ Успешно: {successful} | ❌ Ошибок: {failed} | ❗ Зависаний: {timeouts}", -1)
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        self._update_status(f"🖥️ CPU: {cpu_percent}% | 🧠 Память: {memory.percent}% использована", -1)

    def process_batch(self, urls_batch, safety_settings):
        """Обработка батча URL"""
        batch_start_time = time.time()
        batch_successful, batch_failed, batch_timeouts = 0, 0, 0
        self._update_status(f"🚀 Начинаем обработку батча из {len(urls_batch)} URL...", -1)
        result_queue = multiprocessing.Queue()
        collector_config = {
            "credentials_path": self.credentials_path,
            "gdrive_folder_id": self.gdrive_folder_id,
            "bq_project_id": self.bq_project_id,
            "bq_dataset_id": self.bq_dataset_id,
            "bq_table_id": self.bq_table_id,
            "min_duration_seconds": self.min_duration_seconds,
            "cookies_path": self.cookies_path
        }
        
        for i, url_data in enumerate(urls_batch, 1):
            self._update_status(f"▶️ [{i}/{len(urls_batch)}] Запускаем процесс для URL ...{url_data['url'][-40:]}", -1)
            process = multiprocessing.Process(target=worker_process_url, args=(collector_config, url_data, result_queue))
            process.start()
            process.join(timeout=PROCESS_TIMEOUT_PER_URL)
            
            if process.is_alive():
                self._update_status(f"❗ ТАЙМАУТ! Процесс для URL ...{url_data['url'][-40:]} завис. Завершаем.", -1)
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
                    else:
                        batch_failed += 1
                except queue.Empty:
                    batch_failed += 1
                    self.mark_url_as_processed(url_data['url'], success=False)
            
            if i < len(urls_batch):
                delay = random.uniform(safety_settings['min_delay'], safety_settings['max_delay'])
                self._update_status(f"⏱️ Пауза {delay:.1f} сек...", -1)
                time.sleep(delay)
            
            if i % 5 == 0 or i == len(urls_batch):
                self.print_progress(i, len(urls_batch), batch_start_time, batch_successful, batch_failed, batch_timeouts)
        
        self.total_processed += len(urls_batch)
        self.total_successful += batch_successful
        self.total_failed += batch_failed
        self.total_timeouts += batch_timeouts
        batch_time = time.time() - batch_start_time
        self._update_status(f"📦 Батч завершен за {batch_time/60:.1f} мин. [Успешно: {batch_successful}, Ошибок: {batch_failed}, Зависаний: {batch_timeouts}]", -1)
        
    def run(self):
        """Запуск обработки"""
        self.start_time = time.time()
        self._update_status("🚀 СБОРЩИК СКРИНШОТОВ SESSION REPLAY", 10)
        self._update_status("BigQuery → Screenshots → Google Drive", -1)
        
        safety_settings = self.get_safety_settings()
        self._update_status(f"🛡️ Режим безопасности: {safety_settings['name']}", -1)
        self._update_status(f"⏱️ Таймаут на 1 URL: {PROCESS_TIMEOUT_PER_URL} сек", -1)
        self._update_status(f"☁️ Google Drive папка: {self.gdrive_folder_id}", -1)

        urls_data = self.get_unprocessed_urls()
        if not urls_data:
            self._update_status("🎉 Все URL уже обработаны!", -1)
            return

        count_to_process = self.get_url_count(len(urls_data))
        urls_to_process = urls_data[:count_to_process]
        self._update_status(f"🎯 Будет обработано: {len(urls_to_process)} URL", -1)

        try:
            for i in range(0, len(urls_to_process), safety_settings['batch_size']):
                batch = urls_to_process[i:i + safety_settings['batch_size']]
                self.process_batch(batch, safety_settings)
                if i + safety_settings['batch_size'] < len(urls_to_process):
                    batch_pause = random.uniform(safety_settings['batch_pause_min'], safety_settings['batch_pause_max'])
                    self._update_status(f"⏸️ Пауза между батчами: {batch_pause:.1f} сек...", -1)
                    time.sleep(batch_pause)
        except KeyboardInterrupt:
            self._update_status("⚠️ Получен сигнал остановки.", -1)
        except Exception as e:
            self._update_status(f"❌ Критическая ошибка: {e}", -1)
            import traceback
            traceback.print_exc()
        self.print_overall_stats()

    def print_overall_stats(self):
        """Вывод общей статистики"""
        if self.start_time:
            elapsed = time.time() - self.start_time
            elapsed_hours = elapsed / 3600
            success_rate = (self.total_successful / self.total_processed * 100) if self.total_processed > 0 else 0
            self._update_status("=" * 60, -1)
            self._update_status("📊 ОБЩАЯ СТАТИСТИКА РАБОТЫ", -1)
            self._update_status(f"⏱️ Время работы: {elapsed_hours:.1f} часов", -1)
            self._update_status(f"📈 Всего обработано: {self.total_processed} URL", -1)
            self._update_status(f"✅ Успешно: {self.total_successful}", -1)
            self._update_status(f"❌ Ошибок: {self.total_failed}", -1)
            self._update_status(f"❗ Зависаний (Timeout): {self.total_timeouts}", -1)
            self._update_status(f"📊 Процент успеха: {success_rate:.1f}%", -1)
            if self.total_processed > 0:
                avg_time_per_url = elapsed / self.total_processed
                self._update_status(f"⚡ Среднее время на URL: {avg_time_per_url:.1f} сек", -1)
            self._update_status("=" * 60, -1)

def main():
    """Основная функция"""
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