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

# Константы - взяты из локального кода
PROCESS_TIMEOUT = 180  # 3 минуты как в локальном коде
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
]

class DriveOAuthClient:
    """Клиент для работы с Google Drive через OAuth - улучшенная версия из локального кода"""
    
    def __init__(self):
        self.service = None
        self.scopes = ['https://www.googleapis.com/auth/drive.file']
        
    def authenticate(self):
        """Авторизация через сохраненные токены"""
        try:
            # Сначала пробуем встроенные значения, потом переменные окружения (как в локальном коде)
            refresh_token = os.environ.get('GOOGLE_REFRESH_TOKEN',
                                           '1//03T0-itzPoL_wCgYIARAAGAMSNwF-L9Irf0MkzkOaGyIoyuwgd40W4BNDS8LG3vHxLJpbVsKNoWHMiLTomq4TjOlEz-2UN2GLMeg')
            client_id = os.environ.get('GOOGLE_CLIENT_ID',
                                       '660095903838-k0bcv8shborcr6u54hrpv9761vr2bcml.apps.googleusercontent.com')
            client_secret = os.environ.get('GOOGLE_CLIENT_SECRET', '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')
            
            if not all([refresh_token, client_id, client_secret]):
                raise ValueError("Не все токены OAuth настроены")
            
            # Создаем credentials
            creds = Credentials(
                token=None,
                refresh_token=refresh_token,
                token_uri='https://oauth2.googleapis.com/token',
                client_id=client_id,
                client_secret=client_secret,
                scopes=self.scopes
            )
            
            # Обновляем токен
            creds.refresh(Request())
            
            # Создаем сервис
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
            
            # Определяем имя файла
            if not file_name:
                file_name = os.path.basename(file_path)
            
            # Метаданные файла
            file_metadata = {'name': file_name}
            if folder_id:
                file_metadata['parents'] = [folder_id]
            
            # Читаем файл
            with open(file_path, 'rb') as file_data:
                media = MediaIoBaseUpload(
                    io.BytesIO(file_data.read()),
                    mimetype='application/octet-stream',
                    resumable=True
                )
            
            # Загружаем файл
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
    """Проверяет и исправляет cookies, чтобы они соответствовали формату Playwright."""
    if not cookies:
        return []
    valid_same_site_values = {"Strict", "Lax", "None"}
    sanitized_cookies = []
    for cookie in cookies:
        if cookie.get('sameSite') not in valid_same_site_values:
            original_value = cookie.get('sameSite', 'КЛЮЧ ОТСУТСТВОВАЛ')
            print(f"⚠️ Исправляю невалидный/отсутствующий sameSite='{original_value}' на 'Lax' для куки: {cookie.get('name')}")
            cookie['sameSite'] = 'Lax'
        sanitized_cookies.append(cookie)
    return sanitized_cookies

def worker_process_url(url_data, collector_config, safety_settings, result_queue):
    """Эта функция выполняется в отдельном процессе для изоляции и контроля зависаний."""
    try:
        collector_config['verbose'] = False
        collector = RenderScreenshotCollector(config_override=collector_config)
        sanitized_cookies = sanitize_cookies(collector.cookies)

        with sync_playwright() as p:
            # Улучшенные browser_args из локального кода
            browser_args = [
                '--no-proxy-server',
                '--disable-proxy-config-service',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-background-networking',
                '--disable-ipc-flooding-protection',
                '--aggressive-cache-discard',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-default-apps',
                '--no-first-run',
                '--disable-infobars'
            ]

            browser = p.chromium.launch(
                headless=True,
                args=browser_args,
                slow_mo=500
            )

            user_agent = random.choice(USER_AGENTS)

            context = browser.new_context(
                user_agent=user_agent,
                viewport={'width': 1366, 'height': 768},
                locale='en-US',
                timezone_id='America/New_York',
                ignore_https_errors=True,
                java_script_enabled=True,
                accept_downloads=False,
                bypass_csp=True
            )

            context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: function() { return undefined; }});
                window.navigator.chrome = { runtime: {} };
            """)

            context.add_cookies(sanitized_cookies)
            page = context.new_page()

            success, _ = collector.process_single_url(page, url_data, safety_settings)
            if success:
                collector.mark_url_as_processed(url_data['url'], success)
            result_queue.put(success)

            page.close()
            context.close()
            browser.close()

    except Exception as e:
        print(f"❌ [Критическая ошибка в дочернем процессе] URL: {url_data.get('url', 'N/A')}. Ошибка: {e}")
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
            self.max_duration_seconds = config_override.get("max_duration_seconds", 3600)  # Добавлено из локального кода
            self.cookies_path = config_override["cookies_path"]
            self.status_callback = None
            self.verbose = config_override.get('verbose', True)
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
            self.max_duration_seconds = int(os.environ.get('MAX_DURATION_SECONDS', '3600'))  # Добавлено
            self.verbose = True
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
        if self.verbose:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"[{timestamp}] {details}")

    def _load_cookies_from_secret_file(self, verbose=True):
        """Загрузка cookies из файла"""
        if verbose and self.verbose:
            self._update_status(f"Загрузка cookies из {self.cookies_path}...", 2)
        if not os.path.exists(self.cookies_path):
            if verbose and self.verbose:
                self._update_status(f"❌ Файл cookies не найден!", 2)
            return []
        try:
            with open(self.cookies_path, 'r') as f:
                cookies = json.load(f)
            if verbose and self.verbose:
                self._update_status(f"✅ Cookies загружены ({len(cookies)} шт).", 3)
            return cookies
        except Exception as e:
            if verbose and self.verbose:
                self._update_status(f"❌ Ошибка чтения cookies: {e}", 3)
            return []

    def _init_bigquery(self):
        """Инициализация BigQuery"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path, scopes=["https://www.googleapis.com/auth/bigquery"])
            self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project_id)
            if self.verbose:
                self._update_status("✅ BigQuery подключен", 4)
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к BigQuery: {e}")

    def _init_google_drive_oauth(self):
        """Инициализация Google Drive через OAuth"""
        try:
            self.drive_client = DriveOAuthClient()
            if not self.drive_client.authenticate():
                raise Exception("Не удалось авторизоваться в Google Drive")
            if self.verbose:
                self._update_status("✅ Google Drive OAuth подключен", 5)
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к Google Drive: {e}")

    def get_unprocessed_urls(self, limit=None):
        """Получение необработанных URL с фильтром по длительности сессии и сортировкой по дате"""
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
        AND duration_seconds <= {self.max_duration_seconds}
        ORDER BY record_date
        """
        if limit:
            query += f"\nLIMIT {limit}"

        if self.verbose:
            print(f"🔍 Получаем необработанные URL из BigQuery...")
            print(f"⏱️ Длительность сессий: от {self.min_duration_seconds} до {self.max_duration_seconds} сек")

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
            if self.verbose:
                print(f"📊 Найдено {len(urls_data)} необработанных URL")
                # Логируем первые несколько записей для проверки
                for i, url_data in enumerate(urls_data[:3], 1):
                    print(f"URL {i}: record_date={url_data['record_date']}, duration_seconds={url_data['duration_seconds']}")
            return urls_data
        except Exception as e:
            print(f"❌ Ошибка получения URL: {e}")
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
            status_message = "✅ URL отмечен как обработанный" if success else "⚠️ URL отмечен как обработанный (с ошибкой)"
            if self.verbose:
                print(status_message)
        except Exception as e:
            print(f"❌ Ошибка обновления статуса URL: {e}")

    def get_session_id_from_url(self, url):
        """Извлечение ID сессии из URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        if "sessionReplayId=" in url:
            parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
            session_replay_id = parts[0]
            session_start_time = parts[1] if len(parts) > 1 else "unknown"
            return f"{session_replay_id}_{session_start_time}_{url_hash}"
        return f"no_session_id_{url_hash}"

    def wait_for_content(self, page, selector, bad_texts=("Loading", "Loading summary"), timeout=60, min_text_length=10, retries=3):
        """Ожидание загрузки контента с увеличенным таймаутом и попытками - из локального кода"""
        print(f"⏳ Ждем загрузку контента (таймаут {timeout} сек, попыток {retries})...")
        for attempt in range(retries):
            start = time.time()
            last_log = 0
            while True:
                el = page.query_selector(selector)
                if el:
                    txt = el.inner_text().strip()
                    if txt and all(bad not in txt for bad in bad_texts) and len(txt) >= min_text_length:
                        print(f"✅ Контент загружен за {time.time() - start:.1f} сек на попытке {attempt + 1}")
                        return el
                elapsed = time.time() - start
                if elapsed - last_log >= 5:
                    print(f"⏳ Ожидание (попытка {attempt + 1})... {elapsed:.1f}/{timeout} сек")
                    last_log = elapsed
                if elapsed > timeout:
                    print(f"⚠️ Контент не загрузился за {timeout} сек на попытке {attempt + 1}")
                    break
                time.sleep(0.5)
            if attempt < retries - 1:
                time.sleep(5)  # Пауза перед следующей попыткой
        return None

    def simulate_human_behavior(self, page, full_scroll=False):
        """Имитация человеческого поведения с увеличенной вероятностью прокрутки - из локального кода"""
        try:
            for _ in range(random.randint(3, 5)):
                x = random.randint(200, 1200)
                y = random.randint(200, 700)
                page.mouse.move(x, y, steps=random.randint(5, 15))
                time.sleep(random.uniform(0.2, 0.5))
            if random.random() < 0.8 or full_scroll:
                for _ in range(2):
                    scroll_amount = random.randint(200, 600)
                    direction = random.choice([1, -1])
                    page.evaluate(f"window.scrollBy(0, {scroll_amount * direction})")
                    time.sleep(random.uniform(1.0, 2.0))
            if random.random() < 0.5:
                safe_x = random.randint(50, 1300)
                safe_y = random.randint(50, 150)
                page.mouse.click(safe_x, safe_y)
                time.sleep(random.uniform(0.5, 1.0))
        except Exception:
            pass

    def screenshot_summary_flexible(self, page, session_id, base_dir="screens", summary_el=None):
        """Улучшенный скриншот Summary блока - взято из локального кода"""
        os.makedirs(base_dir, exist_ok=True)
        print("📄 Ищем Summary блок...")

        el = summary_el
        if not el:
            # Если summary_el не передан, ищем контент после клика на Summary
            print("   Summary элемент не передан, ищем на странице...")

            # Ждем немного для загрузки
            time.sleep(3)

            # ПРИОРИТЕТ: Ищем именно текстовую часть Summary, БЕЗ заголовка
            text_only_selectors = [
                'p.ltext-_uoww22',  # Основной селектор для текста Summary
                'div:has(p.ltext-_uoww22) p.ltext-_uoww22',  # Точно текст внутри контейнера
                '[data-testid="session-replay-summary"] p',  # Параграф внутри Summary
                'div[class*="summary"] p:not(:has(button))',  # Параграф без кнопок
                'p[class*="ltext"]:not(:has(button))'  # Текст без кнопок
            ]

            for selector in text_only_selectors:
                try:
                    el = page.query_selector(selector)
                    if el:
                        text = el.inner_text().strip()
                        if text and len(text) > 20 and "Loading" not in text and "Replay Summary" not in text:
                            print(f"   ✅ Найден текстовый блок через селектор: {selector}")
                            print(f"   Текст: {text[:50]}...")
                            break
                        else:
                            el = None
                except Exception:
                    continue

            # Если не нашли точным селектором, ищем по содержимому
            if not el:
                print("   Ищем Summary текст по содержимому...")
                try:
                    # Ищем только параграфы с текстом Summary (исключаем заголовки)
                    all_paragraphs = page.query_selector_all('p')
                    for paragraph in all_paragraphs:
                        try:
                            text = paragraph.inner_text().strip()
                            bbox = paragraph.bounding_box() if paragraph else None

                            # Фильтры для текста Summary:
                            if (text and len(text) > 50 and len(text) < 2000 and
                                    bbox and bbox['height'] > 30 and  # Минимальная высота
                                    any(word in text.lower() for word in
                                        ['user', 'session', 'the user', 'began', 'placed', 'navigated']) and
                                    "Loading" not in text and
                                    "Replay Summary" not in text and  # Исключаем заголовки
                                    "Summary" not in text and  # Исключаем заголовки
                                    not any(btn in text for btn in ['👍', '👎', 'like', 'dislike'])):  # Исключаем кнопки

                                el = paragraph
                                print(f"   ✅ Найден текст Summary: {text[:50]}...")
                                break
                        except Exception:
                            continue
                except Exception:
                    pass

        if el:
            text_content = el.inner_text().strip()
            if len(text_content) > 20:
                print(f"✅ Summary текст найден (длина: {len(text_content)} символов)")
            else:
                print(f"⚠️ Summary текст слишком короткий ({len(text_content)} символов), пробуем fallback")
                el = None

        # Fallback поиск если основной не сработал
        if not el:
            print("⚠️ Пробуем fallback поиск текста Summary...")

            # Ищем контейнер с Summary и берем только текстовую часть
            try:
                summary_containers = page.query_selector_all(
                    '[data-testid="session-replay-summary"], div:has-text("Replay Summary")')
                for container in summary_containers:
                    # Внутри контейнера ищем только текст
                    text_elements = container.query_selector_all(
                        'p, div:not(:has(button)):not(:has(h1)):not(:has(h2)):not(:has(h3))')

                    for text_el in text_elements:
                        try:
                            text = text_el.inner_text().strip()
                            if (text and len(text) > 50 and
                                    "Replay Summary" not in text and
                                    "Summary" not in text and
                                    any(word in text.lower() for word in ['user', 'session', 'began', 'placed'])):
                                el = text_el
                                print(f"✅ Fallback: найден текст Summary")
                                break
                        except Exception:
                            continue

                    if el:
                        break
            except Exception:
                pass

            if not el:
                print("❌ Не удалось найти текст Summary блока")
                try:
                    html_path = os.path.join(base_dir, f"failure_summary_{session_id}.html")
                    with open(html_path, 'w', encoding='utf-8') as f:
                        f.write(page.content())
                    print(f"    HTML страницы сохранен для диагностики: {html_path}")
                except Exception as e:
                    print(f"    Не удалось сохранить HTML: {e}")
                return []

        # Проверяем, что мы захватываем именно текст, а не весь блок
        try:
            bbox = el.bounding_box()
            if bbox:
                print(f"📏 Размер текста Summary: {bbox['width']}x{bbox['height']} пикселей")

                # Если элемент слишком большой, значит захватили не только текст
                if bbox['height'] > 300:
                    print("⚠️ Элемент слишком большой, ищем только текстовую часть...")

                    # Ищем прямые текстовые элементы внутри
                    inner_texts = el.query_selector_all('p:not(:has(button)), span:not(:has(button))')

                    best_text_element = None
                    max_text_length = 0

                    for inner_text in inner_texts:
                        try:
                            text = inner_text.inner_text().strip()
                            inner_bbox = inner_text.bounding_box()

                            if (text and len(text) > max_text_length and len(text) > 50 and
                                    inner_bbox and inner_bbox['height'] < 250 and
                                    any(word in text.lower() for word in ['user', 'session', 'began'])):
                                best_text_element = inner_text
                                max_text_length = len(text)
                        except Exception:
                            continue

                    if best_text_element:
                        el = best_text_element
                        new_bbox = el.bounding_box()
                        print(f"✅ Найден точный текст: {new_bbox['width']}x{new_bbox['height']}px")

        except Exception as e:
            print(f"⚠️ Ошибка при анализе размера: {e}")

        try:
            img_name = os.path.join(base_dir, f"{session_id}_summary.png")
            el.screenshot(path=img_name)
            print("✅ Summary текст сохранён")
            return [img_name]
        except Exception as e:
            print(f"❌ Ошибка создания скриншота Summary: {e}")
            return []

    def screenshot_by_title(self, page, block_title, session_id, base_dir):
        """Универсальный скриншот блока по заголовку - улучшенная версия из локального кода"""
        os.makedirs(base_dir, exist_ok=True)
        print(f"🔍 Ищем блок '{block_title}'...")
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
            f'[data-testid*="{block_title.lower()}"]',
            f'div[class*="sentiment"]',
            f'div[class*="actions"]',
            f'div:has-text("User Sentiment")',
            f'div:has-text("Session Actions")',
            f'div[class*="heatmap"]',
            f'div:has-text("Guides")'
        ]
        for selector in search_selectors:
            try:
                maybe = page.query_selector(selector)
                if maybe:
                    print(f"📍 Найден элемент с '{block_title}' через селектор: {selector}")
                    parent = maybe
                    for level in range(6):
                        try:
                            bbox = parent.bounding_box()
                            if bbox and bbox['height'] > 60 and bbox['width'] > 200:
                                text_content = parent.inner_text().strip()
                                if text_content and len(text_content) > 10:
                                    el = parent
                                    print(f"✅ Найден подходящий контейнер на уровне {level}")
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
            print(f"🔄 Пробуем поиск по частичному содержимому '{block_title}'...")
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
                                            print(f"✅ Найден через поиск по содержимому")
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
            print(f"⏳ Ждем загрузку контента блока '{block_title}'...")
            for attempt in range(30):
                try:
                    txt = el.inner_text().strip()
                    if txt and "Loading" not in txt and len(txt) > 10:
                        content_loaded = True
                        print(f"✅ Контент блока '{block_title}' загружен")
                        break
                except Exception:
                    pass
                time.sleep(0.5)
            if not content_loaded:
                print(f"⚠️ {block_title} — Не дождались полной загрузки, скриню как есть")
        else:
            print(f"❌ Блок '{block_title}' не найден!")
            try:
                html_path = os.path.join(base_dir, f"failure_{block_title.lower()}_{session_id}.html")
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(page.content())
                print(f"    HTML страницы сохранен для диагностики ({block_title}): {html_path}")
            except Exception as e:
                print(f"    Не удалось сохранить HTML: {e}")
            return None
        try:
            img_path = os.path.join(base_dir, f"{session_id}_{block_title.lower()}.png")
            el.screenshot(path=img_path)
            print(f"✅ {block_title} скриншот сохранён")
            return img_path
        except Exception as e:
            print(f"❌ Ошибка создания скриншота {block_title}: {e}")
            return None

    def screenshot_userinfo_block(self, page, session_id, base_dir):
        """Скриншот блока с информацией о пользователе - улучшенная версия"""
        os.makedirs(base_dir, exist_ok=True)
        self.simulate_human_behavior(page, full_scroll=True)
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
            print("⚠️ User info не найден")
            try:
                html_path = os.path.join(base_dir, f"failure_userinfo_{session_id}.html")
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(page.content())
                print(f"    HTML страницы сохранен для диагностики (Userinfo): {html_path}")
            except Exception as e:
                print(f"    Не удалось сохранить HTML: {e}")
            return None
        try:
            img_path = os.path.join(base_dir, f"{session_id}_userinfo.png")
            userinfo_div.screenshot(path=img_path)
            print("✅ User info сохранён")
            return img_path
        except Exception:
            print("❌ Ошибка создания скриншота user info")
            return None

    def create_session_folder_structure(self, session_id, screenshots, url_data):
        """Создание структуры папки сессии - улучшенная версия"""
        session_dir = f"temp_session_{session_id}"
        os.makedirs(session_dir, exist_ok=True)
        session_screenshots = []
        for screenshot_path in screenshots:
            if screenshot_path and os.path.exists(screenshot_path):
                filename = os.path.basename(screenshot_path)
                new_path = os.path.join(session_dir, filename)
                import shutil
                shutil.copy2(screenshot_path, new_path)
                session_screenshots.append(new_path)
        # Include failure HTML files if they exist
        for block in ['userinfo', 'summary', 'sentiment', 'actions']:
            html_path = os.path.join("screens", f"failure_{block}_{session_id}.html")
            if os.path.exists(html_path):
                new_html_path = os.path.join(session_dir, f"failure_{block}.html")
                shutil.copy2(html_path, new_html_path)
                session_screenshots.append(new_html_path)
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
        """Загрузка файла в Google Drive"""
        try:
            return self.drive_client.upload_file(file_path, filename, folder_id)
        except Exception as e:
            print(f"❌ Ошибка загрузки в Google Drive: {e}")
            return None

    def create_and_upload_session_archive(self, session_dir, session_id, is_failure=False):
        """Создание и загрузка архива сессии - улучшенная версия"""
        try:
            prefix = "FAILURE" if is_failure else "session_replay"
            archive_name = f"{prefix}_{session_id}_{int(time.time())}.zip"
            archive_path = archive_name
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(session_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, session_dir)
                        zipf.write(file_path, arcname)
            print(f"📦 Создан архив: {archive_name}")
            uploaded_file = self.upload_to_google_drive(
                archive_path,
                archive_name,
                self.gdrive_folder_id
            )
            if uploaded_file:
                print(f"☁️ Архив загружен в Google Drive")
                print(f"🔗 Ссылка: {uploaded_file.get('webViewLink')}")
                import shutil
                shutil.rmtree(session_dir, ignore_errors=True)
                if os.path.exists(archive_path):
                    os.remove(archive_path)
                return uploaded_file
            else:
                print("❌ Не удалось загрузить архив")
                return None
        except Exception as e:
            print(f"❌ Ошибка создания/загрузки архива: {e}")
            return None

    def process_single_url(self, page, url_data, safety_settings):
        """Обработка одного URL - значительно улучшенная версия из локального кода"""
        url = url_data['url']
        session_id = self.get_session_id_from_url(url)
        print(f"▶️ Обрабатываем сессию: {session_id} (длительность: {url_data['duration_seconds']} сек)")
        REQUIRED_BLOCKS = ['userinfo', 'summary', 'sentiment']

        screenshot_paths = []
        summary_el = None

        try:
            print(f"🌐 Загружаем страницу...")
            # Увеличиваем таймаут и используем более мягкие условия
            page.goto(url, timeout=90000, wait_until='domcontentloaded')
            print("✅ DOM загружен")

            # Пробуем дождаться networkidle, но не критично если не получится
            try:
                page.wait_for_load_state('networkidle', timeout=15000)  # Уменьшили таймаут
                print("✅ Сетевая активность стабилизировалась")
            except Exception as e:
                print(f"⚠️ NetworkIdle не дождались за 15 сек: {e}")
                print("   Продолжаем работу...")

            # Ждем появления основных элементов интерфейса
            print("⏳ Ждем появления интерфейса...")
            try:
                # Ждем любой из основных элементов Amplitude
                page.wait_for_selector('button, [role="button"], nav, header', timeout=20000)
                print("✅ Интерфейс загружен")
            except Exception:
                print("⚠️ Основные элементы не найдены, но продолжаем...")

            # Увеличенная пауза для стабилизации
            time.sleep(10)
            print("⏳ Дополнительная пауза для стабилизации...")

            self.simulate_human_behavior(page, full_scroll=True)

            # Проверка на необходимость авторизации
            if "/login" in page.url:
                login_successful = self.login_and_update_cookies(page)
                if not login_successful:
                    return False, []
                self._update_status("    Возвращаемся к исходной ссылке...", -1)
                page.goto(url, timeout=60000, wait_until='domcontentloaded')
                time.sleep(random.uniform(2, 5))

            # Улучшенный поиск Summary вкладки
            summary_tab = None
            print("🔍 Ищем Summary вкладку...")

            # Ждем появления вкладок
            try:
                page.wait_for_selector('[role="tab"], button, .tab, [class*="tab"]', timeout=20000)
                print("✅ Вкладки найдены")
            except Exception:
                print("⚠️ Вкладки не найдены, ищем по тексту...")

            summary_selectors = [
                "text=Summary",
                "button:has-text('Summary')",
                "[role='tab']:has-text('Summary')",
                "[data-testid*='summary']",
                ".tab:has-text('Summary')",
                "[class*='tab']:has-text('Summary')",
                "div:has-text('Summary')",
                "*:has-text('Summary')"  # Самый широкий поиск
            ]

            for i, selector in enumerate(summary_selectors, 1):
                try:
                    print(f"   {i}. Пробуем селектор: {selector}")
                    elements = page.query_selector_all(selector)

                    for element in elements:
                        try:
                            text = element.inner_text().strip()
                            bbox = element.bounding_box()

                            # Проверяем, что это именно вкладка Summary
                            if (text == "Summary" or "Summary" in text) and bbox:
                                print(f"      Найден элемент: текст='{text}', размер={bbox['width']}x{bbox['height']}")

                                # Проверяем видимость и кликабельность
                                is_visible = element.is_visible()
                                is_enabled = element.is_enabled()

                                if is_visible and is_enabled:
                                    summary_tab = element
                                    print(f"✅ Summary вкладка найдена! Селектор: {selector}")
                                    break

                        except Exception as ex:
                            print(f"      Ошибка проверки элемента: {ex}")
                            continue

                    if summary_tab:
                        break

                except Exception as ex:
                    print(f"   Ошибка с селектором {selector}: {ex}")
                    continue

            if summary_tab:
                print("🖱️ Кликаем на Summary вкладку...")
                self.simulate_human_behavior(page)

                try:
                    # Прокручиваем к элементу перед кликом
                    summary_tab.scroll_into_view_if_needed()
                    time.sleep(1)

                    # Пробуем обычный клик
                    summary_tab.click()
                    print("✅ Клик выполнен")

                except Exception as e:
                    print(f"⚠️ Обычный клик не сработал: {e}")

                    try:
                        # Пробуем клик с force
                        summary_tab.click(force=True)
                        print("✅ Force клик выполнен")

                    except Exception as e2:
                        print(f"⚠️ Force клик не сработал: {e2}")

                        try:
                            # Пробуем JavaScript клик
                            summary_tab.evaluate("element => element.click()")
                            print("✅ JavaScript клик выполнен")

                        except Exception as e3:
                            print(f"❌ Все виды кликов не сработали: {e3}")
                            return False, []

                # Ждем после клика
                print("⏳ Ждем загрузку Summary контента...")
                time.sleep(random.uniform(5, 8))

                # Проверяем, что Summary действительно загрузился
                summary_loaded = False
                summary_content = None

                # Расширенный список селекторов для поиска Summary контента
                summary_content_selectors = [
                    'p.ltext-_uoww22',
                    '[data-testid="session-replay-summary"]',
                    'p:has-text("The user")',
                    'p:has-text("session")',
                    'div[class*="summary"] p',
                    'div[class*="text"] p',
                    '.ltext-_uoww22',
                    'p[class*="ltext"]',
                    'div p:has-text("user")',
                    'div p:has-text("session")'
                ]

                for attempt in range(15):  # Увеличили количество попыток
                    try:
                        # Пробуем каждый селектор
                        for selector in summary_content_selectors:
                            try:
                                element = page.query_selector(selector)
                                if element:
                                    text = element.inner_text().strip()
                                    if text and len(
                                            text) > 20 and "Loading" not in text and "summary" not in text.lower():
                                        summary_content = element
                                        summary_loaded = True
                                        print(f"✅ Summary контент найден (попытка {attempt + 1}, селектор: {selector})")
                                        print(f"   Текст: {text[:100]}...")
                                        break
                            except Exception:
                                continue

                        if summary_loaded:
                            break

                        # Если не нашли точный селектор, ищем любой текст с нужными словами
                        all_paragraphs = page.query_selector_all('p')
                        for p in all_paragraphs:
                            try:
                                text = p.inner_text().strip()
                                if (text and len(text) > 50 and
                                        any(word in text.lower() for word in
                                            ['user', 'session', 'the user', 'began']) and
                                        "Loading" not in text):
                                    summary_content = p
                                    summary_loaded = True
                                    print(f"✅ Summary контент найден через поиск по параграфам (попытка {attempt + 1})")
                                    print(f"   Текст: {text[:100]}...")
                                    break
                            except Exception:
                                continue

                        if summary_loaded:
                            break

                    except Exception:
                        pass

                    print(f"   Попытка {attempt + 1}/15 - Summary еще загружается...")
                    time.sleep(1.5)  # Уменьшили интервал, но увеличили количество попыток

                if not summary_loaded:
                    print("⚠️ Summary контент не найден, но продолжаем...")
                    # Сохраняем HTML для анализа
                    try:
                        html_path = os.path.join("screens", f"failure_summary_content_{session_id}.html")
                        with open(html_path, 'w', encoding='utf-8') as f:
                            f.write(page.content())
                        print(f"   HTML сохранен для анализа: {html_path}")
                    except Exception:
                        pass

                summary_el = summary_content if summary_loaded else None

            else:
                print("❌ Summary вкладка не найдена!")
                # Сохраняем HTML для диагностики
                try:
                    html_path = os.path.join("screens", f"failure_summary_tab_{session_id}.html")
                    with open(html_path, 'w', encoding='utf-8') as f:
                        f.write(page.content())
                    print(f"    HTML страницы сохранен для диагностики: {html_path}")
                except Exception as e:
                    print(f"    Не удалось сохранить HTML: {e}")

                return False, []

            # НАЧИНАЕМ СОЗДАНИЕ СКРИНШОТОВ
            screenshot_results = {}
            print("\n📸 Начинаем создание скриншотов...")

            print("\n1️⃣ User Info блок:")
            self.simulate_human_behavior(page, full_scroll=True)
            userinfo_path = self.screenshot_userinfo_block(page, session_id, "screens")
            screenshot_results['userinfo'] = userinfo_path is not None
            screenshot_paths = [userinfo_path] if userinfo_path else []
            time.sleep(random.uniform(1, 2))

            print("\n2️⃣ Summary блок:")
            self.simulate_human_behavior(page, full_scroll=True)
            summary_paths = self.screenshot_summary_flexible(page, session_id, "screens", summary_el=summary_el)
            screenshot_results['summary'] = len(summary_paths) > 0
            if summary_paths:
                screenshot_paths.extend(summary_paths)
            time.sleep(random.uniform(1, 2))

            print("\n3️⃣ Sentiment блок:")
            self.simulate_human_behavior(page, full_scroll=True)
            sentiment_path = self.screenshot_by_title(page, "Sentiment", session_id, "screens")
            screenshot_results['sentiment'] = sentiment_path is not None
            if sentiment_path:
                screenshot_paths.append(sentiment_path)
            time.sleep(random.uniform(1, 2))

            print("\n4️⃣ Actions блок:")
            self.simulate_human_behavior(page, full_scroll=True)
            actions_path = self.screenshot_by_title(page, "Actions", session_id, "screens")
            screenshot_results['actions'] = actions_path is not None
            if actions_path:
                screenshot_paths.append(actions_path)

            print(f"\n📊 Результаты скриншотов:")
            for block, success in screenshot_results.items():
                status = "✅" if success else "❌"
                print(f"   {status} {block.capitalize()}")

            all_required_success = all(screenshot_results.get(block, False) for block in REQUIRED_BLOCKS)
            total_blocks = len([path for path in screenshot_paths if path and os.path.exists(path)])

            print(f"\n🎯 Анализ качества:")
            print(f"   📋 Все обязательные блоки: {'✅' if all_required_success else '❌'}")
            print(f"   📸 Всего скриншотов: {total_blocks}")

            if not all_required_success:
                print("❌ Не получены все обязательные блоки. Результат не будет загружен.")
                return False, screenshot_paths
            if total_blocks < 3:
                print(f"❌ Получено меньше 3 скриншотов ({total_blocks}). Результат не будет загружен.")
                return False, screenshot_paths

            session_dir, all_files = self.create_session_folder_structure(
                session_id, screenshot_paths, url_data
            )

            uploaded_file = self.create_and_upload_session_archive(session_dir, session_id)

            if uploaded_file:
                # Очищаем временные файлы скриншотов
                for path in screenshot_paths:
                    if path and os.path.exists(path):
                        os.remove(path)
                for path in all_files:
                    if path.endswith('.html') and os.path.exists(path):
                        os.remove(path)
                return True, screenshot_paths
            else:
                print("❌ Не удалось загрузить архив")
                return False, screenshot_paths

        except Exception as e:
            print(f"❌ Ошибка при обработке URL {url}: {e}")
            import traceback
            traceback.print_exc()
            try:
                html_path = os.path.join("screens", f"failure_page_{session_id}.html")
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(page.content())
                print(f"    HTML страницы сохранен для диагностики: {html_path}")
                screenshot_paths.append(html_path)
            except Exception as save_e:
                print(f"    Не удалось сохранить HTML: {save_e}")
            return False, screenshot_paths
        finally:
            # Очистка временных файлов
            import shutil
            if 'temp_screenshots_dir' in locals() and os.path.exists(temp_screenshots_dir):
                shutil.rmtree(temp_screenshots_dir, ignore_errors=True)
            if 'session_dir' in locals() and os.path.exists(session_dir):
                shutil.rmtree(session_dir, ignore_errors=True)

    def login_and_update_cookies(self, page, max_retries=3):
        """Автоматическая авторизация с повторными попытками"""
        login = os.environ.get('AMPLITUDE_LOGIN')
        password = os.environ.get('AMPLITUDE_PASSWORD')
        if not login or password:
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

    def get_safety_settings(self):
        """Получение настроек режима безопасности - обновлено как в локальном коде"""
        safety_mode = os.environ.get('SAFETY_MODE', 'normal').lower()
        settings = {
            'slow': {'min_delay': 3, 'max_delay': 8, 'batch_size': 10, 'batch_pause_min': 60, 'batch_pause_max': 120, 'name': 'МЕДЛЕННЫЙ'},
            'normal': {'min_delay': 2, 'max_delay': 5, 'batch_size': 15, 'batch_pause_min': 45, 'batch_pause_max': 90, 'name': 'ОБЫЧНЫЙ'},  # Уменьшен размер батча
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
        """Вывод прогресса с ETA - улучшенная версия"""
        elapsed = time.time() - start_time
        percent = (current / total) * 100
        eta = "неизвестно"
        if current > 0:
            avg_time = elapsed / current
            remaining = (total - current) * avg_time
            remaining_min = remaining / 60
            eta = f"{remaining_min / 60:.1f}ч" if remaining_min > 60 else f"{remaining_min:.1f}мин"

        print(f"\n" + "=" * 20 + " ПРОГРЕСС " + "=" * 20)
        print(f"📊 Обработано: {current}/{total} ({percent:.1f}%) | ⏳ Осталось: ~{eta}")
        print(f"✅ Успешно: {successful} | ❌ Ошибок: {failed} | ❗ Зависаний (Timeout): {timeouts}")
        
        # Добавляем мониторинг системных ресурсов
        try:
            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()
            print(f"🖥️ CPU: {cpu_percent}% | 🧠 Память: {memory.percent}% использована")
        except:
            pass
        print("=" * 50)

    def process_batch(self, urls_batch, safety_settings):
        """Обработка батча URL - улучшенная версия с мультипроцессингом"""
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
            "max_duration_seconds": self.max_duration_seconds,
            "cookies_path": self.cookies_path
        }
        
        for i, url_data in enumerate(urls_batch, 1):
            print(f"\n--- [{i}/{len(urls_batch)}] Запуск процесса для URL: ...{url_data['url'][-50:]} ---")

            process = multiprocessing.Process(
                target=worker_process_url,
                args=(url_data, collector_config, safety_settings, result_queue)
            )
            process.start()
            process.join(timeout=PROCESS_TIMEOUT)

            if process.is_alive():
                print(f"❗❗❗ ПРЕВЫШЕН ТАЙМАУТ ({PROCESS_TIMEOUT} сек)! Принудительное завершение процесса...")
                process.terminate()
                process.join()
                batch_timeouts += 1
                batch_failed += 1
                self.mark_url_as_processed(url_data['url'], success=False)
                print(f"❌ URL {url_data['url']} отмечен как ошибочный из-за зависания.")
            else:
                try:
                    success = result_queue.get_nowait()
                    if success:
                        batch_successful += 1
                        print("✅ URL успешно обработан.")
                    else:
                        batch_failed += 1
                        print("❌ Произошла ошибка при обработке URL в дочернем процессе.")
                except queue.Empty:
                    batch_failed += 1
                    self.mark_url_as_processed(url_data['url'], success=False)
                    print("❌ Дочерний процесс завершился, но не вернул результат. Считаем ошибкой.")

            if i % 5 == 0 or i == len(urls_batch):
                self.print_progress(i, len(urls_batch), batch_start_time, batch_successful, batch_failed, batch_timeouts)

            if i < len(urls_batch):
                if i % safety_settings['batch_size'] == 0:
                    batch_pause = random.uniform(safety_settings['batch_pause_min'], safety_settings['batch_pause_max'])
                    print(f"\n⏸️ Пауза между батчами: {batch_pause:.1f} сек...")
                    time.sleep(batch_pause)
                else:
                    delay = random.uniform(safety_settings['min_delay'], safety_settings['max_delay'])
                    print(f"⏱️ Пауза {delay:.1f} сек...")
                    time.sleep(delay)
            time.sleep(30)  # Глобальная пауза после каждого батча
        
        # Обновляем общую статистику
        self.total_processed += len(urls_batch)
        self.total_successful += batch_successful
        self.total_failed += batch_failed
        self.total_timeouts += batch_timeouts
        batch_time = time.time() - batch_start_time
        print(f"\n📦 Батч завершен за {batch_time/60:.1f} мин. [Успешно: {batch_successful}, Ошибок: {batch_failed}, Зависаний: {batch_timeouts}]")

    def run(self):
        """Запуск обработки - улучшенная версия"""
        self.start_time = time.time()
        print("🚀 СБОРЩИК СКРИНШОТОВ SESSION REPLAY")
        print("BigQuery → Screenshots → Google Drive")
        print("=" * 50)
        
        safety_settings = self.get_safety_settings()
        print(f"🛡️ Режим безопасности: {safety_settings['name']}")
        print(f"⏱️ Таймаут на 1 URL: {PROCESS_TIMEOUT} сек")
        print(f"☁️ Google Drive папка: {self.gdrive_folder_id}")

        urls_data = self.get_unprocessed_urls()
        if not urls_data:
            print("🎉 Все URL уже обработаны!")
            return

        count_to_process = self.get_url_count(len(urls_data))
        urls_to_process = urls_data[:count_to_process]
        print(f"🎯 Будет обработано: {len(urls_to_process)} URL")

        try:
            for i in range(0, len(urls_to_process), safety_settings['batch_size']):
                batch = urls_to_process[i:i + safety_settings['batch_size']]
                self.process_batch(batch, safety_settings)
                if i + safety_settings['batch_size'] < len(urls_to_process):
                    batch_pause = random.uniform(safety_settings['batch_pause_min'], safety_settings['batch_pause_max'])
                    print(f"⏸️ Пауза между батчами: {batch_pause:.1f} сек...")
                    time.sleep(batch_pause)
        except KeyboardInterrupt:
            print("⚠️ Получен сигнал остановки.")
        except Exception as e:
            print(f"❌ Критическая ошибка: {e}")
            import traceback
            traceback.print_exc()
        
        self.print_overall_stats()

    def print_overall_stats(self):
        """Вывод общей статистики - улучшенная версия"""
        if self.start_time:
            elapsed = time.time() - self.start_time
            elapsed_hours = elapsed / 3600
            success_rate = (self.total_successful / self.total_processed * 100) if self.total_processed > 0 else 0
            
            print(f"\n" + "=" * 50)
            print(f"🎉 ОБРАБОТКА ЗАВЕРШЕНА!")
            print(f"📊 Всего обработано: {self.total_processed} URL")
            print(f"✅ Успешно: {self.total_successful}")
            print(f"❌ Ошибок (включая зависания): {self.total_failed}")
            print(f"❗ Из них зависаний (Timeout): {self.total_timeouts}")
            print(f"⏱️ Общее время: {elapsed_hours:.1f} часов")
            print(f"📊 Процент успеха: {success_rate:.1f}%")
            if self.total_processed > 0:
                avg_time_per_url = elapsed / self.total_processed
                print(f"⚡ Среднее время на URL: {avg_time_per_url:.1f} сек")
            print(f"☁️ Все успешные результаты загружены в Google Drive.")
            print(f"💾 Статусы обновлены в BigQuery.")
            print("=" * 50)

def main():
    """Основная функция - улучшенная версия"""
    # Настройка мультипроцессинга для разных платформ
    if sys.platform != 'win32':
        multiprocessing.set_start_method('spawn', force=True)
    multiprocessing.freeze_support()

    print("🔧 OAuth токены встроены в код - переменные окружения не требуются")

    try:
        collector = RenderScreenshotCollector()
        collector.run()
    except Exception as e:
        print(f"❌ Критическая ошибка в главном процессе: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()