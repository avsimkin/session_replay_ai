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

# Импорты для работы с Google API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Добавляем путь к корню проекта для импорта config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from config.settings import settings
except ImportError:
    class MockSettings:
        GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '/etc/secrets/bigquery-credentials.json')
        GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID', '1K8cbFU2gYpvP3PiHwOOHS1KREqdj6fQX')
    settings = MockSettings()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
]

TEST_URLS = [
    'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D1247117195850?sessionReplayId=b04f4dad-3dea-4249-b9fe-78b689c822a5/1749812689447&sessionStartTime=1749812689447',
    'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D1247144093674?sessionReplayId=09d7d9ec-9d2f-453b-83f5-5b403e45c202/1749823352686&sessionStartTime=1749823352686',
    'https://app.amplitude.com/analytics/rn/session-replay/project/258068/search/amplitude_id%3D868026320025?sessionReplayId=03e5a484-6f63-4fb2-8964-2893e062ea27/1749825242509&sessionStartTime=1749825242509'
]

class RenderScreenshotCollector:
    def __init__(self, status_callback: Optional[Callable[[str, int], None]] = None):
        self.status_callback = status_callback
        self.credentials_path = settings.GOOGLE_APPLICATION_CREDENTIALS
        self.gdrive_folder_id = settings.GDRIVE_FOLDER_ID
        
        self._update_status("Настройка подключений...", 1)
        self.cookies = self._load_cookies_from_secret_file()
        self._init_google_drive()

    def _update_status(self, details: str, progress: int):
        if self.status_callback: 
            self.status_callback(details, progress)
        if progress != -1: 
            print(f"[{progress}%] {details}")

    def _load_cookies_from_secret_file(self):
        secret_file_path = "/etc/secrets/cookies.json"
        self._update_status(f"Загрузка cookies из {secret_file_path}...", 2)
        if not os.path.exists(secret_file_path):
            self._update_status(f"❌ Файл cookies не найден!", 2)
            return []
        try:
            with open(secret_file_path, 'r') as f: 
                cookies = json.load(f)
            self._update_status(f"✅ Cookies загружены ({len(cookies)} шт).", 2)
            return cookies
        except Exception as e:
            self._update_status(f"❌ Ошибка чтения cookies: {e}", 2)
            return []

    def _init_google_drive(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path, 
                scopes=['https://www.googleapis.com/auth/drive']
            )
            self.drive_service = build('drive', 'v3', credentials=credentials)
            self._update_status("Google Drive подключен", 4)
        except Exception as e:
            raise Exception(f"Ошибка подключения к Google Drive: {e}")

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
        Проверяем каждые 0.5 сек, логируем каждые 3 сек.
        Как только появился валидный текст — сразу возвращаем элемент.
        """
        self._update_status(f"⏳ Ждем контент (селектор: {selector}, таймаут: {timeout}с)...", -1)
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
                self._update_status(f"⚠️ Таймаут ожидания контента ({selector})", -1)
                return None
            time.sleep(0.5)

    def simulate_human_behavior(self, page):
        self._update_status("Имитация действий пользователя...", -1)
        try:
            # Случайные движения мыши
            for _ in range(random.randint(2, 4)):
                x = random.randint(200, 1200)
                y = random.randint(200, 700)
                page.mouse.move(x, y, steps=random.randint(5, 15))
                time.sleep(random.uniform(0.1, 0.3))
            
            # Случайная прокрутка
            if random.random() < 0.4:
                scroll_amount = random.randint(100, 500)
                direction = random.choice([1, -1])
                page.evaluate(f"window.scrollBy(0, {scroll_amount * direction})")
                time.sleep(random.uniform(0.5, 1.5))
                
            # Случайный клик в безопасной зоне
            if random.random() < 0.2:
                safe_x = random.randint(50, 1300)
                safe_y = random.randint(50, 150)
                page.mouse.click(safe_x, safe_y)
                time.sleep(random.uniform(0.3, 0.8))
                
            # Случайное нажатие Tab
            if random.random() < 0.3:
                page.keyboard.press('Tab')
                time.sleep(random.uniform(0.2, 0.5))
        except Exception as e:
            self._update_status(f"Небольшая ошибка при имитации: {e}", -1)

    def screenshot_summary_flexible(self, page, session_id, base_dir, summary_el=None):
        self._update_status("📄 Ищем Summary блок...", -1)
        
        # Используем переданный элемент или ищем
        el = summary_el
        if not el:
            el = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=3)

        # Проверяем качество найденного элемента
        if el:
            text_content = el.inner_text().strip()
            if len(text_content) > 20:
                self._update_status(f"✅ Summary загружен (длина: {len(text_content)} символов)", -1)
            else:
                self._update_status(f"⚠️ Summary слишком короткий ({len(text_content)} символов), пробуем fallback", -1)
                el = None

        # Fallback селекторы если основной не сработал
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
                return None

        # Создаем скриншот
        try:
            img_name = os.path.join(base_dir, f"{session_id}_summary.png")
            el.screenshot(path=img_name)
            self._update_status("✅ Summary скриншот сохранён", -1)
            return img_name
        except Exception as e:
            self._update_status(f"❌ Ошибка создания скриншота Summary: {e}", -1)
            return None

    def screenshot_by_title(self, page, block_title, session_id, base_dir):
        self._update_status(f"🔍 Ищем блок '{block_title}'...", -1)
        el = None
        
        # Основные селекторы для поиска заголовка
        search_selectors = [
            f'h4:has-text("{block_title}")',
            f'div:has-text("{block_title}")',
            f'span:has-text("{block_title}")',
            f'h3:has-text("{block_title}")',
            f'h5:has-text("{block_title}")',
            f'[title="{block_title}"]',
            f'[aria-label="{block_title}"]'
        ]
        
        # Поиск заголовка и подходящего родительского контейнера
        for selector in search_selectors:
            try:
                maybe = page.query_selector(selector)
                if maybe:
                    self._update_status(f"📍 Найден элемент с '{block_title}' через селектор: {selector}", -1)
                    
                    # Ищем подходящий родительский контейнер
                    parent = maybe
                    for level in range(6):  # Проверяем до 6 уровней вверх
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
                        
                        # Переходим к родительскому элементу
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

        # Fallback поиск по частичному содержимому
        if not el:
            self._update_status(f"🔄 Пробуем поиск по частичному содержимому '{block_title}'...", -1)
            try:
                all_elements = page.query_selector_all('div, span, h1, h2, h3, h4, h5, h6')
                for element in all_elements:
                    try:
                        text = element.inner_text().strip()
                        if block_title.lower() in text.lower() and len(text) < 100:
                            # Ищем подходящий родительский контейнер
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

        # Ждем загрузки контента если элемент найден
        if el:
            content_loaded = False
            self._update_status(f"⏳ Ждем загрузку контента блока '{block_title}'...", -1)
            
            for attempt in range(30):  # 15 секунд максимум
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

        # Создаем скриншот
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
        
        # Основной поиск по CSS классу
        try:
            css_selector = '.cerulean-cardbase.cerulean-alpha-general-card'
            elements = page.query_selector_all(css_selector)
            
            for element in elements:
                try:
                    text = element.inner_text().strip()
                    bbox = element.bounding_box()
                    
                    # Проверяем критерии для user info блока
                    if (bbox and 
                        bbox['y'] < 400 and  # Блок должен быть в верхней части
                        text and 
                        len(text) > 10 and len(text) < 500 and
                        (any(char.isdigit() for char in text) or  # Есть цифры
                         any(country in text for country in ["Spain", "Peru", "Bolivia", "Ecuador", 
                                                            "Netherlands", "Costa Rica", "Russia"]))):  # Или страны
                        userinfo_div = element
                        break
                except Exception:
                    continue
        except Exception:
            pass

        # Fallback поиск по ключевым словам
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
                        # Ищем подходящий родительский контейнер
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

        # Создаем скриншот
        try:
            img_path = os.path.join(base_dir, f"{session_id}_userinfo.png")
            userinfo_div.screenshot(path=img_path)
            self._update_status("✅ User info скриншот сохранён", -1)
            return img_path
        except Exception:
            self._update_status("❌ Ошибка создания скриншота user info", -1)
            return None

    def create_session_folder_structure(self, session_id, screenshots, url_data):
        session_dir = tempfile.mkdtemp(prefix=f"session_folder_{session_id}_")
        
        # Копируем скриншоты в папку сессии
        for screenshot_path in screenshots:
            if screenshot_path and os.path.exists(screenshot_path):
                shutil.copy2(screenshot_path, session_dir)
        
        # Создаем метаданные
        metadata = {
            "session_id": session_id,
            "url": url_data.get('session_replay_url'),
            "processed_at": datetime.now().isoformat(),
            "screenshots": [os.path.basename(p) for p in screenshots if p and os.path.exists(p)]
        }
        
        with open(os.path.join(session_dir, "metadata.json"), 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, default=str)
            
        return session_dir

    def create_and_upload_archive(self, session_dir, session_id, is_failure=False):
        archive_path = None
        try:
            prefix = "FAILURE" if is_failure else "session_replay"
            archive_name_base = f"{prefix}_{session_id}"
            archive_path = shutil.make_archive(archive_name_base, 'zip', session_dir)
            
            # Загружаем в Google Drive
            file_metadata = {
                'name': os.path.basename(archive_path),
                'parents': [self.gdrive_folder_id]
            }
            media = MediaFileUpload(archive_path, resumable=True)
            uploaded_file = self.drive_service.files().create(
                body=file_metadata, 
                media_body=media, 
                fields='id, name'
            ).execute()
            
            self._update_status(f"☁️ Архив загружен. ID: {uploaded_file.get('id')}", -1)
            return uploaded_file
        finally:
            # Очищаем временные файлы
            if archive_path and os.path.exists(archive_path):
                os.remove(archive_path)

    def process_single_url(self, page, url_data):
        url = url_data['session_replay_url']
        session_id = self.get_session_id_from_url(url)
        temp_screenshots_dir = tempfile.mkdtemp(prefix=f"screenshots_{session_id}_")
        
        # Определяем обязательные блоки
        REQUIRED_BLOCKS = ['userinfo', 'summary', 'sentiment']
        OPTIONAL_BLOCKS = ['actions']

        try:
            # Переходим на страницу
            self.simulate_human_behavior(page)
            page.goto(url, timeout=90000, wait_until="networkidle")
            time.sleep(random.uniform(3, 5))
            
            # Ищем и кликаем на вкладку Summary
            summary_tab = page.query_selector("text=Summary")
            if not summary_tab: 
                raise PlaywrightError("Вкладка Summary не найдена!")
            
            summary_tab.click()
            self._update_status("🖱️ Кликнули на Summary", -1)
            time.sleep(random.uniform(8, 12))
            self.simulate_human_behavior(page)
            
            # Ждем загрузки Summary контента
            summary_el = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=20)
            
            # Создаем скриншоты
            screenshot_results = {}
            screenshot_paths = []
            
            self._update_status("\n📸 Начинаем создание скриншотов...", -1)
            
            # 1. User Info блок
            self._update_status("\n1️⃣ User Info блок:", -1)
            userinfo_path = self.screenshot_userinfo_block(page, session_id, temp_screenshots_dir)
            screenshot_results['userinfo'] = userinfo_path is not None
            if userinfo_path:
                screenshot_paths.append(userinfo_path)
            time.sleep(random.uniform(1, 2))
            
            # 2. Summary блок
            self._update_status("\n2️⃣ Summary блок:", -1)
            summary_path = self.screenshot_summary_flexible(page, session_id, temp_screenshots_dir, summary_el)
            screenshot_results['summary'] = summary_path is not None
            if summary_path:
                screenshot_paths.append(summary_path)
            time.sleep(random.uniform(1, 2))
            
            # 3. Sentiment блок
            self._update_status("\n3️⃣ Sentiment блок:", -1)
            sentiment_path = self.screenshot_by_title(page, "Sentiment", session_id, temp_screenshots_dir)
            screenshot_results['sentiment'] = sentiment_path is not None
            if sentiment_path:
                screenshot_paths.append(sentiment_path)
            time.sleep(random.uniform(1, 2))
            
            # 4. Actions блок (опциональный)
            self._update_status("\n4️⃣ Actions блок:", -1)
            actions_path = self.screenshot_by_title(page, "Actions", session_id, temp_screenshots_dir)
            screenshot_results['actions'] = actions_path is not None
            if actions_path:
                screenshot_paths.append(actions_path)
            
            # Анализ результатов
            self._update_status(f"\n📊 Результаты скриншотов:", -1)
            for block, success in screenshot_results.items():
                status = "✅" if success else "❌"
                self._update_status(f"   {status} {block.capitalize()}", -1)
            
            # Проверяем успешность
            all_required_success = all(screenshot_results.get(block, False) for block in REQUIRED_BLOCKS)
            valid_screenshots = [p for p in screenshot_paths if p is not None and os.path.exists(p)]
            total_blocks = len(valid_screenshots)
            
            self._update_status(f"\n🎯 Анализ качества:", -1)
            self._update_status(f"   📋 Все обязательные блоки: {'✅' if all_required_success else '❌'}", -1)
            self._update_status(f"   📸 Всего скриншотов: {total_blocks}", -1)
            
            if not all_required_success:
                raise PlaywrightError(f"Не получены все обязательные блоки, сессия неудачная.")
            
            if total_blocks < 3:
                raise PlaywrightError(f"Сделано меньше 3 скриншотов ({total_blocks}), сессия неудачная.")

            # Создаем и загружаем архив
            session_archive_dir = self.create_session_folder_structure(session_id, valid_screenshots, url_data)
            if not self.create_and_upload_archive(session_archive_dir, session_id):
                raise PlaywrightError("Ошибка загрузки архива в Google Drive.")
            
            # Очищаем временные папки
            shutil.rmtree(session_archive_dir, ignore_errors=True)
            
            return True, len(valid_screenshots)

        except (PlaywrightError, PlaywrightTimeoutError) as e:
            self._update_status(f"❌ Ошибка Playwright: {e}", -1)
            
            # Сохраняем скриншот ошибки
            failure_path = os.path.join(temp_screenshots_dir, f"FAILURE_screenshot.png")
            try: 
                page.screenshot(path=failure_path, full_page=True, timeout=15000)
            except: 
                pass
            
            # Загружаем архив с ошибкой
            self.create_and_upload_archive(temp_screenshots_dir, session_id, is_failure=True)
            return False, 0
            
        finally:
            # Очищаем временную папку со скриншотами
            shutil.rmtree(temp_screenshots_dir, ignore_errors=True)

    def run(self):
        self._update_status("⚡️ RENDER SCREENSHOT COLLECTOR: Тестовый запуск с полной логикой", 5)
        
        # Используем тестовые URL
        urls_to_process = [{'session_replay_url': url} for url in TEST_URLS]
        total_urls = len(urls_to_process)
        
        self._update_status(f"🎯 Найдено {total_urls} тестовых URL для обработки.", 10)
        
        successful, failed = 0, 0
        
        with sync_playwright() as p:
            # Настройки браузера для Render
            browser_args = [
                '--no-proxy-server',
                '--disable-proxy-config-service', 
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
            
            browser = p.chromium.launch(headless=True, args=browser_args)
            
            try:
                for i, url_data in enumerate(urls_to_process, 1):
                    progress = 10 + int((i / total_urls) * 85)
                    self._update_status(f"▶️ [{i}/{total_urls}] URL: {url_data['session_replay_url'][:70]}...", progress)
                    
                    # Создаем новый контекст для каждого URL
                    user_agent = random.choice(USER_AGENTS)
                    context = browser.new_context(
                        user_agent=user_agent,
                        viewport={'width': 1600, 'height': 1200},
                        locale='en-US',
                        timezone_id='America/New_York'
                    )
                    
                    # Добавляем анти-детекцию
                    context.add_init_script("""
                        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                        window.navigator.chrome = { runtime: {} };
                        Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    """)
                    
                    # Добавляем cookies
                    if self.cookies: 
                        context.add_cookies(self.cookies)
                    
                    page = context.new_page()

                    # Обрабатываем URL
                    is_success, screenshot_count = self.process_single_url(page, url_data)
                    
                    if is_success:
                        successful += 1
                        self._update_status(f"✅ URL успешно обработан ({screenshot_count} скриншотов)", -1)
                    else:
                        failed += 1
                        self._update_status("❌ Ошибка обработки URL", -1)
                    
                    # Закрываем контекст
                    page.close()
                    context.close()
                    
                    # Пауза между URL (кроме последнего)
                    if i < total_urls:
                        delay = random.uniform(2, 5)
                        self._update_status(f"⏱️ Пауза {delay:.1f} сек...", -1)
                        time.sleep(delay)
                        
            finally:
                browser.close()
        
        # Формируем результат
        result = {
            "status": "completed", 
            "processed": total_urls, 
            "successful": successful, 
            "failed": failed,
            "success_rate": f"{(successful/total_urls*100):.1f}%" if total_urls > 0 else "0%"
        }
        
        self._update_status(f"🏁 ОБРАБОТКА ЗАВЕРШЕНА!", 100)
        self._update_status(f"📊 Результаты: {successful} успешно, {failed} ошибок", 100)
        self._update_status(f"⚡ Процент успеха: {result['success_rate']}", 100)
        self._update_status(f"☁️ Все архивы загружены в Google Drive", 100)
        
        return result


def main():
    """
    Основная функция для запуска из командной строки или как модуль
    """
    try:
        def console_status_callback(details: str, progress: int):
            """Простой callback для вывода в консоль"""
            if progress != -1:
                print(f"[{progress}%] {details}")
            else:
                print(f"[INFO] {details}")
        
        collector = RenderScreenshotCollector(status_callback=console_status_callback)
        result = collector.run()
        
        print("\n" + "="*60)
        print("🎉 ФИНАЛЬНЫЙ РЕЗУЛЬТАТ:")
        print(f"📊 Обработано: {result['processed']} URL")
        print(f"✅ Успешно: {result['successful']}")
        print(f"❌ Ошибок: {result['failed']}")
        print(f"⚡ Процент успеха: {result['success_rate']}")
        print("☁️ Все данные сохранены в Google Drive")
        print("="*60)
        
        return result
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    main()