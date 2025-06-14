Адаптированный скрипт для Render
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

# Добавляем путь к корню проекта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Импорт настроек (как в первом скрипте)
try:
    from config.settings import settings
    print("✅ Используем настройки из config.settings")
except ImportError:
    print("⚠️ config.settings недоступен, используем переменные окружения")
    class MockSettings:
        GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '/etc/secrets/bigquery-credentials.json')
        BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'codellon-dwh')
        BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'amplitude_session_replay')
        BQ_TABLE_URLS = os.environ.get('BQ_TABLE_URLS', 'session_replay_urls')
        GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID', '1K8cbFU2gYpvP3PiHwOOHS1KREqdj6fQX')
        COOKIES = os.environ.get('COOKIES', '[]')  # JSON строка
        PROCESSING_LIMIT = int(os.environ.get('PROCESSING_LIMIT', '10'))
        MIN_DURATION_SECONDS = int(os.environ.get('MIN_DURATION_SECONDS', '20'))
    
    settings = MockSettings()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

class RenderScreenshotCollector:
    def __init__(self):
        """Инициализация для Render окружения"""
        self.credentials_path = settings.GOOGLE_APPLICATION_CREDENTIALS
        self.bq_project_id = settings.BQ_PROJECT_ID
        self.bq_dataset_id = settings.BQ_DATASET_ID
        self.bq_table_id = settings.BQ_TABLE_URLS
        self.gdrive_folder_id = settings.GDRIVE_FOLDER_ID
        self.processing_limit = settings.PROCESSING_LIMIT
        self.min_duration = settings.MIN_DURATION_SECONDS
        
        # Загружаем cookies из переменной окружения
        self.cookies = self._load_cookies_from_env()
        
        # Настройки безопасности для автоматического режима
        self.safety_settings = {
            'min_delay': 2,
            'max_delay': 4,
            'batch_size': 5,  # Меньше для Render
            'batch_pause_min': 30,
            'batch_pause_max': 60,
            'name': 'RENDER_AUTO'
        }
        
        self.full_table_name = f"`{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table_id}`"
        
        print("🔐 Настраиваем подключения для Render...")
        self._init_bigquery()
        self._init_google_drive()

    def _load_cookies_from_env(self):
        """Загрузка cookies из переменной окружения"""
        try:
            cookies_json = settings.COOKIES
            if cookies_json:
                cookies = json.loads(cookies_json)
                print(f"✅ Cookies загружены из переменной окружения ({len(cookies)} записей)")
                return cookies
            else:
                print("⚠️ COOKIES не установлена, используем пустой список")
                return []
        except Exception as e:
            print(f"❌ Ошибка загрузки cookies: {e}")
            return []

    def _init_bigquery(self):
        """Инициализация BigQuery"""
        try:
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(f"Credentials файл не найден: {self.credentials_path}")
            
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project_id)
            print("✅ BigQuery подключен")
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к BigQuery: {e}")

    def _init_google_drive(self):
        """Инициализация Google Drive"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=['https://www.googleapis.com/auth/drive']
            )
            self.drive_service = build('drive', 'v3', credentials=credentials)
            print("✅ Google Drive подключен")
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к Google Drive: {e}")

    def get_unprocessed_urls(self):
        """Получение необработанных URL с лимитом"""
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
        AND duration_seconds >= {self.min_duration}
        ORDER BY record_date DESC
        LIMIT {self.processing_limit}
        """

        print(f"🔍 Получаем необработанные URL (лимит: {self.processing_limit})...")
        
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
            print(f"📊 Найдено {len(urls_data)} необработанных URL")
            return urls_data
        except Exception as e:
            print(f"❌ Ошибка получения URL: {e}")
            raise

    def mark_url_as_processed(self, url, success=True, screenshots_count=0, drive_folder_id=None):
        """Обновление статуса URL в BigQuery"""
        try:
            update_query = f"""
            UPDATE {self.full_table_name}
            SET 
                is_processed = TRUE,
                processed_datetime = CURRENT_TIMESTAMP(),
                screenshots_count = @screenshots_count,
                drive_folder_id = @drive_folder_id
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
            status = "✅" if success else "⚠️"
            print(f"{status} URL отмечен как обработанный (скриншотов: {screenshots_count})")
        except Exception as e:
            print(f"❌ Ошибка обновления статуса URL: {e}")

    # ... (остальные методы остаются такими же, но убираем интерактивные элементы)
    
    def get_session_id_from_url(self, url):
        """Генерация ID сессии из URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        if "sessionReplayId=" in url:
            parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
            session_replay_id = parts[0]
            session_start_time = parts[1] if len(parts) > 1 else "unknown"
            return f"{session_replay_id}_{session_start_time}_{url_hash}"
        return f"no_session_id_{url_hash}"

    def wait_for_content(self, page, selector, bad_texts=("Loading", "Loading summary"), 
                        timeout=10, min_text_length=10):
        """Ожидание загрузки контента"""
        print(f"⏳ Ждем загрузку контента (таймаут {timeout} сек)...")
        start = time.time()
        last_log = 0
        
        while True:
            el = page.query_selector(selector)
            if el:
                txt = el.inner_text().strip()
                if txt and all(bad not in txt for bad in bad_texts) and len(txt) >= min_text_length:
                    print(f"✅ Контент загружен за {time.time() - start:.1f} сек")
                    return el
            
            elapsed = time.time() - start
            if elapsed - last_log >= 3:
                print(f"⏳ Ожидание... {elapsed:.1f}/{timeout} сек")
                last_log = elapsed
            
            if elapsed > timeout:
                print(f"⚠️ Контент не загрузился за {timeout} сек")
                return None
            
            time.sleep(0.5)

    def simulate_human_behavior(self, page):
        """Имитация человеческого поведения"""
        try:
            # Движения мыши
            for _ in range(random.randint(2, 4)):
                x = random.randint(200, 1200)
                y = random.randint(200, 700)
                page.mouse.move(x, y, steps=random.randint(5, 15))
                time.sleep(random.uniform(0.1, 0.3))
            
            # Скролл
            if random.random() < 0.4:
                scroll_amount = random.randint(100, 500)
                direction = random.choice([1, -1])
                page.evaluate(f"window.scrollBy(0, {scroll_amount * direction})")
                time.sleep(random.uniform(0.5, 1.5))
        except Exception:
            pass

    def create_and_upload_session_archive(self, session_dir, session_id):
        """Создание и загрузка архива в Google Drive"""
        try:
            archive_name = f"session_replay_{session_id}_{int(time.time())}.zip"
            
            # Используем временную директорию
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
                archive_path = temp_file.name
            
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(session_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, session_dir)
                        zipf.write(file_path, arcname)
            
            print(f"📦 Создан архив: {archive_name}")
            
            # Загрузка в Google Drive
            file_metadata = {
                'name': archive_name,
                'parents': [self.gdrive_folder_id]
            }
            media = MediaFileUpload(archive_path, resumable=True)
            uploaded_file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink'
            ).execute()
            
            if uploaded_file:
                print(f"☁️ Архив загружен в Google Drive")
                print(f"🔗 ID файла: {uploaded_file.get('id')}")
                
                # Очистка временных файлов
                import shutil
                shutil.rmtree(session_dir, ignore_errors=True)
                os.unlink(archive_path)
                
                return uploaded_file
            else:
                print("❌ Не удалось загрузить архив")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка создания архива: {e}")
            return None

    def process_single_url(self, page, url_data):
        """Обработка одного URL"""
        url = url_data['url']
        session_id = self.get_session_id_from_url(url)
        print(f"▶️ Обрабатываем сессию: {session_id}")
        
        try:
            # Переход на страницу
            self.simulate_human_behavior(page)
            page.goto(url, timeout=30000)
            time.sleep(random.uniform(2, 5))
            
            # Поиск и клик по Summary
            summary_tab = page.query_selector("text=Summary")
            if summary_tab:
                self.simulate_human_behavior(page)
                summary_tab.click()
                print("🖱️ Кликнули на Summary")
                time.sleep(random.uniform(3, 6))
                summary_el = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=10)
            else:
                print("❌ Вкладка Summary не найдена!")
                return False, 0
            
            # Создание временной директории для скриншотов
            session_dir = tempfile.mkdtemp(prefix=f"session_{session_id}_")
            screenshot_paths = []
            
            # Создание скриншотов (упрощенная версия)
            try:
                # User Info
                userinfo_path = self.screenshot_userinfo_block(page, session_id, session_dir)
                if userinfo_path:
                    screenshot_paths.append(userinfo_path)
                
                # Summary
                summary_paths = self.screenshot_summary_flexible(page, session_id, session_dir, summary_el)
                screenshot_paths.extend(summary_paths)
                
                # Sentiment
                sentiment_path = self.screenshot_by_title(page, "Sentiment", session_id, session_dir)
                if sentiment_path:
                    screenshot_paths.append(sentiment_path)
                
                # Actions
                actions_path = self.screenshot_by_title(page, "Actions", session_id, session_dir)
                if actions_path:
                    screenshot_paths.append(actions_path)
                
            except Exception as e:
                print(f"❌ Ошибка создания скриншотов: {e}")
                return False, 0
            
            # Проверка качества
            valid_screenshots = [p for p in screenshot_paths if p and os.path.exists(p)]
            screenshots_count = len(valid_screenshots)
            
            if screenshots_count < 2:
                print(f"❌ Недостаточно скриншотов: {screenshots_count}")
                return False, screenshots_count
            
            # Создание метаданных
            metadata = {
                "session_id": session_id,
                "url": url_data['url'],
                "amplitude_id": url_data['amplitude_id'],
                "session_replay_id": url_data['session_replay_id'],
                "duration_seconds": url_data['duration_seconds'],
                "events_count": url_data['events_count'],
                "record_date": url_data['record_date'],
                "processed_at": datetime.now().isoformat(),
                "screenshots": [os.path.basename(path) for path in valid_screenshots],
                "screenshots_count": screenshots_count
            }
            
            metadata_path = os.path.join(session_dir, "metadata.json")
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            # Загрузка в Google Drive
            uploaded_file = self.create_and_upload_session_archive(session_dir, session_id)
            
            if uploaded_file:
                return True, screenshots_count
            else:
                return False, screenshots_count
                
        except Exception as e:
            print(f"❌ Ошибка обработки URL: {e}")
            return False, 0

    # Упрощенные методы скриншотов (без интерактивности)
    def screenshot_userinfo_block(self, page, session_id, base_dir):
        """Скриншот блока с информацией о пользователе"""
        try:
            # Поиск блока с пользовательской информацией
            css_selector = '.cerulean-cardbase.cerulean-alpha-general-card'
            elements = page.query_selector_all(css_selector)
            
            for element in elements:
                try:
                    text = element.inner_text().strip()
                    bbox = element.bounding_box()
                    if (bbox and bbox['y'] < 400 and text and 
                        len(text) > 10 and len(text) < 500):
                        img_path = os.path.join(base_dir, f"{session_id}_userinfo.png")
                        element.screenshot(path=img_path)
                        print("✅ User info скриншот сохранён")
                        return img_path
                except Exception:
                    continue
            
            print("⚠️ User info блок не найден")
            return None
            
        except Exception as e:
            print(f"❌ Ошибка скриншота user info: {e}")
            return None

    def screenshot_summary_flexible(self, page, session_id, base_dir, summary_el=None):
        """Скриншот блока Summary"""
        try:
            el = summary_el
            if not el:
                el = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=3)
            
            if el:
                text_content = el.inner_text().strip()
                if len(text_content) > 20:
                    img_path = os.path.join(base_dir, f"{session_id}_summary.png")
                    el.screenshot(path=img_path)
                    print("✅ Summary скриншот сохранён")
                    return [img_path]
            
            print("❌ Summary блок не найден")
            return []
            
        except Exception as e:
            print(f"❌ Ошибка скриншота Summary: {e}")
            return []

    def screenshot_by_title(self, page, block_title, session_id, base_dir):
        """Скриншот блока по заголовку"""
        try:
            # Поиск элемента по тексту
            element = page.query_selector(f'text={block_title}')
            if element:
                # Поиск родительского контейнера
                parent = element
                for _ in range(5):
                    try:
                        parent = parent.evaluate_handle('el => el.parentElement').as_element()
                        if parent:
                            bbox = parent.bounding_box()
                            if bbox and bbox['height'] > 60 and bbox['width'] > 200:
                                img_path = os.path.join(base_dir, f"{session_id}_{block_title.lower()}.png")
                                parent.screenshot(path=img_path)
                                print(f"✅ {block_title} скриншот сохранён")
                                return img_path
                    except Exception:
                        break
            
            print(f"⚠️ {block_title} блок не найден")
            return None
            
        except Exception as e:
            print(f"❌ Ошибка скриншота {block_title}: {e}")
            return None

    def run(self):
        """Основной метод запуска (автоматический режим для Render)"""
        print("🚀 RENDER SCREENSHOT COLLECTOR")
        print("BigQuery → Screenshots → Google Drive")
        print("=" * 50)
        
        # Получаем URL для обработки
        urls_data = self.get_unprocessed_urls()
        if not urls_data:
            print("🎉 Все URL уже обработаны!")
            return {
                "status": "success",
                "processed_urls": 0,
                "message": "No URLs to process"
            }
        
        print(f"🎯 Будет обработано: {len(urls_data)} URL")
        print(f"🛡️ Режим: {self.safety_settings['name']}")
        print(f"☁️ Google Drive папка: {self.gdrive_folder_id}")
        
        start_time = time.time()
        successful = 0
        failed = 0
        total_screenshots = 0
        
        # Запуск браузера
        with sync_playwright() as p:
            # Настройки браузера для Render
            browser_args = [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor',
                '--no-proxy-server',
                '--disable-proxy-config-service'
            ]
            
            browser = p.chromium.launch(headless=True, args=browser_args)
            
            try:
                for i, url_data in enumerate(urls_data, 1):
                    print(f"\n▶️ [{i}/{len(urls_data)}] Обрабатываем URL...")
                    
                    # Создание контекста
                    user_agent = random.choice(USER_AGENTS)
                    context = browser.new_context(
                        user_agent=user_agent,
                        viewport={'width': 1366, 'height': 768},
                        locale='en-US',
                        timezone_id='America/New_York'
                    )
                    
                    # Добавление cookies
                    if self.cookies:
                        context.add_cookies(self.cookies)
                    
                    page = context.new_page()
                    
                    # Обработка URL
                    success, screenshots_count = self.process_single_url(page, url_data)
                    
                    # Обновление статуса в BigQuery
                    self.mark_url_as_processed(
                        url_data['url'], 
                        success, 
                        screenshots_count,
                        self.gdrive_folder_id if success else None
                    )
                    
                    if success:
                        successful += 1
                        total_screenshots += screenshots_count
                        print(f"✅ URL успешно обработан ({screenshots_count} скриншотов)")
                    else:
                        failed += 1
                        print("❌ Ошибка обработки URL")
                    
                    # Закрытие страницы и контекста
                    page.close()
                    context.close()
                    
                    # Пауза между URL
                    if i < len(urls_data):
                        delay = random.uniform(
                            self.safety_settings['min_delay'], 
                            self.safety_settings['max_delay']
                        )
                        print(f"⏱️ Пауза {delay:.1f} сек...")
                        time.sleep(delay)
                    
                    # Пауза между батчами
                    if i % self.safety_settings['batch_size'] == 0 and i < len(urls_data):
                        batch_pause = random.uniform(
                            self.safety_settings['batch_pause_min'],
                            self.safety_settings['batch_pause_max']
                        )
                        print(f"\n⏸️ Пауза между батчами: {batch_pause:.1f} сек...")
                        time.sleep(batch_pause)
                
            finally:
                browser.close()
        
        # Финальный отчет
        total_time = time.time() - start_time
        
        result = {
            "status": "success",
            "processed_urls": len(urls_data),
            "successful": successful,
            "failed": failed,
            "total_screenshots": total_screenshots,
            "processing_time_minutes": round(total_time / 60, 1),
            "message": f"Processed {successful}/{len(urls_data)} URLs successfully"
        }
        
        print(f"\n" + "=" * 50)
        print(f"🎉 ОБРАБОТКА ЗАВЕРШЕНА!")
        print(f"📊 Обработано: {len(urls_data)} URL")
        print(f"✅ Успешно: {successful}")
        print(f"❌ Ошибок: {failed}")
        print(f"📸 Всего скриншотов: {total_screenshots}")
        print(f"⏱️ Время: {total_time / 60:.1f} минут")
        print(f"☁️ Все файлы загружены в Google Drive")
        print(f"💾 Статусы обновлены в BigQuery")
        
        return result


def main():
    """Основная функция для Render"""
    print("🚀 ЗАПУСК СБОРЩИКА СКРИНШОТОВ ДЛЯ RENDER")
    print("=" * 50)
    
    try:
        collector = RenderScreenshotCollector()
        result = collector.run()
        
        print(f"\n📋 Итоговый результат: {result}")
        return result
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }


if __name__ == "__main__":
    result = main()
    print(f"\n📋 Финальный результат: {result}")

🔧 Необходимые изменения для Render:
1. requirements.txt (добавить):
playwright==1.40.0
google-cloud-bigquery==3.13.0
google-api-python-client==2.108.0
google-auth==2.23.4
google-oauth2-tool==0.0.3

2. Переменные окружения в Render:
# Основные настройки
BQ_PROJECT_ID=codellon-dwh
BQ_DATASET_ID=amplitude_session_replay
BQ_TABLE_URLS=session_replay_urls
GDRIVE_FOLDER_ID=1K8cbFU2gYpvP3PiHwOOHS1KREqdj6fQX

# Настройки обработки
PROCESSING_LIMIT=10
MIN_DURATION_SECONDS=20

# Cookies в JSON формате (из твоего файла cookies_new.json)
COOKIES=[{"name":"cookie_name","value":"cookie_value","domain":".amplitude.com"}]

3. Dockerfile или Build Command:
# Установка Playwright браузеров
pip install playwright
playwright install chromium

4. Интеграция в main.py:

Замени пустой 2_replay_ai_gbq.py на этот адаптированный код.

🎯 Ключевые изменения:

✅ Убрал интерактивный ввод - все настройки через переменные окружения

✅ Автоматический режим - без пользовательского выбора

✅ Headless браузер - для работы в облаке

✅ Временные файлы - используем tempfile вместо локальных папок

✅ Обработка ошибок - возвращаем результат для API

✅ Лимиты - контролируем количество обрабатываемых URL

✅ Cookies из env - загружаем из переменной окружения

Хочешь, чтобы я создал готовый файл 2_replay_ai_gbq.py с этим кодом?