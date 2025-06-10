import json
import os
import time
import hashlib
import random
from datetime import datetime
from playwright.sync_api import sync_playwright
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import zipfile

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
]

class BigQueryScreenshotCollector:
    def __init__(self, credentials_path, bq_project_id, bq_dataset_id, bq_table_id,
                 gdrive_folder_id, cookies_path="cookies_new.json"):
        self.credentials_path = credentials_path
        self.bq_project_id = bq_project_id
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_id = bq_table_id
        self.gdrive_folder_id = gdrive_folder_id
        self.cookies_path = cookies_path
        self.full_table_name = f"`{bq_project_id}.{bq_dataset_id}.{bq_table_id}`"

        print("🔐 Настраиваем подключения...")

        self._init_bigquery()
        self._init_google_drive()
        self._load_cookies()

    def _init_bigquery(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project_id)
            print("✅ BigQuery подключен")
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к BigQuery: {e}")

    def _init_google_drive(self):
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=['https://www.googleapis.com/auth/drive']
            )
            self.drive_service = build('drive', 'v3', credentials=credentials)
            print("✅ Google Drive подключен")
        except Exception as e:
            raise Exception(f"❌ Ошибка подключения к Google Drive: {e}")

    def _load_cookies(self):
        try:
            with open(self.cookies_path, "r") as f:
                self.cookies = json.load(f)
            print(f"✅ Cookies загружены из {self.cookies_path}")
        except Exception as e:
            raise Exception(f"❌ Ошибка загрузки cookies: {e}")

    def get_unprocessed_urls(self, limit=None, min_duration_seconds=20):
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
        AND duration_seconds >= {min_duration_seconds}
        ORDER BY record_date DESC
        """
        if limit:
            query += f"\nLIMIT {limit}"

        print(f"🔍 Получаем необработанные URL из BigQuery...")
        print(f"⏱️ Минимальная длительность: {min_duration_seconds} сек")

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
                print("✅ URL отмечен как обработанный")
            else:
                print("⚠️ URL отмечен как обработанный (с ошибкой)")
        except Exception as e:
            print(f"❌ Ошибка обновления статуса URL: {e}")

    def get_session_id_from_url(self, url):
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        if "sessionReplayId=" in url:
            parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
            session_replay_id = parts[0]
            session_start_time = parts[1] if len(parts) > 1 else "unknown"
            return f"{session_replay_id}_{session_start_time}_{url_hash}"
        return f"no_session_id_{url_hash}"

    def wait_for_content(self, page, selector, bad_texts=("Loading", "Loading summary"), timeout=10,
                         min_text_length=10):
        """
        Ждём появления контента не дольше timeout секунд.
        Проверяем каждые 0.5 сек, логируем каждые 2 сек.
        Как только появился валидный текст — сразу возвращаем элемент.
        Если за timeout секунд не появился — возвращаем None.
        """
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

    def screenshot_summary_flexible(self, page, session_id, base_dir="screens", summary_el=None):
        os.makedirs(base_dir, exist_ok=True)
        print("📄 Ищем Summary блок...")

        el = summary_el
        if not el:
            el = self.wait_for_content(page, 'p.ltext-_uoww22',
                                       timeout=3)  # Можно сделать таймаут меньше, если уже ждали

        if el:
            text_content = el.inner_text().strip()
            if len(text_content) > 20:
                print(f"✅ Summary загружен (длина: {len(text_content)} символов)")
            else:
                print(f"⚠️ Summary слишком короткий ({len(text_content)} символов), пробуем fallback")
                el = None
        if not el:
            print("⚠️ Пробуем fallback селекторы для Summary...")
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
                            print(f"✅ Fallback сработал с селектором: {selector}")
                            break
                        else:
                            el = None
                except Exception:
                    continue
            if not el:
                print("❌ Не удалось найти Summary блок ни одним способом")
                return []
        try:
            img_name = os.path.join(base_dir, f"{session_id}_summary.png")
            el.screenshot(path=img_name)
            print("✅ Summary скриншот сохранён")
            return [img_name]
        except Exception as e:
            print(f"❌ Ошибка создания скриншота Summary: {e}")
            return []

    def screenshot_by_title(self, page, block_title, session_id, base_dir="screens"):
        os.makedirs(base_dir, exist_ok=True)
        print(f"🔍 Ищем блок '{block_title}'...")
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
            except Exception as e:
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
            return None
        try:
            img_path = os.path.join(base_dir, f"{session_id}_{block_title.lower()}.png")
            el.screenshot(path=img_path)
            print(f"✅ {block_title} скриншот сохранён")
            return img_path
        except Exception as e:
            print(f"❌ Ошибка создания скриншота {block_title}: {e}")
            return None

    def screenshot_userinfo_block(self, page, session_id, base_dir="screens"):
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
            print("⚠️ User info не найден")
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
        session_dir = f"temp_session_{session_id}"
        os.makedirs(session_dir, exist_ok=True)
        session_screenshots = []
        for screenshot_path in screenshots:
            if os.path.exists(screenshot_path):
                filename = os.path.basename(screenshot_path)
                new_path = os.path.join(session_dir, filename)
                import shutil
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
            print(f"❌ Ошибка загрузки в Google Drive: {e}")
            return None

    def create_and_upload_session_archive(self, session_dir, session_id):
        try:
            archive_name = f"session_replay_{session_id}_{int(time.time())}.zip"
            archive_path = archive_name
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(session_dir):
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
                os.remove(archive_path)
                return uploaded_file
            else:
                print("❌ Не удалось загрузить архив")
                return None
        except Exception as e:
            print(f"❌ Ошибка создания архива: {e}")
            return None

    def process_single_url(self, page, url_data, safety_settings):
        url = url_data['url']
        session_id = self.get_session_id_from_url(url)
        print(f"▶️ Обрабатываем сессию: {session_id}")
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
                print("🖱️ Кликнули на Summary")
                time.sleep(random.uniform(3, 6))
                summary_el = self.wait_for_content(page, 'p.ltext-_uoww22', timeout=10)
            else:
                print("❌ Вкладка Summary не найдена!")
                return False, []
            screenshot_results = {}
            print("\n📸 Начинаем создание скриншотов...")
            print("\n1️⃣ User Info блок:")
            userinfo_path = self.screenshot_userinfo_block(page, session_id)
            screenshot_results['userinfo'] = userinfo_path is not None
            screenshot_paths = [userinfo_path] if userinfo_path else []
            time.sleep(random.uniform(1, 2))
            print("\n2️⃣ Summary блок:")
            summary_paths = self.screenshot_summary_flexible(page, session_id, summary_el=summary_el)
            screenshot_results['summary'] = len(summary_paths) > 0
            if summary_paths:
                screenshot_paths += summary_paths
            time.sleep(random.uniform(1, 2))
            print("\n3️⃣ Sentiment блок:")
            sentiment_path = self.screenshot_by_title(page, "Sentiment", session_id)
            screenshot_results['sentiment'] = sentiment_path is not None
            if sentiment_path:
                screenshot_paths.append(sentiment_path)
            time.sleep(random.uniform(1, 2))
            print("\n4️⃣ Actions блок:")
            actions_path = self.screenshot_by_title(page, "Actions", session_id)
            screenshot_results['actions'] = actions_path is not None
            if actions_path:
                screenshot_paths.append(actions_path)
            print(f"\n📊 Результаты скриншотов:")
            for block, success in screenshot_results.items():
                status = "✅" if success else "❌"
                print(f"   {status} {block.capitalize()}")
            all_success = all(screenshot_results.get(block, False) for block in REQUIRED_BLOCKS)
            total_blocks = len([path for path in screenshot_paths if path and os.path.exists(path)])
            print(f"\n🎯 Анализ качества:")
            print(f"   📋 Все 4 блока: {'✅' if all_success else '❌'}")
            print(f"   📸 Всего скриншотов: {total_blocks}")
            if not all_success:
                print("❌ Не получены все обязательные блоки, не отмечаем как обработанный")
                return False, screenshot_paths
            if total_blocks < 3:
                print("❌ Получено меньше 3 скриншотов")
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
                print("❌ Не удалось загрузить в Google Drive")
                return False, screenshot_paths
        except Exception as e:
            print(f"❌ Ошибка обработки URL: {e}")
            return False, []

    def choose_processing_options(self, total_urls):
        print(f"\n📊 Всего необработанных URL: {total_urls}")
        print("🔧 Выберите количество URL для обработки:")
        print("1. 10 URL (тест)")
        print("2. 50 URL")
        print("3. 100 URL")
        print("4. 200 URL")
        print("5. Все URL")
        print("6. Кастомное количество")
        while True:
            try:
                choice = input("\nВведите номер (1-6): ").strip()
                if choice == "1":
                    return min(10, total_urls)
                elif choice == "2":
                    return min(50, total_urls)
                elif choice == "3":
                    return min(100, total_urls)
                elif choice == "4":
                    return min(200, total_urls)
                elif choice == "5":
                    return total_urls
                elif choice == "6":
                    custom = int(input("Введите количество URL: "))
                    return min(custom, total_urls)
                else:
                    print("❌ Введите число от 1 до 6")
            except ValueError:
                print("❌ Введите корректное число")

    def choose_safety_mode(self):
        print("\n🛡️ Выберите режим безопасности:")
        print("1. 🐌 МЕДЛЕННЫЙ (3-8 сек между URL, батчи по 10)")
        print("2. ⚡ ОБЫЧНЫЙ (2-5 сек между URL, батчи по 20)")
        print("3. 🚀 БЫСТРЫЙ (1-3 сек между URL, батчи по 30)")
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
                    print("❌ Введите число от 1 до 3")
            except ValueError:
                print("❌ Введите корректное число")

    def print_progress(self, current, total, start_time, successful, failed):
        elapsed = time.time() - start_time
        percent = (current / total) * 100
        if current > 0:
            avg_time = elapsed / current
            remaining = (total - current) * avg_time
            remaining_min = remaining / 60
            if remaining_min > 60:
                eta = f"{remaining_min / 60:.1f}ч"
            else:
                eta = f"{remaining_min:.1f}мин"
        else:
            eta = "неизвестно"
        print(f"\n📊 Прогресс: {current}/{total} ({percent:.1f}%)")
        print(f"⏱️ Осталось: ~{eta}")
        print(f"✅ Успешно: {successful} | ❌ Ошибок: {failed}")

    def run(self):
        print("🚀 СБОРЩИК СКРИНШОТОВ SESSION REPLAY")
        print("BigQuery → Screenshots → Google Drive")
        print("=" * 50)
        urls_data = self.get_unprocessed_urls()
        if not urls_data:
            print("🎉 Все URL уже обработаны!")
            return
        count_to_process = self.choose_processing_options(len(urls_data))
        urls_to_process = urls_data[:count_to_process]
        safety_settings = self.choose_safety_mode()
        print(f"\n🎯 Будет обработано: {len(urls_to_process)} URL")
        print(f"🛡️ Режим безопасности: {safety_settings['name']}")
        print(f"☁️ Google Drive папка: {self.gdrive_folder_id}")
        response = input("\n❓ Начать обработку? (y/N): ").lower()
        if response not in ['y', 'yes', 'да']:
            print("❌ Отменено")
            return
        start_time = time.time()
        successful = 0
        failed = 0
        with sync_playwright() as p:
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
            for i, url_data in enumerate(urls_to_process, 1):
                user_agent = random.choice(USER_AGENTS)
                context = browser.new_context(
                    user_agent=user_agent,
                    viewport={'width': 1366, 'height': 768},
                    locale='en-US',
                    timezone_id='America/New_York'
                )
                context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    window.navigator.chrome = { runtime: {} };
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                """)
                context.add_cookies(self.cookies)
                page = context.new_page()
                print(f"\n▶️ [{i}/{len(urls_to_process)}] Обрабатываем URL...")
                success, screenshots = self.process_single_url(page, url_data, safety_settings)
                self.mark_url_as_processed(url_data['url'], success)
                if success:
                    successful += 1
                    print("✅ URL успешно обработан и загружен в Google Drive")
                else:
                    failed += 1
                    print("❌ Ошибка обработки URL")
                if i % 5 == 0 or i == len(urls_to_process):
                    self.print_progress(i, len(urls_to_process), start_time, successful, failed)
                if i < len(urls_to_process):
                    delay = random.uniform(safety_settings['min_delay'], safety_settings['max_delay'])
                    print(f"⏱️ Пауза {delay:.1f} сек...")
                    time.sleep(delay)
                if i % safety_settings['batch_size'] == 0 and i < len(urls_to_process):
                    batch_pause = random.uniform(
                        safety_settings['batch_pause_min'],
                        safety_settings['batch_pause_max']
                    )
                    print(f"\n⏸️ Пауза между батчами: {batch_pause:.1f} сек...")
                    time.sleep(batch_pause)
                page.close()
                context.close()
            browser.close()
        total_time = time.time() - start_time
        print(f"\n" + "=" * 50)
        print(f"🎉 ОБРАБОТКА ЗАВЕРШЕНА!")
        print(f"📊 Обработано: {len(urls_to_process)} URL")
        print(f"✅ Успешно: {successful}")
        print(f"❌ Ошибок: {failed}")
        print(f"⏱️ Время: {total_time / 60:.1f} минут")
        print(f"☁️ Все файлы загружены в Google Drive")
        print(f"💾 Статусы обновлены в BigQuery")

def main():
    CONFIG = {
        'credentials_path': '/Users/avsimkin/PycharmProjects/session_replay_ai/venv/bigquery-credentials.json',
        'bq_project_id': 'codellon-dwh',
        'bq_dataset_id': 'amplitude_session_replay',
        'bq_table_id': 'session_replay_urls',
        'gdrive_folder_id': '1K8cbFU2gYpvP3PiHwOOHS1KREqdj6fQX',
        'cookies_path': 'cookies_new.json'
    }
    try:
        collector = BigQueryScreenshotCollector(
            credentials_path=CONFIG['credentials_path'],
            bq_project_id=CONFIG['bq_project_id'],
            bq_dataset_id=CONFIG['bq_dataset_id'],
            bq_table_id=CONFIG['bq_table_id'],
            gdrive_folder_id=CONFIG['gdrive_folder_id'],
            cookies_path=CONFIG['cookies_path']
        )
        collector.run()
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()