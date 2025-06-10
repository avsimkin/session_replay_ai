import json
import os
import time
import hashlib
import random
from playwright.sync_api import sync_playwright


def load_cookies(filepath="cookies.json"):
    """Загрузка cookies с проверкой валидности"""
    if not os.path.exists(filepath):
        print("❌ Файл cookies.json не найден!")
        return []

    with open(filepath, "r") as f:
        cookies = json.load(f)

    # Проверяем, не истекли ли cookies
    current_time = time.time()
    valid_cookies = []

    for cookie in cookies:
        if 'expires' in cookie:
            if cookie['expires'] > current_time:
                valid_cookies.append(cookie)
            else:
                print(f"⚠️ Cookie {cookie.get('name', 'unknown')} истек")
        else:
            valid_cookies.append(cookie)

    print(f"✅ Загружено {len(valid_cookies)} валидных cookies")
    return valid_cookies


def save_cookies(page, filepath="cookies_new.json"):
    """Сохранение обновленных cookies"""
    try:
        cookies = page.context.cookies()
        with open(filepath, "w") as f:
            json.dump(cookies, f, indent=2)
        print(f"✅ Cookies сохранены в {filepath}")
        return True
    except Exception as e:
        print(f"❌ Ошибка сохранения cookies: {e}")
        return False


def create_stealth_browser(p):
    """Создание браузера с максимальной защитой от детекции"""
    browser_args = [
        '--no-proxy-server',
        '--disable-proxy-config-service',
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-blink-features=AutomationControlled',
        '--exclude-switches=enable-automation',
        '--disable-web-security',
        '--disable-features=VizDisplayCompositor',
        '--disable-dev-shm-usage',
        '--disable-background-timer-throttling',
        '--disable-backgrounding-occluded-windows',
        '--disable-renderer-backgrounding',
        '--disable-ipc-flooding-protection',
        '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    ]

    browser = p.chromium.launch(
        headless=False,
        args=browser_args,
        slow_mo=random.randint(50, 150)  # Замедляем действия
    )

    context = browser.new_context(
        proxy=None,
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={'width': 1920, 'height': 1080},
        locale='en-US',
        timezone_id='America/New_York',
        extra_http_headers={
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    )

    # Более продвинутые скрипты для маскировки
    context.add_init_script("""
        // Удаляем webdriver property
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
        });

        // Маскируем chrome object
        window.chrome = {
            runtime: {},
        };

        // Добавляем реалистичные свойства navigator
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
        });

        // Маскируем permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );

        // Переопределяем функции, которые могут выдать автоматизацию
        const getParameter = WebGLRenderingContext.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) {
                return 'Intel Inc.';
            }
            if (parameter === 37446) {
                return 'Intel Iris OpenGL Engine';
            }
            return getParameter(parameter);
        };
    """)

    return browser, context


def enhanced_human_behavior(page):
    """Расширенная имитация человеческого поведения"""
    try:
        # Случайные движения мыши по реалистичной траектории
        for _ in range(random.randint(1, 3)):
            x = random.randint(100, 1200)
            y = random.randint(100, 800)
            page.mouse.move(x, y, steps=random.randint(5, 15))
            time.sleep(random.uniform(0.1, 0.3))

        # Иногда делаем паузы, как будто читаем
        if random.random() < 0.4:
            time.sleep(random.uniform(1, 3))

        # Случайное прокручивание
        if random.random() < 0.3:
            scroll_amount = random.randint(-200, 500)
            page.evaluate(f"window.scrollBy(0, {scroll_amount})")
            time.sleep(random.uniform(0.5, 1.5))

        # Иногда кликаем в пустое место (как будто фокусируемся)
        if random.random() < 0.2:
            safe_x = random.randint(400, 800)
            safe_y = random.randint(200, 400)
            page.mouse.click(safe_x, safe_y)
            time.sleep(random.uniform(0.2, 0.5))

    except Exception as e:
        print(f"⚠️ Ошибка имитации поведения: {e}")


def check_page_health(page):
    """Проверка состояния страницы"""
    try:
        # Проверяем, что страница отвечает
        page.evaluate("1 + 1")

        # Проверяем URL
        current_url = page.url
        if "amplitude.com" not in current_url:
            print(f"⚠️ Неожиданный URL: {current_url}")
            return False

        # Проверяем на страницу логина
        login_selectors = [
            'input[type="email"]',
            'input[type="password"]',
            'text=Sign in',
            'text=Log in'
        ]

        for selector in login_selectors:
            if page.query_selector(selector):
                print("❌ Обнаружена страница авторизации!")
                return False

        return True

    except Exception as e:
        print(f"❌ Ошибка проверки страницы: {e}")
        return False


def handle_authorization(page):
    """Обработка страницы авторизации"""
    print("🔐 Обнаружена страница авторизации")
    print("📝 Инструкции:")
    print("1. Авторизуйтесь вручную в открытом браузере")
    print("2. Дождитесь полной загрузки Amplitude")
    print("3. Нажмите Enter в консоли для продолжения")

    input("⏸️ Нажмите Enter после авторизации...")

    # Сохраняем новые cookies
    if save_cookies(page):
        print("✅ Новые cookies сохранены")

    return check_page_health(page)


class EnhancedAntiBlockingProtection:
    """Усиленная защита от блокировок"""

    def __init__(self, settings):
        self.settings = settings
        self.consecutive_errors = 0
        self.max_consecutive_errors = 3  # Снижаем лимит
        self.total_requests = 0
        self.session_start_time = time.time()
        self.last_success_time = time.time()

    def should_recreate_browser(self):
        """Нужно ли пересоздать браузер"""
        session_time = time.time() - self.session_start_time
        time_since_success = time.time() - self.last_success_time

        return (self.consecutive_errors >= 2 or
                session_time > 1800 or  # 30 минут
                time_since_success > 300)  # 5 минут без успеха

    def should_take_long_break(self):
        """Нужна ли длинная пауза"""
        return self.consecutive_errors >= 2

    def should_stop(self):
        """Нужно ли остановиться"""
        return self.consecutive_errors >= self.max_consecutive_errors

    def on_success(self):
        """Успешная обработка"""
        self.consecutive_errors = 0
        self.total_requests += 1
        self.last_success_time = time.time()

    def on_error(self):
        """Ошибка"""
        self.consecutive_errors += 1
        self.total_requests += 1

    def get_delay(self):
        """Адаптивная задержка"""
        base_delay = random.uniform(
            self.settings['min_delay'],
            self.settings['max_delay']
        )

        # Экспоненциальное увеличение при ошибках
        if self.consecutive_errors > 0:
            multiplier = 2 ** self.consecutive_errors
            base_delay *= multiplier

        return min(base_delay, 30)  # Максимум 30 секунд

    def get_long_break_time(self):
        """Длинная пауза при проблемах"""
        return random.uniform(60, 180)  # 1-3 минуты


def safe_screenshot(element, path, description):
    """Безопасное создание скриншота с проверками"""
    try:
        if not element:
            print(f"⚠️ {description} - элемент не найден")
            return None

        # Проверяем видимость элемента
        if not element.is_visible():
            print(f"⚠️ {description} - элемент не видим")
            return None

        # Проверяем размеры
        bbox = element.bounding_box()
        if not bbox or bbox['width'] < 50 or bbox['height'] < 50:
            print(f"⚠️ {description} - элемент слишком маленький")
            return None

        # Создаем директорию если нужно
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Делаем скриншот
        element.screenshot(path=path)
        print(f"✅ {description} сохранён")
        return path

    except Exception as e:
        print(f"❌ Ошибка скриншота {description}: {e}")
        return None


def main():
    print("🚀 УЛУЧШЕННЫЙ СБОРЩИК СКРИНШОТОВ SESSION REPLAY")
    print("=" * 60)

    # Загружаем данные
    cookies = load_cookies()
    if not cookies:
        print("❌ Не удалось загрузить cookies. Создайте файл cookies.json")
        return

    # ... (остальная логика загрузки URLs и настроек остается такой же)

    all_urls = load_urls()
    processed_urls = load_processed_urls()
    unprocessed_urls = [url for url in all_urls if url not in processed_urls]

    if not unprocessed_urls:
        print("🎉 Все URL уже обработаны!")
        return

    # Настройки по умолчанию для тестирования
    count_to_process = min(10, len(unprocessed_urls))  # Начинаем с малого
    urls_to_process = unprocessed_urls[:count_to_process]

    safety_settings = {
        'min_delay': 5,  # Увеличиваем задержки
        'max_delay': 10,
        'batch_size': 5,  # Уменьшаем размер батча
        'batch_pause_min': 30,
        'batch_pause_max': 60,
        'name': 'ОСТОРОЖНЫЙ'
    }

    print(f"🎯 Тестовый режим: {len(urls_to_process)} URL")
    print(f"🛡️ Режим: {safety_settings['name']}")

    results = []
    start_time = time.time()
    protection = EnhancedAntiBlockingProtection(safety_settings)
    browser = None
    context = None
    page = None

    try:
        with sync_playwright() as p:
            for i, url in enumerate(urls_to_process, 1):
                print(f"\n▶️ [{i}/{len(urls_to_process)}] Обрабатываем URL...")

                # Пересоздаем браузер при необходимости
                if (not browser or not page or
                        protection.should_recreate_browser()):

                    if browser:
                        print("🔄 Пересоздаем браузер...")
                        browser.close()
                        time.sleep(random.uniform(5, 10))

                    browser, context = create_stealth_browser(p)
                    context.add_cookies(cookies)
                    page = context.new_page()
                    protection.session_start_time = time.time()

                # Проверяем нужно ли остановиться
                if protection.should_stop():
                    print("🛑 КРИТИЧЕСКОЕ: Слишком много ошибок!")
                    break

                # Длинная пауза при проблемах
                if protection.should_take_long_break():
                    long_break = protection.get_long_break_time()
                    print(f"⏸️ Длинная пауза: {long_break:.1f} сек...")
                    time.sleep(long_break)

                try:
                    # Имитируем человеческое поведение
                    enhanced_human_behavior(page)

                    # Переходим на страницу
                    response = page.goto(url, timeout=45000, wait_until="domcontentloaded")

                    if not response or response.status >= 400:
                        print(f"❌ Плохой ответ сервера: {response.status if response else 'None'}")
                        protection.on_error()
                        continue

                    # Дополнительное ожидание загрузки
                    time.sleep(random.uniform(3, 6))

                    # Проверяем состояние страницы
                    if not check_page_health(page):
                        if "amplitude.com/login" in page.url or page.query_selector('input[type="email"]'):
                            if not handle_authorization(page):
                                print("❌ Не удалось пройти авторизацию")
                                break
                        else:
                            print("❌ Страница в плохом состоянии")
                            protection.on_error()
                            continue

                    # Остальная логика обработки...
                    # (работа с Summary, скриншоты и т.д.)

                    session_id = get_session_id_from_url(url)
                    print(f"📋 Session ID: {session_id}")

                    protection.on_success()
                    save_processed_url(url)

                except Exception as e:
                    error_msg = str(e)
                    print(f"❌ Ошибка: {error_msg[:100]}...")
                    protection.on_error()
                    save_processed_url(url)  # Чтобы не зависнуть на проблемном URL

                # Адаптивная задержка
                if i < len(urls_to_process):
                    delay = protection.get_delay()
                    print(f"⏱️ Пауза {delay:.1f} сек...")
                    time.sleep(delay)

    finally:
        if browser:
            browser.close()

    print("\n" + "=" * 60)
    print("🎉 ТЕСТИРОВАНИЕ ЗАВЕРШЕНО!")
    print("💡 Если тест прошел успешно, можете увеличить количество URL")


# Остальные функции остаются без изменений...
def load_urls(filepath="urls.txt"):
    """Загрузка URLs с фильтрацией комментариев и пустых строк"""
    with open(filepath, "r") as f:
        urls = []
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and line.startswith('http'):
                urls.append(line)
        return urls


def load_processed_urls(filepath="processed_urls.txt"):
    """Загрузка списка уже обработанных URL"""
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def save_processed_url(url, filepath="processed_urls.txt"):
    """Сохранение обработанного URL"""
    with open(filepath, "a") as f:
        f.write(url + "\n")


def get_session_id_from_url(url):
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    if "sessionReplayId=" in url:
        parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
        session_replay_id = parts[0]
        session_start_time = parts[1] if len(parts) > 1 else "unknown"
        return f"{session_replay_id}_{session_start_time}_{url_hash}"
    return f"no_session_id_{url_hash}"


if __name__ == "__main__":
    main()