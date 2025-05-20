import json
import os
import time
import hashlib
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright

def load_cookies(filepath="cookies.json"):
    with open(filepath, "r") as f:
        return json.load(f)

def load_urls(filepath="urls.txt"):
    with open(filepath, "r") as f:
        return [line.strip() for line in f if line.strip()]

def get_session_id_from_url(url):
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    if "sessionReplayId=" in url:
        parts = url.split("sessionReplayId=")[1].split("&")[0].split("/")
        session_replay_id = parts[0]
        session_start_time = parts[1] if len(parts) > 1 else "unknown"
        return f"{session_replay_id}_{session_start_time}_{url_hash}"
    return f"no_session_id_{url_hash}"

def screenshot_by_title(page, block_title, session_id, base_dir="screens"):
    block = None
    # Ищем h4/div/span с нужным заголовком
    for tag in ['h4', 'div', 'span']:
        el = page.query_selector(f'{tag}:has-text("{block_title}")')
        if el:
            # ищем ближайший родитель-контейнер (до 3 уровней вверх)
            parent = el
            for _ in range(4):
                parent = parent.evaluate_handle('el => el.parentElement').as_element()
                if not parent:
                    break
                try:
                    bbox = parent.bounding_box()
                    if bbox and bbox['height'] > 60:
                        block = parent
                        break
                except Exception:
                    continue
            if block:
                break
    if not block:
        print(f"⚠️ Блок '{block_title}' не найден!")
        return None
    img_path = os.path.join(base_dir, f"{session_id}_{block_title.lower()}.png")
    block.screenshot(path=img_path)
    print(f"✅ Скриншот блока '{block_title}' сохранён: {img_path}")
    return img_path

def screenshot_summary_flexible(page, session_id, base_dir="screens"):
    summary_p = page.query_selector('p.ltext-_uoww22')
    if summary_p:
        print("✅ Нашли summary по селектору p.ltext-_uoww22")
    else:
        paragraphs = page.query_selector_all('p')
        for el in paragraphs:
            try:
                txt = el.inner_text()
                if "Replay Summary" in txt or "user session began" in txt:
                    summary_p = el
                    print("✅ Нашли summary по тексту в параграфе")
                    break
            except:
                continue

    if not summary_p:
        all_h4s = page.query_selector_all('h4')
        for h in all_h4s:
            try:
                if "Replay Summary" in h.inner_text():
                    container = h.evaluate_handle('el => el.parentElement').as_element()
                    if container:
                        ps = container.query_selector_all('p')
                        if ps:
                            summary_p = ps[0]
                            print("✅ Нашли summary после заголовка h4")
                            break
            except:
                continue

    if not summary_p:
        print("⚠️ Fallback: скриншот всей правой панели")
        summary_p = None
        for div in page.query_selector_all('div'):
            try:
                style = div.get_attribute('style') or ""
                if 'min-width: 460px' in style or 'max-width: 460px' in style or 'max-width: 480px' in style:
                    summary_p = div
                    break
            except:
                continue
        if not summary_p:
            print("❌ Не удалось найти даже fallback-панель!")
            return []

    # Скриншот
    os.makedirs(base_dir, exist_ok=True)
    img_name = os.path.join(base_dir, f"{session_id}_summary.png")
    summary_p.screenshot(path=img_name)
    print(f"✅ Скриншот summary сохранён: {img_name}")
    return [img_name]

def main():
    cookies = load_cookies()
    urls = load_urls()
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        context.add_cookies(cookies)
        page = context.new_page()

        for url in urls:
            print(f"▶️ Открываем: {url}")
            try:
                page.goto(url, timeout=30000)
                time.sleep(5)
                summary_tab = page.query_selector("text=Summary")
                if summary_tab:
                    summary_tab.click()
                    print("🖱️ Кликнули на Summary")
                    time.sleep(2.5)
                else:
                    print("❌ Вкладка Summary не найдена!")
                    results.append({"url": url, "session_id": "no_summary_tab", "screenshots": []})
                    continue
                session_id = get_session_id_from_url(url)

                # Скриншоты всех нужных блоков
                screenshot_paths = []
                summary_paths = screenshot_summary_flexible(page, session_id)
                if summary_paths: screenshot_paths += summary_paths

                sentiment_path = screenshot_by_title(page, "Sentiment", session_id)
                if sentiment_path: screenshot_paths.append(sentiment_path)

                actions_path = screenshot_by_title(page, "Actions", session_id)
                if actions_path: screenshot_paths.append(actions_path)

                results.append({"url": url, "session_id": session_id, "screenshots": screenshot_paths})
            except Exception as e:
                print(f"❌ Ошибка загрузки: {str(e)}")
                results.append({"url": url, "session_id": "error", "screenshots": []})

        browser.close()

    with open("summary_screens_results.json", "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("✅ Всё готово! Список скринов в summary_screens_results.json")

if __name__ == "__main__":
    main()