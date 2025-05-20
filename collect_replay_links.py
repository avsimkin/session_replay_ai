import datetime
import json
import time
from playwright.sync_api import sync_playwright

def get_yesterday_timestamps():
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    start = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)
    end = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)
    return int(start.timestamp()), int(end.timestamp())

def get_replay_url(start_ts, end_ts):
    date_range = f'{{"startDate":{start_ts},"endDate":{end_ts}}}'
    url = f"https://app.amplitude.com/analytics/rn/session-replay?dateRangeParams={date_range}"
    return url

def collect_replay_links(cookies_path="cookies.json", outfile="urls.txt"):
    start_ts, end_ts = get_yesterday_timestamps()
    amplitude_replays_url = get_replay_url(start_ts, end_ts)
    print(f"Открываем: {amplitude_replays_url}")

    with open(cookies_path, "r") as f:
        cookies = json.load(f)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        context.add_cookies(cookies)
        page = context.new_page()
        page.goto(amplitude_replays_url)
        time.sleep(8)  # ждать полной загрузки таблицы

        # Скроллим вниз для подгрузки строк
        for _ in range(10):
            page.keyboard.press('PageDown')
            time.sleep(0.8)

        # Ищем все строки-реплеи
        rows = page.query_selector_all('div[role="row"][row-index]')
        print(f"Нашли {len(rows)} строк для клика.")

        session_links = set()
        for idx, row in enumerate(rows):
            # Ищем и кликаем по иконке ▶️ (обычно это первый div внутри строки с aria-label="Play" или svg/play)
            try:
                play_btn = row.query_selector('svg, [aria-label*="Play"], [data-testid*="play"]')
                if not play_btn:
                    play_btn = row.query_selector('div, span, a')
                if play_btn:
                    with context.expect_page() as new_page_info:
                        play_btn.click()
                        time.sleep(2)
                    try:
                        new_page = new_page_info.value
                        url = new_page.url
                        if "sessionReplayId=" in url:
                            session_links.add(url)
                        new_page.close()
                    except Exception as e:
                        print(f"[{idx}] Не поймали страницу: {e}")
            except Exception as e:
                print(f"[{idx}] Не удалось кликнуть по строке: {e}")

        print(f"Нашли {len(session_links)} Replay-ссылок.")

        with open(outfile, "w") as f:
            for url in session_links:
                f.write(url + "\n")
        print(f"Ссылки записаны в {outfile}")
        browser.close()

if __name__ == "__main__":
    collect_replay_links()