import os
import sys
import json
import time
import hashlib
import random
import logging
from datetime import datetime
from playwright.sync_api import sync_playwright
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import zipfile

# Add project root to path for config import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import settings or use environment variables
try:
    from config.settings import settings
    print("✅ Using config.settings")
except ImportError:
    print("⚠️ config.settings not available, using environment variables")
    class MockSettings:
        GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '/etc/secrets/bigquery-credentials.json')
        BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'codellon-dwh')
        BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'amplitude_session_replay')
        BQ_TABLE_EVENTS = os.environ.get('BQ_TABLE_EVENTS', 'EVENTS_258068')
        GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID', '1K8cbFU2gYpvP3PiHwOOHS1KREqdj6fQX')
        COOKIES_PATH = os.environ.get('COOKIES_PATH', '/etc/secrets/cookies.json')
        BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '20'))
        MIN_DELAY = int(os.environ.get('MIN_DELAY', '2'))
        MAX_DELAY = int(os.environ.get('MAX_DELAY', '5'))
        BATCH_PAUSE_MIN = int(os.environ.get('BATCH_PAUSE_MIN', '30'))
        BATCH_PAUSE_MAX = int(os.environ.get('BATCH_PAUSE_MAX', '60'))
        MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
    
    settings = MockSettings()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('session_replay_processor.log')
    ]
)
logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
]

class BigQueryScreenshotCollector:
    def __init__(self, credentials_path, bq_project_id, bq_dataset_id, bq_table_id,
                 gdrive_folder_id, cookies_path):
        self.credentials_path = credentials_path
        self.bq_project_id = bq_project_id
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_id = bq_table_id
        self.gdrive_folder_id = gdrive_folder_id
        self.cookies_path = cookies_path
        self.full_table_name = f"`{bq_project_id}.{bq_dataset_id}.{bq_table_id}`"

        logger.info("🔐 Setting up connections...")
        self._init_bigquery()
        self._init_google_drive()
        self._load_cookies()

    def _init_bigquery(self):
        """Initialize BigQuery client"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project_id)
            logger.info("✅ BigQuery connected")
        except Exception as e:
            logger.error(f"❌ Error connecting to BigQuery: {e}")
            raise

    def _init_google_drive(self):
        """Initialize Google Drive client"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=['https://www.googleapis.com/auth/drive']
            )
            self.drive_service = build('drive', 'v3', credentials=credentials)
            logger.info("✅ Google Drive connected")
        except Exception as e:
            logger.error(f"❌ Error connecting to Google Drive: {e}")
            raise

    def _load_cookies(self):
        """Load cookies from file"""
        try:
            with open(self.cookies_path, "r") as f:
                self.cookies = json.load(f)
            logger.info(f"✅ Cookies loaded from {self.cookies_path}")
        except Exception as e:
            logger.error(f"❌ Error loading cookies: {e}")
            raise

    def run_automated(self):
        """Run the collector in automated mode"""
        logger.info("🚀 Starting automated Session Replay processing")
        logger.info("=" * 50)

        while True:
            try:
                # Get unprocessed URLs
                urls_data = self.get_unprocessed_urls()
                if not urls_data:
                    logger.info("🎉 All URLs have been processed!")
                    break

                # Process in batches
                batch_size = settings.BATCH_SIZE
                total_urls = len(urls_data)
                logger.info(f"📊 Found {total_urls} unprocessed URLs")

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

                    for i in range(0, total_urls, batch_size):
                        batch_urls = urls_data[i:i + batch_size]
                        logger.info(f"🔄 Processing batch {i//batch_size + 1}/{(total_urls + batch_size - 1)//batch_size}")

                        for url_data in batch_urls:
                            retries = 0
                            while retries < settings.MAX_RETRIES:
                                try:
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

                                    success, screenshots = self.process_single_url(page, url_data, {
                                        'min_delay': settings.MIN_DELAY,
                                        'max_delay': settings.MAX_DELAY,
                                        'batch_size': settings.BATCH_SIZE,
                                        'batch_pause_min': settings.BATCH_PAUSE_MIN,
                                        'batch_pause_max': settings.BATCH_PAUSE_MAX,
                                        'name': 'AUTOMATED'
                                    })

                                    self.mark_url_as_processed(url_data['url'], success)
                                    page.close()
                                    context.close()

                                    if success:
                                        logger.info(f"✅ Successfully processed URL: {url_data['url']}")
                                        break
                                    else:
                                        retries += 1
                                        if retries < settings.MAX_RETRIES:
                                            logger.warning(f"⚠️ Retry {retries}/{settings.MAX_RETRIES} for URL: {url_data['url']}")
                                            time.sleep(random.uniform(5, 10))
                                        else:
                                            logger.error(f"❌ Failed to process URL after {settings.MAX_RETRIES} retries: {url_data['url']}")

                                except Exception as e:
                                    logger.error(f"❌ Error processing URL: {str(e)}")
                                    retries += 1
                                    if retries < settings.MAX_RETRIES:
                                        time.sleep(random.uniform(5, 10))
                                    continue

                        # Pause between batches
                        if i + batch_size < total_urls:
                            pause_time = random.uniform(settings.BATCH_PAUSE_MIN, settings.BATCH_PAUSE_MAX)
                            logger.info(f"⏸️ Pausing between batches for {pause_time:.1f} seconds...")
                            time.sleep(pause_time)

                    browser.close()

            except Exception as e:
                logger.error(f"❌ Critical error in automated processing: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(300)  # Wait 5 minutes before retrying
                continue

        logger.info("🎉 Automated processing completed!")

def main():
    """Main function"""
    try:
        collector = BigQueryScreenshotCollector(
            credentials_path=settings.GOOGLE_APPLICATION_CREDENTIALS,
            bq_project_id=settings.BQ_PROJECT_ID,
            bq_dataset_id=settings.BQ_DATASET_ID,
            bq_table_id=settings.BQ_TABLE_EVENTS,
            gdrive_folder_id=settings.GDRIVE_FOLDER_ID,
            cookies_path=settings.COOKIES_PATH
        )
        collector.run_automated()
    except Exception as e:
        logger.error(f"❌ Critical error: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
