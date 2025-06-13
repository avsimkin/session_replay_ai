import os
import sys
import json
import time
import hashlib
import random
import logging
import traceback
from datetime import datetime
from contextlib import contextmanager
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import zipfile

# Add project root to path for config import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging to stdout for cloud environments
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)  # Use stdout for cloud environments
    ]
)
logger = logging.getLogger(__name__)

# Import settings or use environment variables
try:
    from config.settings import settings
    logger.info("✅ Using config.settings")
except ImportError:
    logger.info("⚠️ config.settings not available, using environment variables")
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
        PLAYWRIGHT_TIMEOUT = int(os.environ.get('PLAYWRIGHT_TIMEOUT', '30000'))
    
    settings = MockSettings()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
]

@contextmanager
def browser_context(playwright, user_agent, cookies):
    """Context manager for browser operations"""
    browser = None
    context = None
    try:
        browser_args = [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--disable-gpu',
            '--window-size=1366,768',
        ]
        browser = playwright.chromium.launch(
            headless=True,
            args=browser_args
        )
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
        context.add_cookies(cookies)
        yield context
    finally:
        if context:
            try:
                context.close()
            except Exception as e:
                logger.error(f"Error closing context: {e}")
        if browser:
            try:
                browser.close()
            except Exception as e:
                logger.error(f"Error closing browser: {e}")

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
        """Initialize BigQuery connection"""
        try:
            # Check if credentials file exists
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(f"Credentials file not found at {self.credentials_path}")
            
            # Initialize BigQuery client with explicit location
            self.bq_client = bigquery.Client(
                project=self.bq_project_id,
                credentials=service_account.Credentials.from_service_account_file(
                    self.credentials_path
                ),
                location='US'  # Explicitly set location to US
            )
            
            # Test connection
            self.bq_client.get_project()
            logger.info("✅ BigQuery connected")
            
            # Create processed_urls table if it doesn't exist
            self._create_processed_urls_table()
            
            # List available tables for debugging
            tables = list(self.bq_client.list_tables(f"{self.bq_project_id}.{self.bq_dataset_id}"))
            logger.info(f"Available tables in dataset: {[table.table_id for table in tables]}")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize BigQuery: {str(e)}")
            raise

    def _create_processed_urls_table(self):
        """Create processed_urls table if it doesn't exist"""
        try:
            table_id = f"{self.bq_project_id}.{self.bq_dataset_id}.processed_urls"
            
            # Check if table exists
            try:
                self.bq_client.get_table(table_id)
                logger.info("✅ processed_urls table already exists")
                return
            except Exception:
                pass
            
            # Create table
            schema = [
                bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("success", "BOOLEAN", mode="REQUIRED")
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            table = self.bq_client.create_table(table)
            logger.info(f"✅ Created table {table_id}")
            
        except Exception as e:
            logger.error(f"❌ Error creating processed_urls table: {e}")
            logger.error(traceback.format_exc())
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
            if not os.path.exists(self.cookies_path):
                raise FileNotFoundError(f"Cookies file not found: {self.cookies_path}")
                
            with open(self.cookies_path, "r") as f:
                self.cookies = json.load(f)
            logger.info(f"✅ Cookies loaded from {self.cookies_path}")
        except Exception as e:
            logger.error(f"❌ Error loading cookies: {e}")
            raise

    def get_unprocessed_urls(self):
        """Get unprocessed URLs from BigQuery"""
        try:
            # First check if table exists
            table_ref = f"{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table_id}"
            try:
                self.bq_client.get_table(table_ref)
                logger.info(f"✅ Table {table_ref} exists")
            except Exception as e:
                logger.error(f"❌ Table {table_ref} not found: {str(e)}")
                raise

            query = f"""
            SELECT DISTINCT
                url,
                session_id,
                user_id,
                event_time
            FROM `{self.bq_project_id}.{self.bq_dataset_id}.{self.bq_table_id}`
            WHERE url IS NOT NULL
            AND url NOT IN (
                SELECT url
                FROM `{self.bq_project_id}.{self.bq_dataset_id}.processed_urls`
            )
            ORDER BY event_time DESC
            LIMIT 100
            """
            
            logger.info("🔍 Executing query:")
            logger.info(query)
            
            query_job = self.bq_client.query(
                query,
                location='US'  # Explicitly set location for query
            )
            
            results = query_job.result()
            return list(results)
            
        except Exception as e:
            logger.error(f"❌ Error fetching unprocessed URLs: {str(e)}")
            raise

    def mark_url_as_processed(self, url, success):
        """Mark URL as processed in BigQuery"""
        try:
            query = f"""
            INSERT INTO `{self.bq_project_id}.{self.bq_dataset_id}.processed_urls`
            (url, processed_at, success)
            VALUES (@url, CURRENT_TIMESTAMP(), @success)
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("url", "STRING", url),
                    bigquery.ScalarQueryParameter("success", "BOOLEAN", success)
                ]
            )
            
            query_job = self.bq_client.query(query, job_config=job_config)
            query_job.result()
            logger.info(f"✅ Marked URL as processed: {url}")
            
        except Exception as e:
            logger.error(f"❌ Error marking URL as processed: {e}")
            logger.error(traceback.format_exc())
            raise

    def process_single_url(self, page, url_data, config):
        """Process a single URL and take screenshots"""
        try:
            url = url_data['url']
            logger.info(f"🌐 Opening URL: {url}")
            
            # Set timeout for page operations
            page.set_default_timeout(settings.PLAYWRIGHT_TIMEOUT)
            
            # Navigate to URL
            response = page.goto(url, wait_until='networkidle')
            if not response:
                logger.error(f"❌ Failed to load URL: {url}")
                return False, []
                
            if response.status != 200:
                logger.error(f"❌ Bad status code {response.status} for URL: {url}")
                return False, []
            
            # Wait for page to be fully loaded
            page.wait_for_load_state('networkidle')
            
            # Take screenshots
            screenshots = []
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            session_id = url_data.get('session_id', 'unknown')
            
            # Create temp directory for screenshots
            temp_dir = f"temp_session_{session_id}"
            os.makedirs(temp_dir, exist_ok=True)
            
            try:
                # Take full page screenshot
                screenshot_path = f"{temp_dir}/full_{timestamp}.png"
                page.screenshot(path=screenshot_path, full_page=True)
                screenshots.append(screenshot_path)
                
                # Take viewport screenshot
                viewport_path = f"{temp_dir}/viewport_{timestamp}.png"
                page.screenshot(path=viewport_path)
                screenshots.append(viewport_path)
                
                # Upload to Google Drive
                for screenshot in screenshots:
                    self.upload_to_drive(screenshot, session_id)
                
                return True, screenshots
                
            finally:
                # Cleanup temp files
                for screenshot in screenshots:
                    try:
                        os.remove(screenshot)
                    except Exception as e:
                        logger.error(f"Error removing temp file {screenshot}: {e}")
                try:
                    os.rmdir(temp_dir)
                except Exception as e:
                    logger.error(f"Error removing temp directory {temp_dir}: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Error processing URL {url}: {e}")
            logger.error(traceback.format_exc())
            return False, []

    def upload_to_drive(self, file_path, session_id):
        """Upload file to Google Drive"""
        try:
            file_metadata = {
                'name': os.path.basename(file_path),
                'parents': [self.gdrive_folder_id],
                'description': f'Session ID: {session_id}'
            }
            
            media = MediaFileUpload(
                file_path,
                mimetype='image/png',
                resumable=True
            )
            
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            logger.info(f"✅ Uploaded {file_path} to Drive (ID: {file.get('id')})")
            
        except Exception as e:
            logger.error(f"❌ Error uploading to Drive: {e}")
            logger.error(traceback.format_exc())
            raise

    def run_automated(self):
        """Run the collector in automated mode"""
        logger.info("🚀 Starting automated Session Replay processing")
        logger.info("=" * 50)

        try:
            # Get unprocessed URLs
            logger.info("🔍 Fetching unprocessed URLs from BigQuery...")
            urls_data = self.get_unprocessed_urls()
            if not urls_data:
                logger.info("🎉 All URLs have been processed!")
                return {"status": "success", "message": "No URLs to process"}

            # Process in batches
            batch_size = settings.BATCH_SIZE
            total_urls = len(urls_data)
            logger.info(f"📊 Found {total_urls} unprocessed URLs")
            logger.info(f"⚙️ Processing in batches of {batch_size}")

            with sync_playwright() as p:
                for i in range(0, total_urls, batch_size):
                    batch_urls = urls_data[i:i + batch_size]
                    current_batch = i//batch_size + 1
                    total_batches = (total_urls + batch_size - 1)//batch_size
                    logger.info(f"🔄 Processing batch {current_batch}/{total_batches} ({len(batch_urls)} URLs)")

                    for url_data in batch_urls:
                        retries = 0
                        while retries < settings.MAX_RETRIES:
                            try:
                                logger.info(f"🔗 Processing URL: {url_data['url']}")
                                user_agent = random.choice(USER_AGENTS)
                                
                                with browser_context(p, user_agent, self.cookies) as context:
                                    page = context.new_page()
                                    try:
                                        success, screenshots = self.process_single_url(page, url_data, {
                                            'min_delay': settings.MIN_DELAY,
                                            'max_delay': settings.MAX_DELAY,
                                            'batch_size': settings.BATCH_SIZE,
                                            'batch_pause_min': settings.BATCH_PAUSE_MIN,
                                            'batch_pause_max': settings.BATCH_PAUSE_MAX,
                                            'name': 'AUTOMATED'
                                        })
                                        self.mark_url_as_processed(url_data['url'], success)
                                        
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
                                    except PlaywrightTimeoutError as e:
                                        logger.error(f"❌ Timeout error processing URL: {e}")
                                        retries += 1
                                        if retries < settings.MAX_RETRIES:
                                            time.sleep(random.uniform(5, 10))
                                        continue
                                    finally:
                                        try:
                                            page.close()
                                        except Exception as e:
                                            logger.error(f"Error closing page: {e}")

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

            logger.info("🎉 Automated processing completed!")
            return {"status": "success", "message": "Processing completed successfully"}

        except Exception as e:
            error_msg = f"❌ Critical error in automated processing: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            return {"status": "error", "error": error_msg, "traceback": traceback.format_exc()}

def main():
    """Main function"""
    logger.info("🚀 Starting Session Replay Screenshot Collector")
    logger.info("=" * 50)
    logger.info(f"📁 Credentials path: {settings.GOOGLE_APPLICATION_CREDENTIALS}")
    logger.info(f"🏢 Project ID: {settings.BQ_PROJECT_ID}")
    logger.info(f"📊 Dataset ID: {settings.BQ_DATASET_ID}")
    logger.info(f"📋 Table ID: {settings.BQ_TABLE_EVENTS}")
    logger.info(f"📁 Cookies path: {settings.COOKIES_PATH}")
    logger.info(f"📁 Drive folder: {settings.GDRIVE_FOLDER_ID}")
    
    try:
        # Check if credentials file exists
        if not os.path.exists(settings.GOOGLE_APPLICATION_CREDENTIALS):
            error_msg = f"❌ Credentials file not found: {settings.GOOGLE_APPLICATION_CREDENTIALS}"
            logger.error(error_msg)
            return {"status": "error", "error": error_msg}
            
        # Check if cookies file exists
        if not os.path.exists(settings.COOKIES_PATH):
            error_msg = f"❌ Cookies file not found: {settings.COOKIES_PATH}"
            logger.error(error_msg)
            return {"status": "error", "error": error_msg}

        logger.info("🔐 Initializing BigQueryScreenshotCollector...")
        collector = BigQueryScreenshotCollector(
            credentials_path=settings.GOOGLE_APPLICATION_CREDENTIALS,
            bq_project_id=settings.BQ_PROJECT_ID,
            bq_dataset_id=settings.BQ_DATASET_ID,
            bq_table_id=settings.BQ_TABLE_EVENTS,
            gdrive_folder_id=settings.GDRIVE_FOLDER_ID,
            cookies_path=settings.COOKIES_PATH
        )
        
        logger.info("▶️ Starting automated processing...")
        result = collector.run_automated()
        logger.info(f"📋 Processing result: {result}")
        return result
        
    except Exception as e:
        error_msg = f"❌ Critical error: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        return {"status": "error", "error": error_msg, "traceback": traceback.format_exc()}

if __name__ == "__main__":
    result = main()
    logger.info(f"📋 Final result: {result}")
