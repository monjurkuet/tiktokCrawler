import undetected_chromedriver as uc
import time
import json
import sqlite3
import logging
from typing import Optional

# Configuration
IS_HEADLESS = False
DB_PATH = "database.db"
TARGET_URL = 'https://www.tiktok.com/api/explore/item_list/'
SCROLL_TIME = 10
WAIT_TIME = 5
CATEGORY_LOAD_WAIT_TIME = 5

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def connect_db(db_path: str) -> Optional[sqlite3.Connection]:
    """Establish connection to SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        return None

def insert_into_explore(db_path: str, hashtag: str, category: str, play_count: int):
    """Insert hashtag data into the 'explore' table of SQLite."""
    conn = connect_db(db_path)
    if not conn:
        return

    query = """
    INSERT INTO explore (category, playCount, hashtag) 
    VALUES (?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (category, play_count, hashtag))
        conn.commit()
        logger.info(f"Inserted: {hashtag} | {category} | {play_count}")
    except sqlite3.IntegrityError:
        logger.warning(f"Error: Hashtag '{hashtag}' already exists.")
    finally:
        conn.close()

def get_new_driver() -> uc.Chrome:
    """Instantiate a new Chrome driver with headless options."""
    options = uc.ChromeOptions()
    caps = options.to_capabilities()
    caps['goog:loggingPrefs'] = {'performance': 'ALL'}
    if IS_HEADLESS:
        options.add_argument('--headless')
    return uc.Chrome(options=options, desired_capabilities=caps)

def parse_logs(driver: uc.Chrome, target_url: str) -> Optional[dict]:
    """Capture and parse network logs from Chrome DevTools."""
    check_time = 2
    response_json = None

    while check_time > 0:
        check_time -= 1
        time.sleep(WAIT_TIME)
        logs_raw = driver.get_log("performance")
        logs = [json.loads(log["message"])["message"] for log in logs_raw]
        
        for log in logs:
            try:
                resp_url = log["params"]["response"]["url"]
                if target_url in resp_url:
                    request_id = log["params"]["requestId"]
                    response_body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                    response_json = json.loads(response_body['body'])
                    return response_json
            except Exception as e:
                logger.warning(f"Error parsing logs: {e}")
        
        logger.info('Checking network logs...')
    return response_json

def process_items(parsed_data: dict, category_text: str, db_path: str):
    """Extract playCount and hashtags, and insert into the database."""
    if 'itemList' not in parsed_data:
        return

    for item in parsed_data['itemList']:
        play_count = item['stats']['playCount']
        if 'contents' in item:
            contents = item['contents']
            for content in contents:
                if 'textExtra' in content:
                    hashtags = [i['hashtagName'] for i in content['textExtra']]
                    for hashtag in hashtags:
                        insert_into_explore(db_path, hashtag, category_text, play_count)

def scroll_and_extract_data(driver: uc.Chrome, target_url: str, category_text: str, db_path: str):
    """Scroll the page, capture network logs, and extract data."""
    scroll_time = SCROLL_TIME

    while scroll_time > 0:
        parsed_data = parse_logs(driver, target_url)
        if parsed_data:
            process_items(parsed_data, category_text, db_path)
        else:
            scroll_time -= 5

        scroll_time -= 1
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(WAIT_TIME)

def scrape_tiktok_explore_page(db_path: str):
    """Main function to scrape TikTok explore page and extract data."""
    driver = get_new_driver()
    
    try:
        driver.get("https://www.tiktok.com/explore?lang=en-US")
        time.sleep(10)  # Wait for explore page to load

        categories = driver.find_elements('xpath', '//div[@id="main-content-explore_page"]//button')

        for category in categories:
            try:
                driver.execute_script("arguments[0].click();", category)
                category_text = category.text
                time.sleep(CATEGORY_LOAD_WAIT_TIME)

                scroll_and_extract_data(driver, TARGET_URL, category_text, db_path)
            except:
                pass
    
    except Exception as e:
        logger.error(f"Error scraping TikTok explore page: {e}")
    #finally:
        #driver.quit()

if __name__ == "__main__":
    scrape_tiktok_explore_page(DB_PATH)
