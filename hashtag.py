import undetected_chromedriver as uc
import time
import json
import sqlite3
import random
import logging
from typing import List, Optional

# Configurations
DB_PATH = "database.db"  # Path to your SQLite database
IS_HEADLESS = False
TARGET_URL = 'https://www.tiktok.com/api/challenge/detail'
HASHTAG_BASE_URL = 'https://www.tiktok.com/tag/'
MAX_DRIVER_REQUESTS = 100
SLEEP_BETWEEN_REQUESTS = (0, 3)  # Min, Max random sleep
DRIVER_RESTART_DELAY = 4

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def connect_db(db_path: str):
    """Create a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
        return None

def insert_into_hashtagdata(db_path: str, hashtag: str, video_count: int):
    """Insert hashtag data into the SQLite database."""
    conn = connect_db(db_path)
    if not conn:
        return

    query = "INSERT INTO hashtagdata (hashtag, videoCount) VALUES (?, ?)"
    try:
        cursor = conn.cursor()
        cursor.execute(query, (hashtag, video_count))
        conn.commit()
        logger.info(f"Inserted: {hashtag} | {video_count}")
    except sqlite3.IntegrityError:
        logger.warning(f"Error: Hashtag '{hashtag}' already exists.")
    finally:
        conn.close()

def get_all_hashtags(db_path: str) -> List[str]:
    """Retrieve all hashtags from the database."""
    conn = connect_db(db_path)
    if not conn:
        return []

    query = "SELECT hashtag FROM explore"
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        hashtags = cursor.fetchall()
        return [tag[0] for tag in hashtags if tag[0]]
    except sqlite3.Error as e:
        logger.error(f"Error fetching hashtags: {e}")
        return []
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
    """Parse network logs and extract response for the target URL."""
    check_time = 10
    response_json = None
    while check_time > 0:
        check_time -= 1
        time.sleep(1.5)
        logs_raw = driver.get_log("performance")
        logs = [json.loads(lr["message"])["message"] for lr in logs_raw]
        
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

def main():
    hashtags = get_all_hashtags(DB_PATH)
    if not hashtags:
        logger.error("No hashtags found. Exiting.")
        return

    driver = get_new_driver()
    driver_counter = 0

    for hashtag in hashtags:
        try:
            driver_counter += 1
            if driver_counter > MAX_DRIVER_REQUESTS:
                driver_counter = 0
                driver.quit()
                time.sleep(DRIVER_RESTART_DELAY)
                driver = get_new_driver()

            driver.get(f"{HASHTAG_BASE_URL}{hashtag}")
            time.sleep(random.uniform(*SLEEP_BETWEEN_REQUESTS))

            parsed_data = parse_logs(driver, TARGET_URL)
            if parsed_data:
                video_count = parsed_data['challengeInfo']['statsV2']['videoCount']
                insert_into_hashtagdata(DB_PATH, hashtag, video_count)
            else:
                logger.warning(f"No data found for hashtag: {hashtag}")

        except Exception as e:
            logger.error(f"Error processing hashtag '{hashtag}': {e}")

    driver.quit()

if __name__ == "__main__":
    main()
