import undetected_chromedriver as uc
import time
import json
import sqlite3
import random
import logging
from queue import Queue
from threading import Thread, Lock
from typing import List, Optional
import datetime

# Configurations
DB_PATH = "database.db"  # Path to your SQLite database
IS_HEADLESS = False
TARGET_URL = 'https://www.tiktok.com/api/challenge/detail'
HASHTAG_BASE_URL = 'https://www.tiktok.com/tag/'
MAX_DRIVER_REQUESTS = 100
SLEEP_BETWEEN_REQUESTS = (0, 3)  # Min, Max random sleep
DRIVER_RESTART_DELAY = 4
NUM_WORKERS = 5  # Number of worker threads

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Lock for Chrome initialization to avoid race conditions
chrome_lock = Lock()

def connect_db(db_path: str):
    """Create a connection to the SQLite database with threading support."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)  # Enable multithreading mode
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
    except sqlite3.OperationalError as e:
        logger.error(f"Database write error: {e}")
    finally:
        conn.close()

import datetime

def get_all_hashtags(db_path: str) -> List[str]:
    """Retrieve hashtags from the database that haven't been updated today."""
    conn = connect_db(db_path)
    if not conn:
        return []

    # Get today's date
    today_date = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d')

    # Query to get hashtags that haven't been updated today
    query = """
        SELECT hashtag FROM explore
        WHERE hashtag NOT IN (
            SELECT hashtag FROM hashtagdata
            WHERE DATE(updatedAt) = ?
        )
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (today_date,))
        hashtags = cursor.fetchall()
        return [tag[0] for tag in hashtags if tag[0]]
    except sqlite3.Error as e:
        logger.error(f"Error fetching hashtags: {e}")
        return []
    finally:
        conn.close()


def get_new_driver() -> uc.Chrome:
    """Instantiate a new Chrome driver with headless options, ensuring only one thread at a time can initialize."""
    with chrome_lock:  # Lock to ensure only one thread patches the Chrome driver at a time
        options = uc.ChromeOptions()
        caps = options.to_capabilities()
        caps['goog:loggingPrefs'] = {'performance': 'ALL'}
        if IS_HEADLESS:
            options.add_argument('--headless')
        driver = uc.Chrome(options=options, desired_capabilities=caps)
    return driver

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
                if "Network.responseReceived" in log["method"]:  # Ensure it's a network response
                    resp_url = log["params"]["response"]["url"]
                    if target_url in resp_url:
                        request_id = log["params"]["requestId"]
                        response_body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                        response_json = json.loads(response_body['body'])
                        return response_json
            except:
                pass
        
        logger.info('Checking network logs...')
    return response_json

def worker(hashtag_queue: Queue):
    """Worker function that continuously processes hashtags from the queue using a Chrome instance."""
    driver = get_new_driver()
    driver_counter = 0

    while not hashtag_queue.empty():
        hashtag = hashtag_queue.get()
        try:
            driver_counter += 1
            if driver_counter > MAX_DRIVER_REQUESTS:
                # Restart the driver after processing MAX_DRIVER_REQUESTS
                driver.quit()
                time.sleep(DRIVER_RESTART_DELAY)
                driver = get_new_driver()
                driver_counter = 0  # Reset the counter after restarting the driver

            driver.get(f"{HASHTAG_BASE_URL}{hashtag}")
            time.sleep(random.uniform(*SLEEP_BETWEEN_REQUESTS))

            parsed_data = parse_logs(driver, TARGET_URL)
            if parsed_data:
                video_count = parsed_data['challengeInfo']['statsV2']['videoCount']
                insert_into_hashtagdata(DB_PATH, hashtag, video_count)
            else:
                logger.warning(f"No data found for hashtag: {hashtag}")

        except:
            pass

        finally:
            hashtag_queue.task_done()

    driver.quit()

def main():
    hashtags = get_all_hashtags(DB_PATH)
    print('Total hashtags found : {}'.format(len(hashtags)))
    if not hashtags:
        logger.error("No hashtags found. Exiting.")
        return

    hashtag_queue = Queue()

    # Add all hashtags to the queue
    for hashtag in hashtags:
        hashtag_queue.put(hashtag)

    # Create and start worker threads (all processing hashtags concurrently)
    threads = []
    for i in range(NUM_WORKERS):
        thread = Thread(target=worker, args=(hashtag_queue,))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
