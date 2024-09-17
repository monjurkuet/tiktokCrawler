import undetected_chromedriver as uc
import time
import json
import sqlite3
import random

IS_HEADLESS=False
db_path = "database.db"  # Path to your SQLite database

def insert_into_hashtagdata(db_path, hashtag, videoCount):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # SQL query to insert the row
    query = """
    INSERT INTO hashtagdata (hashtag, videoCount) 
    VALUES (?, ?)
    """
    # Execute the query with the provided data
    try:
        cursor.execute(query, (hashtag, videoCount))
        conn.commit()  # Commit the changes
        print(f"Inserted: {hashtag} | {videoCount}")
    except sqlite3.IntegrityError:
        print(f"Error: Hashtag '{hashtag}' already exists.")
    finally:
        conn.close()  # Close the database connection

def get_all_hashtags(db_path):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # SQL query to select all hashtags
    query = "SELECT hashtag FROM explore"
    # Execute the query and fetch all results
    cursor.execute(query)
    hashtags = cursor.fetchall()
    # Close the database connection
    conn.close()
    # Extract the hashtags from the result tuples
    hashtag_list = [tag[0] for tag in hashtags if tag[0]]
    return hashtag_list


def get_new_driver():
    """
    Instantiate a new chrome driver with headless options
    """
    options = uc.ChromeOptions()
    caps = options.to_capabilities()
    caps['goog:loggingPrefs'] = {'performance': 'ALL'}
    if IS_HEADLESS:
        options.add_argument(f'--headless')
    return uc.Chrome(options=options, desired_capabilities=caps)

def parse_logs(driver, target_url):
        # keep checking for 10 secs until api request is intercepted
        check_time = 2
        wait_time_each_iter = 5
        response_json = None
        while check_time > 0:
            check_time
            check_time -= 1
            time.sleep(wait_time_each_iter)
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
                    response_json = None
                    #print(e)
            if response_json:
                break
            else:
                print('Checking network logs.....')
        return response_json

driver=get_new_driver()
driver.get("https://www.tiktok.com/explore?lang=en-US")

target_url='https://www.tiktok.com/api/challenge/detail'
hashtag_base_url='https://www.tiktok.com/tag/'
hashtags = get_all_hashtags(db_path)

for hashtag in hashtags:
    driver.get(hashtag_base_url+hashtag)
    time.sleep(random.uniform(5,10))
    parsed_data=parse_logs(driver, target_url)
    try:
        videoCount=parsed_data['challengeInfo']['statsV2']['videoCount']
        insert_into_hashtagdata(db_path, hashtag, videoCount)
    except Exception as e:
        print(e)
