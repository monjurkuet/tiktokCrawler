import undetected_chromedriver as uc
import time
import json

IS_HEADLESS=False

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
        check_time = 5
        wait_time_each_iter = 5
        response_json = None
        while check_time > 0:
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

target_url='https://www.tiktok.com/api/explore/item_list/'

# category buttons    
categories=driver.find_elements('xpath','//div[@id="main-content-explore_page"]//button')
for category in categories[1:]:
    driver.execute_script("arguments[0].click();", category)
    category.text
    SCROLL_TIME=10
    WAIT_TIME=5
    while SCROLL_TIME!=0:
        parsed_data=parse_logs(driver, target_url)
        items=parsed_data['itemList']
        # iterate through videos
        for item in items:
            playCount=item['stats']['playCount']
            if 'contents' in item.keys():
                contents=item['contents']
                for content in contents:
                    if 'textExtra' in content.keys():
                        hashtags=content['textExtra']
                playCount
                [i['hashtagName'] for i in hashtags]
        SCROLL_TIME-=1
        #scroll to bottom of page 
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # wait for new videos to load
        time.sleep(WAIT_TIME)
