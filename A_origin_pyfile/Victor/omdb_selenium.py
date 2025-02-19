from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import json
import requests
import random
import math
import pandas as pd


def omdb_get():
    movie_detail_pd = pd.read_csv(r'C:\Users\User\Desktop\my-repo\read\tmdb_detail_raw.csv')
    #針對movie_id進行請求
    url = 'http://www.omdbapi.com/?i=tt8373206'
    headers = {
        'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    }
    
    response = requests.get(url, headers=headers)
    #需要api key才能請求成功
    if response.status_code == 200:
        print('成功取得資料')
        
    else:
        print(f'失敗，狀態碼: {response.status_code}')
        # 錯誤訊息
        print(f'錯誤資訊: {response.text}') 

def omdb_selenium_get():#進行點擊取得資料
    try:
        service = Service(executable_path=r'C:\Users\User\Desktop\my-repo\chromedriver-win64\chromedriver.exe')
        options = webdriver.ChromeOptions()
        #新增三種參數模擬真人行為
        #增加user-agent
        options.add_argument(
            'user-agent = Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
        )
        #隱藏自動化標籤檢測
        #options.add_experimental_option('excludeSwitches', ['enable-automation'])
        #options.add_experimental_option('useAutomationExtension', False)

        #時間延遲及隨機
        time.sleep(random.uniform(1.0, 3.0))

        #建立chromedriver
        driver = webdriver.Chrome(service=service, options=options)
        #目標網站
        url = 'https://www.omdbapi.com/'
        driver.get(url)
        time.sleep(3)

        #讀取csv檔案
        movie_detail_pd = pd.read_csv(r'C:\Users\User\Desktop\my-repo\read\tmdb_detail_raw.csv')
        #轉成list
        movie_id_list = movie_detail_pd['imdb_id'].tolist()
        #去除list內的nan值(型態為float)
        movie_id_list = [movie_id for movie_id in movie_id_list if not (isinstance(movie_id, float) and math.isnan(movie_id))]
        #儲存取得的imdb_id資訊
        
        movies_data = []
        #儲存無法取得資料的imdb_id
        non_search_id = []
 
        for movie_id in movie_id_list:
            try :
                #找到對話框並輸入movier_id
                input_text = driver.find_element(By.ID, 'i')
                input_text.clear()
                input_text.send_keys(movie_id)

                #輸入後進行點擊
                button_search = driver.find_element(By.ID, "search-by-id-button")
                button_search.click()
                time.sleep(10)

                #等待動態畫面載入
                movie_text = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/div/div[5]/div[3]/div/div/form/div[4]/pre'))
                )
                
                #當前的html
                html_content = driver.page_source
                #跳出pre標籤，用beautifulsoup下標籤取內容
                soup = BeautifulSoup(html_content, 'html.parser')
                movie_content_pre = driver.find_element(By.XPATH, '/html/body/div[2]/div/div[5]/div[3]/div/div/form/div[4]/pre')
                time.sleep(10)
                #將資訊存入
                movies_data.append({ 
                    'movie_id': movie_id,  
                    'movie_info' : movie_content_pre.text.strip()
                })
            
                print(f'{movie_id}資訊如下:')
                print(movie_content_pre.text)
                print()

 
            except Exception as e:
                print(f'錯誤：無法取得資訊的ID{movie_id}-{e}')
                non_search_id.append(movie_id)

            time.sleep(random.uniform(1.0, 3.0))
        
        with open('movies_data.json', 'w', encoding='utf-8') as json_file:
            json.dump(movies_data, json_file, ensure_ascii=False, indent=4)

    except Exception as e:
        print(f'無法取得內容{e}')

    finally:
        time.sleep(5)
        driver.quit()





omdb_get()







