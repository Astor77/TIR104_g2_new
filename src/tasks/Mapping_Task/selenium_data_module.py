# selenium_data_module.py ->整合用selenium獲取資料的函式
import json
import time
import os
import pandas as pd
import tasks.Storage_Task.save_file_module as sf
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import utils.path_config as p





# get_tw_movies_list 全國年度電影整理(台灣資料)
# 得到2022,2023,2024,2025 台灣電影json
#抓取全國2022-2025的年票房資料json檔案
#2025年資料需不斷更新

def download_annual_rename(year_list: list, date: str) -> None:

    #下載路徑
    DOWNLOAD_DIR = p.raw_tw_year_sales

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("disable-extensions")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("window-size=1080,720")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--allow-insecure-localhost")
    # chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option("prefs", {
        # "download.default_directory": DOWNLOAD_DIR,  # 設定下載目錄
        "download.prompt_for_download": False,       # 自動下載
        "download.directory_upgrade": True,
    })
    driver = webdriver.Remote(
        command_executor="http://host.docker.internal:14444/wd/hub",
        options=chrome_options,
    )
    # driver = Chrome()


    for year in year_list:

        url = f"https://boxofficetw.tfai.org.tw/statistic/Year/10/0/all/False/Region/{year}-" + date

        # 連結到目標網站
        driver.get(url)
        time.sleep(3)
        # By種類參看 https://selenium-python.readthedocs.io/locating-elements.html
        # 搜尋json按鈕，然後模擬點擊該按鈕
        driver.find_element(By.XPATH, "/html/body/main/section[5]/div[1]/button[3]").click()
        file_downloaded = False
        while not file_downloaded:
            time.sleep(2)  # 每2秒檢查一次
            for file_name in os.listdir(DOWNLOAD_DIR):
                if file_name.endswith(".crdownload"):  # Chrome 的暫存檔案後綴
                    file_downloaded = False
                    break
                elif file_name.startswith("票房資料"):  # 假設檔案以 '票房資料' 開頭
                    original_file_path = os.path.join(DOWNLOAD_DIR, file_name)
                    new_file_path = os.path.join(DOWNLOAD_DIR, f"{year}年票房資料_raw.json")  #修改檔名為{MovieId}.json
                    os.rename(original_file_path, new_file_path)
                    file_downloaded = True
                    break
        
        time.sleep(2)
    # 關閉driver
    driver.quit()




# 原始json格式有誤，需先清理 JSON 檔案

def clean_json_file(file_path):
    with open(file_path, "r", encoding="utf-8-sig") as f:
        data = f.read()
    
    try:
        return json.loads(data)  # 嘗試解析 JSON
    except json.JSONDecodeError:
        # 若 JSON 解析失敗，嘗試刪除最後一筆資料
        try:
            fixed_data = data.rsplit("}", 1)[0] + "}]}"  # 移除最後一個 "}" 後面的部分並補上 "}]}"
            return json.loads(fixed_data)  # 再次嘗試解析
        except json.JSONDecodeError:
            print("JSON 格式錯誤，無法修復")
            return None


#抓取json檔案 "DataItems" 後面的資料
def extract_json(data: json) -> json:

    data_items = data.get("DataItems", [])
    return data_items


# get_tw_one_movie_sale(台灣資料)
# 單片查詢票房json檔案
# 抓取全國單片查詢每一隻電影json檔

def download_rename(MovieIds: list) -> None:

    #下載路徑
    DOWNLOAD_DIR = p.raw_tw_sales

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("disable-extensions")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("window-size=1080,720")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--allow-insecure-localhost")
    # chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option("prefs", {
        # "download.default_directory": DOWNLOAD_DIR,  # 設定下載目錄
        "download.prompt_for_download": False,       # 自動下載
        "download.directory_upgrade": True,
    })
    driver = webdriver.Remote(
        command_executor="http://host.docker.internal:14444/wd/hub",
        options=chrome_options,
    )
    # driver = Chrome()

    for MovieId in MovieIds:

        url = f"https://boxofficetw.tfai.org.tw/search/{MovieId}"

        # 連結到目標網站
        driver.get(url)
        time.sleep(3)
        # By種類參看 https://selenium-python.readthedocs.io/locating-elements.html
        # 搜尋json按鈕，然後模擬點擊該按鈕
        driver.find_element(By.XPATH, "/html/body/main/section[4]/div[1]/div/button[2]").click()
        driver.find_element(By.XPATH, "/html/body/main/section[4]/div[2]/button[3]").click()
        file_downloaded = False
        while not file_downloaded:
            time.sleep(2)  # 每2秒檢查一次
            for file_name in os.listdir(DOWNLOAD_DIR):
                if file_name.endswith(".crdownload"):  # Chrome 的暫存檔案後綴
                    file_downloaded = False
                    break
                elif file_name.startswith("各週票房資料"):  # 假設檔案以 '各週票房資料' 開頭
                    original_file_path = os.path.join(DOWNLOAD_DIR, file_name)
                    new_file_path = os.path.join(DOWNLOAD_DIR, f"{MovieId}.json")  #修改檔名為{MovieId}.json
                    os.rename(original_file_path, new_file_path)
                    file_downloaded = True
                    break
        
        time.sleep(2)
    # 關閉driver
    driver.quit()


#讀取單片查詢json檔案，並新增MovieId欄位

def add_id_column(MovieIds: list) -> None:
    for MovieId in MovieIds:
        try:
            input_path = p.raw_tw_sales + f"{MovieId}.json"
            output_path = p.raw_tw_sales + f"{MovieId}.json"
            
            with open(input_path, "r", encoding="utf-8-sig") as j:
                TWMovie_in = json.load(j)
                
            TWMovie_in_v = TWMovie_in.get('Rows', [])  # 避免 'Rows' 鍵不存在時出錯
            dfTWMovie_in = pd.json_normalize(TWMovie_in_v)
            dfTWMovie_in['MovieId'] = str(MovieId)
            
            dfTWMovie_in.to_json(output_path, orient="records")
            
        except FileNotFoundError:
            print(f"檔案未找到: {input_path}")
        except json.JSONDecodeError:
            print(f"JSON 解析錯誤: {input_path}")
        except (KeyError, ValueError) as e:
            print(f"資料處理錯誤: {e}，檔案: {input_path}")
        except Exception as e:
            print(f"未知錯誤: {e}，檔案: {input_path}")

# get_tw_one_movie_release_date(台灣資料)
# 台灣上映日期
# 抓取全國單片查詢上顯示的release date
def get_release_date(MovieIds: list) -> list:

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("disable-extensions")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("window-size=1080,720")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--allow-insecure-localhost")
    # chrome_options.add_argument("--headless")

    driver = webdriver.Remote(
        command_executor="http://host.docker.internal:14444/wd/hub",
        options=chrome_options,
    )
    # driver = Chrome()

    release_date = []
    for MovieId in MovieIds:
        try:
            data = []
            url = f"https://boxofficetw.tfai.org.tw/search/{MovieId}"
            # 連結到目標網站
            driver.get(url)
            # By種類參看 https://selenium-python.readthedocs.io/locating-elements.html
            time.sleep(2)
            elements = driver.find_elements(By.CLASS_NAME, "info")

            for element in elements:
                data.append(element.text)
                time.sleep(0.2)
            release_date.append(data[1])
            
        except Exception as e:
            print(f"Error processing MovieId {MovieId}: {e}")
            continue  # 繼續處理下一個 MovieId
     
    # 關閉driver
    driver.close()
    return release_date



#合併2022, 2023, 2024, 2025年的資料
def concat_df_json(year_list: list) -> pd.DataFrame:
    all_dfs = []  # 用來儲存每個年份的 DataFrame
    #2022-2025年資料
    for year in year_list:
        json_path = Path(f"/workspaces/TIR104_g2/A0_raw_data/tw/tw_movie_year_sales/{year} 年票房資料.json")
        if json_path.exists():
            try:

            # 讀取JSON文件
                dfJSON = pd.read_json(json_path, encoding= "utf-8-sig")
                dfJSON['Year'] = f"{year}"
                all_dfs.append(dfJSON)  # 將 DataFrame 加入列表
                print(f"成功讀取: {json_path}")
                print("-----------------------------------")
            except Exception as e:
                print(f"Error reading JSON file: {e}")
        else:
            print(f"File not found: {json_path}")


    if all_dfs:
        dfJSON_raw = pd.concat(all_dfs, ignore_index=True)
        return dfJSON_raw
    else:
        print("沒有找到任何 JSON 檔案。")
        return pd.DataFrame()


#合併2022, 2023, 2024, 2025年的資料並刪除重複movieid檔案
def concat_df_json_distinct(year_list: list) -> pd.DataFrame:
    all_dfs = []  # 用來儲存每個年份的 DataFrame
    #2022-2025年資料
    for year in year_list:
        json_path = Path(f"/workspaces/TIR104_g2/A0_raw_data/tw/tw_movie_year_sales/{year} 年票房資料.json")
        if json_path.exists():
            try:

            # 讀取JSON文件
                dfJSON = pd.read_json(json_path, encoding= "utf-8-sig")
                dfJSON['Year'] = f"{year}"
                all_dfs.append(dfJSON)  # 將 DataFrame 加入列表
                print(f"成功讀取: {json_path}")
                print("-----------------------------------")
            except Exception as e:
                print(f"Error reading JSON file: {e}")
        else:
            print(f"File not found: {json_path}")


    if all_dfs:
        dfJSON_distinct = pd.concat(all_dfs).drop_duplicates(subset=['MovieId']).reset_index(drop=True)
        return dfJSON_distinct
    else:
        print("沒有找到任何 JSON 檔案。")
        return pd.DataFrame()




if __name__ == '__main__':

    # 2022-2025年資料
    year_list = [2022, 2023]
    # 2025年更新日期
    date = "02-10"
    #下載全國2022-2025的年票房資料json檔案
    download_annual_rename(year_list, date)

    # 清洗原始json檔案
    dir_path = p.raw_tw_year_sales
    file_name = "2022年票房資料_raw.json"
    file_path = dir_path / file_name

    extract_cleaned_data = extract_json(clean_json_file(file_path))