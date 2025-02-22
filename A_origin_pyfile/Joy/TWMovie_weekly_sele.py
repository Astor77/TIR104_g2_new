#Selenium 的部分若在container執行會報錯
import json
import os
import time
from datetime import datetime, timedelta, timezone
from glob import glob

import module_save_file as ms
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By


# Task 1: Load
# 讀取全國不重複csv檔案
def read_csv(file_path: str) -> pd.DataFrame:
    """
    讀取指定路徑的 CSV 檔案並轉換成 DataFrame。
    Args:
        file_path (str): 檔案路徑（含檔案名稱、副檔名）
    Returns:
        轉換後的 Pandas DataFrame
    """
    try:
        df = pd.read_csv(file_path, engine="python")
    except Exception as err:
        print(f"讀取 {file_path} 時發生錯誤: {err}")
        return None
    return df

# Task 2: Extract
# 抓取全國單片查詢每一隻電影json檔
def download_rename(MovieIds: list) -> None:


    #下載路徑
    # DOWNLOAD_DIR = "/workspaces/TIR104_g2/P_Joy/test"
    DOWNLOAD_DIR = "/workspaces/TIR104_g2/A0_raw_data/tw/test_sele"

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

# Task 3: Transform
#讀取json檔案，並新增MovieId欄位
def add_id_column(MovieIds: list) -> None:
    for MovieId in MovieIds:
        try:
            input_path = f"/workspaces/TIR104_g2/A0_raw_data/tw/test_sele/{MovieId}.json"
            output_path = f"/workspaces/TIR104_g2/A0_raw_data/tw/test_sele/weekly/{MovieId}.json"
            
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

# Task 4: Transform
#合併所有json檔案
def Concat_jsonfile(folder_path: str) -> pd.DataFrame:

    # 找到所有 JSON 檔案
    json_files = glob(os.path.join(folder_path, "*.json"))

    # 建立一個空的 list 來儲存 DataFrame
    df_list_TW = []

    # 讀取每個 JSON 檔案並轉成 DataFrame
    for file in json_files:
        try:
            df_TW = pd.read_json(file)  # 直接讀 JSON 檔
            df_list_TW.append(df_TW)
        except ValueError as e:
            print(f"讀取 {file} 時發生錯誤: {e}")
            continue

    # 合併所有 DataFrame
    merged_df_TW = pd.concat(df_list_TW, ignore_index=True)
    return merged_df_TW


if __name__ == "__main__":
    file_path = "/workspaces/TIR104_g2/A0_raw_data/tw/tw_movie_2022-2025/TWMovie2022-2025.csv"
    dfTWMovie = read_csv(file_path)
    #測試用
    MovieIds = dfTWMovie["MovieId"].loc[0:1]
    print(MovieIds)

    #下載全國單片查詢每一隻電影json檔
    # download_rename(MovieIds)
    #讀取json檔案，並新增MovieId欄位
    add_id_column(MovieIds)
    # #合併所有json檔案
    # folder_path = "/workspaces/TIR104_g2/A0_raw_data/tw/test_sele/"  # 資料夾路徑
    # merged_df = Concat_jsonfile(folder_path)
    # # 存成 CSV 或 JSON
    # ms.save_as_csv(merged_df, "TWMovie_weekly_data.csv", "/workspaces/TIR104_g2/A0_raw_data/tw/tw_movie_weekly/")
