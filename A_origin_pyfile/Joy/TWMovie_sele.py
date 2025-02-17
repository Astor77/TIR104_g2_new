#Selenium 的部分若在container執行會報錯
import json
import os
import time
import module_save_file as ms
from datetime import timezone, datetime
from datetime import timedelta
from glob import glob

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

file_path = "/workspaces/TIR104_g2/A0_raw_data/tw/TWMovie2022-2025.csv"
dfTWMovie = pd.read_csv(file_path, engine = "python")
#測試用
MovieIds = dfTWMovie["MovieId"].loc[0:1]
print(MovieIds)


def download_rename(MovieId: list) -> None:
    # 「./chromedriver」代表Chrome Driver檔案放在本Python程式同目錄內
    #下載檔案路徑
    DOWNLOAD_DIR = r"C:\Users\Shangwei Yang\Downloads\project\movie_sales"
    service = Service("./chromedriver.exe")
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", {
    "download.default_directory": DOWNLOAD_DIR,  # 設定下載目錄
    "download.prompt_for_download": False,       # 自動下載
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})


    # 建立Chrome Driver
    driver = webdriver.Chrome(service=service, options=options)
    for MovieId in MovieIds:
        url = f"https://boxofficetw.tfai.org.tw/search/{MovieId}"
        # 連結到目標網站
        driver.get(url)
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
    driver.close()

#讀取json檔案，並新增MovieId欄位
def id_time_column(MovieIds: list)-> None:
    for MovieId in MovieIds:
        with open(f"/workspaces/TIR104_g2/A0_raw_data/tw/tw_movie_sales/{MovieId}.json", "r", encoding= "utf-8-sig") as j:
            TWMovie_in = json.load(j)
            TWMovie_in_v = TWMovie_in['Rows']
            dfTWMovie_in = pd.json_normalize(TWMovie_in_v)
            dfTWMovie_in['MovieId'] = f"{MovieId}"

            dfTWMovie_in.to_json(f"/workspaces/TIR104_g2/A0_raw_data/tw/tw_movie_sales/{MovieId}.json", orient= "records")

#合併所有json檔案
def Concat_jsonfile() -> pd.DataFrame:
    # 設定 JSON 檔案所在資料夾
    folder_path = "/workspaces/TIR104_g2/A0_raw_data/tw/tw_movie_sales"  # 資料夾路徑

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
    #下載全國單片查詢每一隻電影json檔
    # download_rename(MovieIds)
    #讀取json檔案，並新增MovieId欄位
    # id_time_column(MovieIds)

    #合併所有json檔案
    merged_df = Concat_jsonfile()
    # 存成 CSV 或 JSON
    ms.save_as_csv(merged_df, "TWMovie_weekly_data.csv", "/workspaces/TIR104_g2/A0_raw_data/tw")
