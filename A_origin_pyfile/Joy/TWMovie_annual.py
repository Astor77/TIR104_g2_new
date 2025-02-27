import json
import time
import os
import pandas as pd
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By


#Task 1 Extract
#抓取全國2022-2025的年票房資料json檔案
#2025年資料需不斷更新

def download_annual_rename(year_list: list, date: str) -> None:


    #下載路徑
    # DOWNLOAD_DIR = "/workspaces/TIR104_g2/P_Joy/test"
    DOWNLOAD_DIR = "/workspaces/TIR104_g2_new/A0_raw_data/tw/tw_selenium_download"

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
        time.sleep(4)
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
        
        time.sleep(1)
    # 關閉driver
    driver.quit()

#task 2 Extract
# 原始json格式有誤，需先清理 JSON 檔案
def clean_json_file(file_path):
    with open(file_path, "r", encoding="utf-8-sig") as f:
        data = f.read()
    
    try:
        return json.loads(data)  # 嘗試解析 JSON
    except json.JSONDecodeError:
        # 若 JSON 解析失敗，嘗試刪除最後一筆資料
        try:
            fixed_data = data.rsplit("}", 1)[0] + "}]}"  # 移除最後一個｛並補 "}]}"
            return json.loads(fixed_data)  # 再次嘗試解析
        except json.JSONDecodeError:
            print("JSON 格式錯誤，無法修復")
            return None

#task 3 Extract
#抓取json檔案 "DataItems" 後面的資料
def extract_json(data: json) -> json:

    data_items = data.get("DataItems", [])
    return data_items

#task 4 Extract
#清洗後存成json檔
def save_json_file(data, output_path):
    with open(output_path, "w", encoding="utf-8-sig") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)  # 美化輸出


#task5 Transform
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

#task6 Transform
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
    date = "02-27"
    #下載全國2022-2025的年票房資料json檔案
    # download_annual_rename(year_list, date)

    # 使用範例
    file_path = "/workspaces/TIR104_g2_new/A0_raw_data/tw/tw_selenium_download/2022年票房資料_raw.json"
    output_path = "/workspaces/TIR104_g2_new/A0_raw_data/tw/tw_movie_year_sales/2022年票房資料.json" 
    cleaned_data = clean_json_file(file_path)
    if cleaned_data:
        save_json_file(extract_json(cleaned_data), output_path)
        print(f"清理後的 JSON 已儲存到: {output_path}")
    

    #合併2022, 2023, 2024, 2025年的資料
    # combined_df = concat_df_json(year_list)
    # print(combined_df.head())
    # #存成csv檔
    # ms.save_as_csv(combined_df, "TWMovie2022-2025_raw.csv", "/workspaces/TIR104_g2/A0_raw_data/tw/tw_movie_2022-2025/")
   
    # combined_df2 = concat_df_json_distinct(year_list)
    # ms.save_as_csv(combined_df2, "TWMovie2022-2025.csv", "/workspaces/TIR104_g2/A0_raw_data/tw/tw_movie_2022-2025/")















