import time
import pandas as pd
import numpy as np
import module_save_file as ms

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

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
    command_executor="http://localhost:14444/wd/hub",
    options=chrome_options,
)
# driver = Chrome()

file_path = "/workspaces/TIR104_g2/A1_temp_data/tw/TWMovie_df3.csv"

try:
    # 讀取csv文件
    dfTWMovie = pd.read_csv(file_path, encoding= "utf-8-sig")
    # print(dfTWMovie)
except Exception as e:
    print(f"Error reading file: {e}")


#測試用
MovieIds = dfTWMovie["tw_id"].loc[0:1]
print(MovieIds)

# task 1 Extract
# 抓取全國單片查詢上顯示的release date
def get_release_date(MovieIds: list) -> list:

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

#task 2 Transform
# 存成Dataframe
def release_date_to_df(release_date: list) -> pd.DataFrame:
    columns = ['tw_first_release_date']

    df_release_dates = pd.DataFrame(release_date, columns= columns)
    return df_release_dates


if __name__ == "__main__":
    release_dates = get_release_date(MovieIds)
    print(release_dates)
    ms.save_as_csv(release_date_to_df(release_dates), "tw_release_dates.csv", "/workspaces/TIR104_g2/A0_raw_data/tw/tw_release_dates")


# dfTWMovie_new = pd.concat([dfTWMovie, df_release_dates], axis= 1)

# print(dfTWMovie_new)