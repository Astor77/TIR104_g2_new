import os
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

file_path = "/workspaces/TIR104_g2_new/A1_temp_data/tw/TWMovie_details.csv"
dfTWMovie = pd.read_csv(file_path, engine = "python")
# print(dfTWMovie)

#測試用
MovieIds = dfTWMovie["tw_id"].loc[0:1]
print(MovieIds)


def download_rename(MovieIds: list) -> None:



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


if __name__ == '__main__':
    download_rename(MovieIds)