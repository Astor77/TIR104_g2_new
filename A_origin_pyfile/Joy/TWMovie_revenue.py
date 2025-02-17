import time
import pandas as pd
import numpy as np

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
chrome_options.add_argument("--headless")

driver = webdriver.Remote(
    command_executor="http://localhost:14444/wd/hub",
    options=chrome_options,
)
# driver = Chrome()


def get_movie_revenue(MovieTitles: list) -> list:

    movie_revenue = []
    for MovieTitle in MovieTitles:
        try:
            data = []
            url = f"https://www.the-numbers.com/custom-search?searchterm={MovieTitle}&searchtype=simple"
            # 連結到目標網站
            driver.get(url)
            # By種類參看 https://selenium-python.readthedocs.io/locating-elements.html
            time.sleep(2)
            elements = driver.find_elements(By.CLASS_NAME, "info")

            for element in elements:
                data.append(element.text)
                time.sleep(0.2)
            movie_revenue.append(data[1])
            
        except Exception as e:
            print(f"Error processing MovieId {MovieTitle}: {e}")
            continue  # 繼續處理下一個 MovieId
     
    # 關閉driver
    driver.close()
    return movie_revenue




file_path = "/workspaces/TIR104_g2/Ａ_raw_data/TW/tmdb_detail_raw_en.csv"
try:
    # 讀取csv文件
    dftmdb = pd.read_csv(file_path, encoding= "utf-8-sig")
    # print(dftmdb)
except Exception as e:
    print(f"Error reading file: {e}")


print(dftmdb['title'])
