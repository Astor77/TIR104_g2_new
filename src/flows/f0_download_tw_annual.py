import json
import pandas as pd
from prefect import task, flow
from tasks.Mapping_Task import map_movie_data_module as mmap, search_movie_api_module as msearch, selenium_data_module as mselenium
from tasks.Storage_Task.save_file_module import save_as_csv,save_as_json
import utils.path_config as p

# 這個flow是下載台灣年度資料

# task X
# 運用selenium_data_task.py（Selenium 抓年度票房資料）





# task 1
#下載全國2022-2025的年票房資料json檔案
@task
def e_get_tw_annual_sales() -> None:
    # 2022-2025年資料
    year_list = [2022, 2023]
    # 2025年更新日期
    date = "02-10"

    mselenium.download_annual_rename(year_list, date)

# task 2
# 清洗原始json檔案
@task
def e_tw_clean_annual_sales() -> None:

    dir_path = p.raw_tw_year_sales
    file_name = "2022年票房資料_raw.json"

    file_path = dir_path / file_name

    cleaned_data = mselenium.clean_json_file(file_path)
    return cleaned_data


@task
def e_tw_extract_annual_sales(cleaned_data) -> json:

    extract_annual_sales = mselenium.extract_json(cleaned_data)
    return extract_annual_sales

@task
def e_tw_save_annual_sales(extract_annual_sales) -> None:

    file_name_new = "2022年票房資料.json"
    save_as_json(extract_annual_sales, file_name_new, p.raw_tw_year_sales)
    print(f"清理後的 JSON 已儲存到: {file_path}")
