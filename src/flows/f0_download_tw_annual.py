import json
import pandas as pd
from prefect import task, flow
from tasks.Mapping_Task import selenium_data_module as mselenium
from tasks.Storage_Task.save_file_module import save_as_csv,save_as_json
import utils.path_config as p

# 這個flow是下載台灣年度資料

# 運用selenium_data_task.py（Selenium 抓年度票房資料）
# task 1
#下載全國2022-2025的年票房資料json檔案
@task
def e_get_tw_annual_sales(year_list, date) -> None:
    # 2022-2025年資料
    year_list = [2022, 2023]

    mselenium.download_annual_rename(year_list, date)

# task 2
# 清洗原始json檔案
@task
def e_tw_clean_annual_sales(file_path:str) -> json:

    cleaned_data = mselenium.clean_json_file(file_path)
    extract_annual_sales = mselenium.extract_json(cleaned_data)
    return extract_annual_sales


#task 3
#儲存成json
@task
def save_tw_annual_sales(extract_annual_sales: json, file_name_new: str) -> None:

    save_as_json(extract_annual_sales, file_name_new, p.raw_tw_year_sales)
    print(f"清理後的 JSON 已儲存到: {p.raw_tw_year_sales}")

#task 5
#合併2022, 2023, 2024, 2025年的資料
@task
def t_tw_concat_df_json_annual_sales(year_list) -> pd.DataFrame:

    combined_df = mselenium.concat_df_json(year_list)
    return combined_df

#task 6
#合併2022, 2023, 2024, 2025年的資料，並刪除重複id的資料
@task
def t_tw_concat_df_json_distinct_annual_sales(year_list) -> pd.DataFrame:

    combined_df2 = mselenium.concat_df_json_distinct(year_list)
    return combined_df2

@flow(name="f0_download_tw_annual")
def download_tw_annual_sales_flow() -> None:
    # 2022-2025年資料
    year_list = [2022, 2023, 2024, 2025]
    # 下載日期
    date = "02-10"
    e_get_tw_annual_sales(year_list, date)

    dir_path = p.raw_tw_year_sales
    file_name = "2022年票房資料_raw.json"
    file_path = dir_path / file_name
    extract_annual_sales = e_tw_clean_annual_sales(file_path)

    file_name_new = "2022年票房資料.json"
    save_tw_annual_sales(extract_annual_sales, file_name_new)

    file_name = "2023年票房資料_raw.json"
    file_path = dir_path / file_name
    extract_annual_sales = e_tw_clean_annual_sales(file_path)

    file_name_new = "2023年票房資料.json"
    save_tw_annual_sales(extract_annual_sales, file_name_new)

    file_name = "2024年票房資料_raw.json"
    file_path = dir_path / file_name
    extract_annual_sales = e_tw_clean_annual_sales(file_path)

    file_name_new = "2024年票房資料.json"
    save_tw_annual_sales(extract_annual_sales, file_name_new)

    file_name = "2025年票房資料_raw.json"
    file_path = dir_path / file_name
    extract_annual_sales = e_tw_clean_annual_sales(file_path)

    file_name_new = "2025年票房資料.json"
    save_tw_annual_sales(extract_annual_sales, file_name_new)

    combined_df = t_tw_concat_df_json_annual_sales(year_list)
    save_as_csv(combined_df, "TWMovie2022-2025_raw.csv", p.raw_tw_2022_2025)
    
    combined_df2 = t_tw_concat_df_json_distinct_annual_sales(year_list)
    save_as_csv(combined_df2, "TWMovie2022-2025.csv", p.raw_tw_2022_2025)

if __name__ == "__main__":
    download_tw_annual_sales_flow()