import json
import pandas as pd
from datetime import datetime
from prefect import task, flow, get_run_logger
from tasks.Mapping_Task import selenium_data_module as mselenium
from tasks.Storage_Task.save_file_module import save_as_csv, save_as_json
import utils.path_config as p

# 這個 flow 是下載台灣年度資料

@task
def e_get_tw_annual_sales(year_list, date) -> None:
    logger = get_run_logger()
    try:
        logger.info(f"開始下載 {year_list} 年度票房資料...")
        mselenium.download_annual_rename(year_list, date)
        logger.info(f"✅ 成功下載 {year_list} 年度票房資料")
    except Exception as e:
        logger.error(f"❌ 下載年度票房資料失敗: {e}")
        raise

@task
def e_tw_clean_annual_sales(file_path: str) -> json:
    logger = get_run_logger()
    try:
        cleaned_data = mselenium.clean_json_file(file_path)
        extract_annual_sales = mselenium.extract_json(cleaned_data)
        logger.info(f"✅ 成功清理 JSON 檔案: {file_path}")
        return extract_annual_sales
    except Exception as e:
        logger.error(f"❌ 清理 JSON 失敗: {e}")
        raise

@task
def save_tw_annual_sales(extract_annual_sales: json, file_name_new: str) -> None:
    logger = get_run_logger()
    try:
        save_as_json(extract_annual_sales, p.raw_tw_year_sales, file_name_new)
        logger.info(f"✅ 清理後的 JSON 已儲存到: {p.raw_tw_year_sales}/{file_name_new}")
    except Exception as e:
        logger.error(f"❌ 儲存 JSON 失敗: {e}")
        raise

@task
def t_tw_concat_df_json_annual_sales(year_list) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        combined_df = mselenium.concat_df_json(year_list)
        logger.info(f"✅ 成功合併 {year_list} 年的 JSON 資料")
        return combined_df
    except Exception as e:
        logger.error(f"❌ 合併 JSON 失敗: {e}")
        raise

@task
def t_tw_concat_df_json_distinct_annual_sales(year_list) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        combined_df2 = mselenium.concat_df_json_distinct(year_list)
        logger.info(f"✅ 成功合併並去重 {year_list} 年的 JSON 資料")
        return combined_df2
    except Exception as e:
        logger.error(f"❌ 合併並去重 JSON 失敗: {e}")
        raise

@flow(name="f0_download_tw_annual")
def download_tw_annual_sales_flow() -> None:
    logger = get_run_logger()
    try:
        logger.info("download_tw_annual_sales_flow()開始運行...")
        year_list = list(range(2022, datetime.today().year + 1))
        #year_list = [2022, 2023, 2024, 2025]
        date = datetime.today().strftime("%m-%d")
        #date = "02-28"
        e_get_tw_annual_sales(year_list, date)

        dir_path = p.raw_tw_selenium

        for year in year_list:
            file_name = f"{year}年票房資料_raw.json"
            file_path = f"{dir_path}/{file_name}"
            extract_annual_sales = e_tw_clean_annual_sales(file_path)

            file_name_new = f"{year} 年票房資料.json"
            save_tw_annual_sales(extract_annual_sales, file_name_new)

        combined_df = t_tw_concat_df_json_annual_sales(year_list)
        save_as_csv(combined_df, p.raw_tw_2022_2025, "TWMovie2022-2025_raw.csv")

        combined_df2 = t_tw_concat_df_json_distinct_annual_sales(year_list)
        save_as_csv(combined_df2, p.raw_tw_2022_2025, "TWMovie2022-2025.csv")

        logger.info("✅ 所有任務成功執行，年度票房資料已處理完成")
    except Exception as e:
        logger.error(f"❌ Flow 執行失敗: {e}")
        raise

if __name__ == "__main__":
    download_tw_annual_sales_flow()
