# TIR104_g2_new

## 📌 專案簡介
這個專案負責爬取電影資料，並將處理後的數據存入 GCS 和 BigQuery。

## 主要：
1. **抓取電影資料**
2. **上傳原始資料到 GCS**
3. **資料清理與轉換**
4. **存入 BigQuery**


## 簡易說明
1. **deployments/ 測試部署＆正式部署**
2. **flow/ 以流程的性質區分，避免以API拆分，導致重複、管理不易**
3. **tasks/ 依照用途類別區分，存放著函式模組，每一隻tasks.py內會有n個函式**
4. **utils/ 大家共用的小工具，主要讓大家撰寫程式碼時，更加親民便利**

TIR104_g2_new/
│── README.md               # 本文件
│── A_origin_file           # 存放大家各自的原始py檔案（尚未整併過的版本）
│── project_log             # 存放各式 log file（不會上傳，避免版本衝突）
│── .env                    # API Key 等環境變數（不會上傳）
│── requirements.txt        # 依賴的 Python 套件
│── src/                    # 主要程式碼
|   |── deployments           # 佈署至Prefect Cloud 進行自動化流程監控
│   │── flow/                 # 流程管理
│   │   │── f0_download_tw_annual.py          # 取得台灣年度資料
│   │   │── f1_generate_movie_list_flow.py    # 產生 mapping 電影清單的流程
│   │   │── f2_tmdb_movie_data_flow.py        # 取得tmdb的電影資料
│   │   │── f3_omdb_movie_data_flow.py        # 取得omdb的電影資料
│   │   │── f4_sele_movie_data_flow.py        # 取得sele的電影資料
│   │   │── f5_upload_raw_to_gcs_flow.py      # 上傳各個來源的原始資料至 GCS
│   │   │── f6_transform_to_temp_flow.py      # 第一步的清理與轉換 raw to temp
│   │   │── f7_upload_temp_to_bq.py           # temp data上傳至bigquery
|   |   |── f8_transform_to_final_data.py     # 轉換為最終資料(這段要指使Big Query)
|   |   |── F9_download_final_from_bq.py      # 下載final data提供Tableau進行分析
│   │── tasks/               # 各類任務
│   │   │── __init__.py      # 讓 Python 視為 package
│   │   │── Fetching_Task/   # 資料抓取相關
│   │   │   │── __init__.py      # 讓 Python 視為 package
│   │   │   │── fetch_omdb_data_module.py     # 打 omdb API 抓資料
│   │   │   │── fetch_tmdb_data_module.py     # 打 tmdb API 抓資料
│   │   │── Mapping_Task/    # 最源頭的資料 mapping
│   │   │   │── __init__.py      # 讓 Python 視為 package
│   │   │   │── selenium_data_module.py       # Selenium 爬取資料
│   │   │   │── search_movie_api_module.py    # 搜尋 API
│   │   │   │── map_movie_data_module.py      # 整合與處理數據
│   │   │── Storage_Task/    # 資料儲存與讀取
│   │   │   │── __init__.py      # 讓 Python 視為 package
│   │   │   │── gcs_module.py                 # GCS，Bigquery相關功能(上傳、下載)
│   │   │   │── read_file_module.py           # 讀取 CSV、JSON 轉為pd.DataFrame
│   │   │   │── save_file_module.py           # 本地存檔 CSV、JSON
│   │   │── Transform_Task/  # 轉換數據
│   │   │   │── __init__.py      # 讓 Python 視為 package
│   │   │   │── omdb_transform_temp_module.py # omdb原始資料清理
│   │   │   │── other_module.py               # 時間戳記、dataframe合併功能
│   │   │   │── sele_transform_temp_module.py # sele原始資料清理
│   │   │   │── transform_temp_module.py      # 清理/轉換 raw data 為 temp data
│   │   │   │── transform_final_module.py     # 最終數據轉換（BigQuery SQL）
│   │── utils/               # 輔助工具
│   │   │── __init__.py          # 讓 Python 視為 package
│   │   │── notifier.py                       # Prefect Line報錯通知
│   │   │── write_log_task.py                 # 記錄 Log 的小工具
│   │   │── path.config.py                    # 資料夾路徑變數資料

## README.md 更新紀錄
- 2025.02.17_Astor
- 2025.03.05_Victor

