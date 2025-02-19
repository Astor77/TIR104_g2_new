import pandas as pd
import requests
import time
import os

# 讀取 CSV 檔案
def load_csv(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"找不到檔案: {file_path}")
    
    df = pd.read_csv(file_path)
    if "id" not in df.columns:
        raise ValueError("CSV 檔案缺少 'id' 欄位！")
    return df

# 取得 TMDB API 資料
def fetch_release_dates(id, headers):
    url = f"https://api.themoviedb.org/3/movie/{id}/release_dates"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json().get("results", [])
    else:
        print(f"查詢失敗: {id}, 狀態碼: {response.status_code}, 錯誤訊息: {response.text}")
        return None

# 解析 API 資料
def parse_release_dates(data, id):
    records = []
    for result in data:
        country = result.get("iso_3166_1")
        for release in result.get("release_dates", []):
            records.append({
                "tmdb_id": id,
                "country": country,
                "certification": release.get("certification", ""),
                "descriptors": release.get("descriptors", []),
                "iso_639_1": release.get("iso_639_1", ""),
                "note": release.get("note"),
                "release_date": release.get("release_date"),
                "type": release.get("type")
            })
    return records

# 主要函數: 取得並儲存 TMDB 資料
def fetch_and_save_release_dates(input_file, output_folder):
    df = load_csv(input_file)

    API_KEY = os.getenv("RAIN_TMDB_KEY", "").strip().replace("\"", "")
    if not API_KEY:
        raise ValueError("放置API_KEY")

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "accept": "application/json"
    }
    
    _ids = df["id"].dropna().astype(int).unique()
    release_dates = []
    
    for id in _ids:
        data = fetch_release_dates(id, headers)
        if data:
            release_dates.extend(parse_release_dates(data, id))
        time.sleep(0.5)  # 防止 API 限制
    
    df_releases = pd.DataFrame(release_dates)
    df_releases["release_date"] = pd.to_datetime(df_releases["release_date"], errors="coerce", utc=True)
    df_releases.dropna(subset=["release_date"], inplace=True)
    
    os.makedirs(output_folder, exist_ok=True)
    file_name = os.path.join(output_folder, "v3_release_dates.csv")
    df_releases.to_csv(file_name, index=False, encoding="utf-8-sig")
    print(f"已儲存 {file_name}")

# 確保讀取 CSV 路徑正確
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))  #  Python 檔案所在資料夾
    input_file = os.path.join(script_dir, "v3_test5.csv")  # 建立完整的 CSV 檔案路徑
    output_folder = os.path.join(script_dir, "Rain")  # 指定輸出資料夾
    fetch_and_save_release_dates(input_file, output_folder)
