import json

import pandas as pd

# 讀取上傳的 JSON 檔案
file_path = "iso3166_country.json"
with open(file_path, "r", encoding="utf-8") as file:
    data = json.load(file)  # 這行需要縮排

# 擷取需要的欄位
countries_data = [
    {
        "release_country_name": item["name"],
        "release_country_code": item["twoLetterCode"],
        "Traditional_Chinese_Name": item["traditionalChineseName"],
    }
    for item in data["list"]
]

# 轉換為 DataFrame
df = pd.DataFrame(countries_data)

# 保存為 CSV 檔案
csv_file_path = "tmdb_release_country.csv"
df.to_csv(csv_file_path, index=False, encoding="utf-8-sig")
