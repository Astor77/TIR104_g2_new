from datetime import datetime
from prefect import task


#API_TOKEN = "de467a5d"
API_TOKEN = "5271bd7c"
#時間戳記
timestamp = datetime.now().strftime("%Y-%m-%d")
#第二次存檔function用
filepath = r"/workspaces/TIR104_g2_new/A0_raw_data/tw/omdb_info/omdb_raw_data_2025-02-23.json"

