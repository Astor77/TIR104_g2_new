import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"
from google.cloud import storage

def upload_folder_to_gcs(bucket_name, local_folder):
    """將整個專案目錄上傳到 GCS 儲存桶，並確保 GCS 顯示資料夾層級"""

    # 初始化 GCS 客戶端
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # 遍歷目錄中的所有檔案與資料夾
    for root, dirs, files in os.walk(local_folder):
        relative_root = os.path.relpath(root, local_folder)  # 計算相對路徑
        relative_root = relative_root.replace("\\", "/")  # 修正 Windows 反斜槓問題

        # 上傳空資料夾占位符
        if not files and not dirs:
            placeholder_blob = bucket.blob(relative_root + "/.keep")  
            placeholder_blob.upload_from_string("")  
            print(f"已建立空資料夾占位符：gs://{bucket_name}/{relative_root}/.keep")

        # 上傳檔案
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_folder)  
            relative_path = relative_path.replace("\\", "/")  # 修正 Windows 反斜槓問題
            
            if file == ".keep":
                continue  # 避免重複上傳 `.keep`

            blob = bucket.blob(relative_path)
            blob.upload_from_filename(local_path)
            print(f"已上傳：{local_path} → gs://{bucket_name}/{relative_path}")

if __name__ == "__main__":
    # 設定 GCS 儲存桶名稱
    GCS_BUCKET_NAME = "tir104-bucket-02"

    # 設定要上傳的本機目錄（專案目錄）
    LOCAL_PROJECT_FOLDER = "C:\project_2\TIR104_g2_new\A_origin_pyfile\Rain\GCS"

    # 執行上傳
    upload_folder_to_gcs(GCS_BUCKET_NAME, LOCAL_PROJECT_FOLDER)

