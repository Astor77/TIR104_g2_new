#「物件名稱（Object Name）」來模擬層級結構
#維持目錄結構的，但 GCS 本質上不支援「資料夾」

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\Projects\project-tir10402-0013120778be.json"

from google.cloud import storage


def upload_folder_to_gcs(bucket_name, local_folder):
    """將整個專案目錄上傳到 GCS 儲存桶"""

    # 初始化 GCS 客戶端
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # 遍歷目錄中的所有檔案
    for root, _, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)

            # 轉換成 GCS 儲存的路徑（去掉本機目錄）
            relative_path = os.path.relpath(local_path, local_folder)
            blob = bucket.blob(relative_path)

            # 上傳檔案
            blob.upload_from_filename(local_path)
            print(f"已上傳：{local_path} → gs://{bucket_name}/{relative_path}")

if __name__ == "__main__":
    # 設定 GCS 儲存桶名稱
    GCS_BUCKET_NAME = "tir-104-02"

    # 設定要上傳的本機目錄（專案目錄）
    LOCAL_PROJECT_FOLDER = "C:\Projects\TIR104_g2\P_Rain"

    # 執行上傳
    upload_folder_to_gcs(GCS_BUCKET_NAME, LOCAL_PROJECT_FOLDER)


