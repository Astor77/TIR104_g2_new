import pandas as pd
from wordcloud import WordCloud
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from tasks.Storage_Task.read_file_module import read_file_to_df
import utils.path_config as p
import gc

# 讀取CSV文件  
df_details = read_file_to_df(p.temp_tw, p.details_csv)
df_keywords = read_file_to_df(p.temp_tw, p.keywords_csv)  
df_genre = read_file_to_df(p.temp_tw, p.genres_csv)

df_details_merged = pd.merge(df_details, df_keywords,
                             how = 'left',
                             left_on= "id",
                             right_on= "tmdb_id")

# print(df_details_merged.head())

df_details_merged2 = pd.merge(df_details_merged, df_genre,
                             how = 'left',
                             left_on= "id",
                             right_on= "tmdb_id")

# print(df_details_merged2.head())

genre_list = df_details_merged2["id_y"].dropna().unique()
print(genre_list)



# 假設文本數據在名為'text_column'的列中
for genre in genre_list:

    mask = df_details_merged2["id_y"] == genre
    df_final = df_details_merged2[mask]
    # print(df_final.head())

    text = ' '.join(df_final['name'].dropna())

    # 創建詞雲  
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)  

    # 顯示詞雲  
    plt.figure(figsize=(10, 5))  
    plt.imshow(wordcloud, interpolation='bilinear')  
    plt.axis('off')  # 不顯示坐標軸  
    plt.savefig(f"/workspaces/TIR104_g2_new/A_origin_pyfile/Joy/cloudrun_pic/{int(genre)}.png")
    plt.close()

    # 釋放記憶體
    del wordcloud
    gc.collect()