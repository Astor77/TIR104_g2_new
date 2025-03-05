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
df_annual = read_file_to_df(p.temp_tw, p.tw_annual_csv)
df_keywords = read_file_to_df(p.temp_tw, p.keywords_csv)  
df_genre = read_file_to_df(p.temp_tw, p.genres_csv)

df_details_merged = pd.merge(df_annual, df_details,
                             how = 'left',
                             left_on= "MovieId",
                             right_on= "MovieId")

# print(df_details_merged.columns)

df_details_merged2 = pd.merge(df_details_merged, df_keywords,
                             how = 'left',
                             left_on= "id",
                             right_on= "tmdb_id")

# print(df_details_merged2.columns)

df_details_merged3 = pd.merge(df_details_merged2, df_genre,
                             how = 'left',
                             left_on= "tmdb_id",
                             right_on= "tmdb_id")

print(df_details_merged3.head())

genre_list = df_details_merged3["id_y"].dropna().unique()
print(genre_list)



# 假設文本數據在名為'text_column'的列中
for genre in genre_list:

    df_genre_filtered = df_details_merged3[df_details_merged3["id_y"] == genre]
    
    # 取得該類型中票房前 100 名的電影
    df_top100 = df_genre_filtered.nlargest(100, 'Amount', keep = "all")
    
    # 合併電影名稱來生成詞雲
    text = ' '.join(df_top100['name'].dropna())

    if text:
    # 創建詞雲  
        wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)  

        # 顯示詞雲  
        plt.figure(figsize=(10, 5))  
        plt.imshow(wordcloud, interpolation='bilinear')  
        plt.axis('off')  # 不顯示坐標軸  
        plt.savefig(f"/workspaces/TIR104_g2_new/A_origin_pyfile/Joy/cloudrun_2/{int(genre)}.png")
        plt.close()

        # 釋放記憶體
        del wordcloud
        gc.collect()