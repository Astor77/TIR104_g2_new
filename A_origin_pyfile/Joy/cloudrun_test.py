import pandas as pd
from wordcloud import WordCloud  
import matplotlib.pyplot as plt  

# 讀取CSV文件  
df = pd.read_csv('tmdb_keywords_temp_20250219.csv')  

# 假設文本數據在名為'text_column'的列中  
text = ' '.join(df['name'].dropna())



# 創建詞雲  
wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)  

# 顯示詞雲  
plt.figure(figsize=(10, 5))  
plt.imshow(wordcloud, interpolation='bilinear')  
plt.axis('off')  # 不顯示坐標軸  
plt.show()