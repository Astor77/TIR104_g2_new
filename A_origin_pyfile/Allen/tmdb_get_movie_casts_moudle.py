

ALLEN_TMDB_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJkYzgwM2UyY2E0OWVlZjAxZDE3M2I4ZmEwZDZkZTQ3NCIsIm5iZiI6MTczNjk0NzYzNS45ODg5OTk4LCJzdWIiOiI2Nzg3YjdiMzgyYTY2NTQxOWViYWZlMTQiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.L3bI0Yl-M18pNoBH_Uu5EY2OdU_q1t3aTaeLr844ZR0"
# tmdb_get_movie_casts
# 針對tmdb_ids抓取每一部電影cedits，結果存回一個list
def tmdb_get_movie_casts(tmdb_ids: list, language: str="zh-TW", API_KEY: str = ALLEN_TMDB_KEY) -> list:
    """
    針對tmdb_ids抓取每一部電影casts，返回 list
    Args:
        tmdb_ids (list): one tmdb movie id
        languages (str): 查詢的語言，預設為 zh-TW
        API_KEY (str): API KEY 資訊，預設為 ALLEN 的API
        return: 含多筆 movie casts 資料的 list
    """
    try:
        all_tmdb_id_cast_data = []
        for tmdb_id in tmdb_ids:
            tmdb_id = int(tmdb_id)
            all_tmdb_id_cast_data.append(tmdb_get_movie_casts(tmdb_id, language, API_KEY))
            time.sleep(0.5)
        return all_tmdb_id_cast_data
    except Exception as err:
        print(f"tmdb_id: {tmdb_id}, error: {err}")
