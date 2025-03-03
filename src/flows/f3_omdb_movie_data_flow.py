from datetime import datetime
from prefect import task, flow
from tasks.Fetching_Task import fetch_omdb_data_module as odm


API_TOKEN = "de467a5d"
#API_TOKEN = "5271bd7c"

#第二次存檔function用
filepath = r"/workspaces/TIR104_g2_new/A0_raw_data/tw/omdb_info/omdb_info.json"


@task
def get_id():
    movie_id = odm.fetch_imdb_id()
    return movie_id

@task
def get_api(movie_id):
    results, id_list = odm.crawl_omdb_movies_data(movie_id, API_TOKEN)
    return results, id_list

@task
def save(results, id_list):
    odm.save_data(results)
    odm.id_list_save(id_list)

@task
def get_api_second():
    odm.crawl_omdb_movies_data_second()

@flow
def main_flow():
    movie_id = get_id()
    results, id_list = get_api(movie_id)
    save(results, id_list)

if __name__ == "__main__":
    main_flow()
