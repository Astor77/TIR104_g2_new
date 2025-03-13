from prefect import task, flow
from tasks.Fetching_Task import fetch_omdb_data_module as odm

@task
def get_id():
    movie_id_dp = odm.fetch_imdb_id()
    return movie_id_dp

@task
def get_api(movie_id_dp, API_TOKEN):
    results, id_list = odm.crawl_omdb_movies_data(movie_id_dp, API_TOKEN)
    return results, id_list


@task
def save(results, id_list):
    odm.save_data(results)
    odm.id_list_save(id_list)

@task
def get_api_second():
    odm.crawl_omdb_movies_data_second()

@flow
def f3_omdb_movie_data_flow():
    movie_id_dp = get_id()
    results, id_list = get_api(movie_id_dp, odm.API_TOKEN)
    task3 = save(results, id_list)
    get_api_second(wait_for=[task3])



if __name__ == "__main__":
    f3_omdb_movie_data_flow()
