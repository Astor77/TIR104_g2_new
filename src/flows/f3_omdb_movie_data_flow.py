from prefect import task, flow
from tasks.Fetching_Task import fetch_omdb_data_module as odm

@task
def get_id():
    movie_id_dp = odm.fetch_imdb_id()
    return movie_id_dp

@task
def get_api(movie_id_dp):
    odm.crawl_omdb_movies_data(movie_id_dp)


@task
def save():
    odm.save_data()
    odm.id_list_save()

@task
def get_api_second():
    odm.crawl_omdb_movies_data_second()

@flow
def f3_omdb_movie_data_flow():
    movie_id_dp = get_id()
    task2 = get_api(movie_id_dp)
    task3 = save(wait_for=[task2])
    get_api_second(wait_for=[task3])



if __name__ == "__main__":
    f3_omdb_movie_data_flow()
