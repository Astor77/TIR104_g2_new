from prefect import task, flow
from tasks.Fetching_Task import fetch_omdb_data_module as odm

@task
def get_id():
    odm.fetch_imdb_id()
    

@task
def get_api():
    odm.crawl_omdb_movies_data()


@task
def save():
    odm.save_data()
    odm.id_list_save()

@task
def get_api_second():
    odm.crawl_omdb_movies_data_second()

@flow
def f3_omdb_movie_data_flow():
    task1 = get_id()
    task2 = get_api(wait_for=[task1])
    task3 = save(wait_for=[task2])
    get_api_second(wait_for=[task3])



if __name__ == "__main__":
    f3_omdb_movie_data_flow()
