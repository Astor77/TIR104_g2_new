"""Quick start."""

from prefect import flow


@flow(log_prints=True)
def hello_world_flow(name: str = "world", goodbye: bool = False) -> None:
    print(f"Hello {name} from Prefect! 🤗")

    if goodbye:
        print(f"Goodbye {name}!")


if __name__ == "__main__":

    """
    Run flow directly, just like Python functions. And the process can also be monitored on UI.
    """
    # hello_world_flow()
    # hello_world_flow(name="Ted", goodbye=True)

    """
    Start a temporary local server of worker to execute flows submitted from Prefect cloud.
    It comes with a development named "my-first-deployment" which can be found on UI.
    """
    # hello_world_flow.serve(
    #     name="my-second-deployment",  # Deployment name. It create a temporary deployment.
    #     tags=["onboarding", "tir104"],  # Filtering when searching on UI.
    #     parameters={
    #         "name": "Astor",
    #         "goodbye": True
    #     },  # Overwrite default parameters defined on hello_world_flow. Only for this deployment.
    #     interval=120,  # Like crontab, "* * * * *"
    # )

    """
    Deploy a flow to Prefect cloud.
    """
    from prefect_github import GitHubRepository
    ## 以下這整段都是為了
    ## 要讓他知道這個檔案確切的位置在哪
    ## 這隻檔案指向某個github的repo，把它想像成資料夾
    ## 為什麼要這樣寫？prefect 其實是把檔案存在的位置下載下來
    ## 但一般來說要下載檔案是一件很麻煩的事情
    ## 所以prefect把它包成block資源
    ## 如果這個資源是一個私有的狀態（那就需要credential）
    ## block的應用不限於下載檔案的範圍
    ## 例如我想在AWS上開一台ec2主機，背後需要帶很多步驟、參數進去
    ## 但prefect 把這件事包得很簡單降低難度，讓你輕鬆調度各種資源、服務
    hello_world_flow.from_source(
        source=GitHubRepository.load("github-repository-uuboyscy"),
        entrypoint="src/flows/test/hello_world_flow.py:hello_world_flow",
    ).deploy(
        name="main",  # Deployment name.
        tags=["dev", "sample"],
        work_pool_name="dev-subproc",  # A worker to submit the deployment.
        parameters=dict(
            name="Marvin"
        ),  # Overwrite default parameters defined on hello_world_flow. Only for this deployment.
        cron="*/15 * * * *",  # Crontab for this deployment.
    )
