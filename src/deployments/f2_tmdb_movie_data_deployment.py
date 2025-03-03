# 此為老師deployment的範例寫法改寫，先留存

from flows.f2_tmdb_movie_data_flow import f2_tmdb_movie_data_flow


if __name__ == "__main__":
    from prefect_github import GitHubRepository

    f2_tmdb_movie_data_flow.from_source(
        # 這邊的load 後面接的是你 Block(github-repository)的名稱
        source=GitHubRepository.load("github-repository-tir104-g2-new"),
        entrypoint="src/flows/f2_tmdb_movie_data_flow.py:f2_tmdb_movie_data_flow",
    ).deploy(
        # 以下是deployment 名稱
        name="test-deploy-f2",
        tags=["test", "tir104-g2-new"],
        # work pool 名字
        work_pool_name="dev-tir104-g2-new",
        job_variables=dict(pull_policy="Never"),
        cron="0 21 * * 2"  # ✅ 每週二 21:00 執行
    )
