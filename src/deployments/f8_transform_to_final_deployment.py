# 此為老師deployment的範例寫法改寫，先留存

from flows.f8_transform_to_final_data import f8_monitor_scheduled_queries


if __name__ == "__main__":
    from prefect_github import GitHubRepository

    f8_monitor_scheduled_queries.from_source(
        # 這邊的load 後面接的是你 Block(github-repository)的名稱
        source=GitHubRepository.load("github-repository-tir104-g2-new"),
        entrypoint="src/flows/f8_transform_to_final_data.py:f8_monitor_scheduled_queries",
    ).deploy(
        # 以下是deployment 名稱
        name="test-deploy-f8",
        tags=["test", "tir104-g2-new"],
        # work pool 名字
        work_pool_name="dev-tir104-g2-new",
        job_variables=dict(pull_policy="Never"),
        cron="0 19 * * 4"  # ✅ 每周四 19:00 執行
    )
