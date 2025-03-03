# 此為老師deployment的範例寫法改寫，先留存

from flows.f6_transform_to_temp_flow import f6_transform_to_temp_flow


if __name__ == "__main__":
    from prefect_github import GitHubRepository

    f6_transform_to_temp_flow.from_source(
        # 這邊的load 後面接的是你 Block(github-repository)的名稱
        source=GitHubRepository.load("github-repository-tir104-g2-new"),
        entrypoint="src/flows/f6_transform_to_temp_flow.py:f6_transform_to_temp_flow",
    ).deploy(
        # 以下是deployment 名稱
        name="test-deploy-f6",
        tags=["test", "tir104-g2-new"],
        # work pool 名字
        work_pool_name="dev-tir104-g2-new",
        job_variables=dict(pull_policy="Never"),
        cron="00 17 * * 4"  # ✅ 每週四 17:00 執行
    )
