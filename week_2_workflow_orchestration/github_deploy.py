from prefect.filesystems import GitHub
from prefect.deployments import Deployment

github_block = GitHub.load("github-flowcode-block")
github_block.get_directory(from_path='week_2_workflow_orchestration/flows_for_github_block', local_path='./flows_from_github/')

# from flows_from_github.week_2_workflow_orchestration.flows_for_github_block.etl_web_to_gcs import etl_web_to_gcs

github_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name='github-flow'
)

if __name__ == '__main__':
    github_dep.apply()