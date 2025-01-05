import dlt
import requests


# Example resource
@dlt.resource
def github_events():
    url = f"https://api.github.com/orgs/dlt-hub/events"
    response = requests.get(url)
    yield response.json()

@dlt.resource
def github_repos():
    response = requests.get("https://api.github.com/orgs/dlt-hub/repos")
    yield response.json()


@dlt.transformer(data_from=github_repos, table_name="stargazers")
def github_stargazers(github_repos):
    for repo in github_repos:
        try:
            repo_name = repo['name']
            print(f"https://api.github.com/repos/dlt-hub/{repo_name}/stargazers")
            response = requests.get(f"https://api.github.com/repos/dlt-hub/{repo_name}/stargazers")
            yield response.json()
        except NameError:
            print(f"Error is: {NameError}")


"""
Question

How many columns has the github_repos table? Use duckdb connection, sql_client or pipeline.dataset().

Answer:

60
"""


"""
Exercise 2: Create a pipeline for GitHub API - stargazers endpoint

Create a dlt.transformer for the "stargazers" endpoint https://api.github.com/repos/OWNER/REPO/stargazers for dlt-hub organization.
Use github_repos resource as a main resource for the transformer:
Get all dlt-hub repositories.
Feed these repository names to dlt transformer and get all stargazers for all dlt-hub repositories.
"""

# here is your code
@dlt.source
def github_source():
   return github_events, github_repos, github_stargazers

# Set pipeline name, destination, and dataset name
pipeline = dlt.pipeline(
    pipeline_name="github_pipeline",
    destination="duckdb",
    dataset_name="github_source"
)

load_info = pipeline.run(github_source())
print(load_info)

#print(pipeline.dataset(dataset_type="default").github_repos.df())