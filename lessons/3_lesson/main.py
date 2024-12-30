import dlt
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from os import environ

# define new resource - github stargazers
@dlt.resource
def github_stargazers():
    client = RESTClient(
        base_url="https://api.github.com",
        auth=BearerTokenAuth(token=environ.get("GITHUB_ACCESS_TOKEN"))
    )

    for page in client.paginate("repos/dlt-hub/dlt/stargazers"):
        yield page


# define new dlt pipeline
pipeline = dlt.pipeline(destination="duckdb")


# run the pipeline with the new resource
load_info = pipeline.run(github_stargazers)
print(load_info)


# explore loaded data
pipeline.dataset(dataset_type="default").github_stargazers.df()