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
        print(f"next page: {page}\n")
        yield page


# define new dlt pipeline
pipeline = dlt.pipeline(destination="duckdb")


# run the pipeline with the new resource
load_info = pipeline.run(github_stargazers)
print(load_info)


# explore loaded data
pipeline.dataset(dataset_type="default").github_stargazers.df()


"""
Exercise 1: Pagination with RESTClient

Explore the cells above and answer the question below.
Question

What type of pagination we use for GitHub API?

If a paginator is not specified, the paginate() method will attempt to automatically 
detect the pagination mechanism used by the API. 
If the API uses a standard pagination mechanism like having a next link in the response's headers or JSON body,
the paginate() method will handle this automatically. Otherwise, you can specify a paginator object explicitly 
or implement a custom paginator.

In this case it is HeaderLinkPaginator
"""


"""
different pagination strategy supervised by dlt:

• JSONLinkPaginator - link to the next page is included in the JSON response.
• HeaderLinkPaginator - link to the next page is included in the response headers.
• OffsetPaginator - pagination based on offset and limit query parameters.
• PageNumberPaginator - pagination based on page numbers.
• JSONResponseCursorPaginator - pagination based on a cursor in the JSON response.
"""