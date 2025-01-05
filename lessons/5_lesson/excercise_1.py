import dlt
from dlt.sources.helpers import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
from os import environ

github_token = environ.get("GITHUB_ACCESS_TOKEN")
cursor_date = dlt.sources.incremental("updated_at", initial_value="2024-12-01")

@dlt.source
def github_comments(access_token=github_token):
    client = RESTClient(
        base_url="https://api.github.com",
        auth=BearerTokenAuth(token=access_token),
        paginator=HeaderLinkPaginator(),
    )
    @dlt.resource(
        name="comments",
        write_disposition="merge",
        primary_key="id"
    )
    def github_comments_dlt(cursor_date=cursor_date):
        params = {
            "since": cursor_date.last_value,  # <--- use last_value to request only new data from API
            "status": "open"
        }
        for page in client.paginate("repos/dlt-hub/dlt/pulls/comments", params=params):
            yield page
    return github_comments_dlt


pipeline = dlt.pipeline(
    pipeline_name="github_dlt_comments",
    destination="duckdb",
    dataset_name="dlt_comments"
)

load_info = pipeline.run(github_comments())
print(load_info)