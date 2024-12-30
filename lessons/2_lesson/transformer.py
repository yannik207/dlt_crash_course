"""
imagine a scenario where you need an additional step in between
This step could arise, for example, in a situation where:
Resource 1 returns a list of pokemons IDs, and you need to use each of those IDs to retrieve detailed information about the pokemons from a separate API endpoint.
"""

import dlt
from dlt.sources.helpers import requests


# Define a transformer to enrich pokemon data with additional details
@dlt.resource(table_name='pokemon')
def my_dict_list():
    yield from data # <--- This would yield one item at a time


@dlt.transformer(data_from=my_dict_list, table_name='detailed_info')
def details(data_item): # <--- Transformer receives one item at a time
    id = data_item["id"]
    url = f"https://pokeapi.co/api/v2/pokemon/{id}"
    response = requests.get(url)
    details = response.json()

    yield details


load_info = pipeline.run(details())
print(load_info)