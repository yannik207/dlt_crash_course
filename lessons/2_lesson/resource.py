import dlt

### Upload list of dicts

# Sample data containing pokemon details
data = [
    {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
    {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
    {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
]


# Set pipeline name, destination, and dataset name
# pipeline = dlt.pipeline(
#     pipeline_name="quick_start",
#     destination="duckdb",
#     dataset_name="mydata",
# )

# Create a dlt resource from the data
# A better way is to wrap it in the @dlt.resource decorator which denotes
# a logical grouping of data within a data source, typically holding data of similar structure and origin:

@dlt.resource(table_name='pokemon_new') # <--- we set new table name
def my_dict_list():
    yield data

# Why is it a better way? This allows you to use dlt functionalities 
# to the fullest that follow Data Engineering best practices, including incremental loading and data contracts.

"""
Instead of a dict list, the data could also be a/an:
dataframe
database query response
API request response
Anything you can transform into JSON/dict format
"""


### Upload dataframe
import pandas as pd

# Define a resource to load data from a CSV
@dlt.resource(table_name='df_data')
def my_df():
  sample_df = pd.read_csv("https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv")
  yield sample_df

### Upload database

import dlt
from sqlalchemy import create_engine

# Define a resource to fetch genome data from the database
@dlt.resource(table_name='genome_data')
def get_genome_data():
  engine = create_engine("mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam")
  with engine.connect() as conn:
      query = "SELECT * FROM genome LIMIT 1000"
      rows = conn.execution_options(yield_per=100).exec_driver_sql(query)
      yield from map(lambda row: dict(row._mapping), rows)

### Rest APIs

from dlt.sources.helpers import requests


# Define a resource to fetch pokemons from PokeAPI
@dlt.resource(table_name='pokemon_api')
def get_pokemon():
    url = "https://pokeapi.co/api/v2/pokemon"
    response = requests.get(url)
    yield response.json()["results"]


# load_info = pipeline.run([my_df, get_genome_data, get_pokemon])
# print(load_info)

# # List all table names from the database
# with pipeline.sql_client() as client:
#     with client.execute_query("SELECT table_name FROM information_schema.tables") as table:
#         print(table.df())

"""
A source is a logical grouping of resources, e.g., endpoints of a single API. The most common approach is to define it in a separate Python module.
A source is a function decorated with @dlt.source that returns one or more resources.
A source can optionally define a schema with tables, columns, performance hints, and more.
The source Python module typically contains optional customizations and data transformations.
The source Python module typically contains the authentication and pagination code for a particular API.
Read more about sources and resources here.
"""

@dlt.source
def all_data():
  return my_df, get_genome_data, get_pokemon

# Create a pipeline
pipeline = dlt.pipeline(
    pipeline_name="resource_source_new",
    destination="duckdb",
    dataset_name="all_data"
)

# Run the pipeline
load_info = pipeline.run(all_data())

# Print load info
print(load_info)

"""
Why does this matter?:
It is more efficient than running your resources separately.
It organizes both your schema and your code. 🙂
It enables the option for parallelization.
"""