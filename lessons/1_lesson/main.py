# Sample data containing pokemon details
data = [
    {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
    {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
    {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
]

import dlt
import duckdb
#from google.colab import data_table
#data_table.enable_dataframe_formatter()

# Set pipeline name, destination, and dataset name
"""
A pipeline is a connection that moves data from your Python code to a destination. 
The pipeline accepts dlt sources or resources, as well as generators, async generators, lists, and any iterables. 
Once the pipeline runs, all resources are evaluated and the data is loaded at the destination.
"""
pipeline = dlt.pipeline(
    pipeline_name="quick_start",
    destination="duckdb",
    dataset_name="mydata",
)

# Run the pipeline with data and table name
# To load the data, you call the run method and pass your data in the data argument.
load_info = pipeline.run(data, table_name="pokemon")

print(load_info)

# A database '<pipeline_name>.duckdb' was created in working directory so just connect to it

# Connect to the DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# Describe the dataset
conn.sql("DESCRIBE").df()

# Fetch all data from 'pokemon' as a DataFrame
table = conn.sql("SELECT * FROM pokemon").df()

# Display the DataFrame
print(table)