from dlt.sources.sql_database import sql_database
import dlt

source = sql_database(
    "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
    table_names=["family",]
)

pipeline = dlt.pipeline(
    pipeline_name="sql_database_example",
    destination="duckdb",
    dataset_name="sql_data",
    dev_mode=True,
)

load_info = pipeline.run(source)
print(load_info)

print(pipeline.dataset(dataset_type="default").family.df())

"""
Question

How many columns has the family table?

37
"""