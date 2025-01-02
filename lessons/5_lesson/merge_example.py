# We added `created_at` field to the data
data = [
    {
        "id": "1",
        "name": "bulbasaur",
        "size": {"weight": 6.9, "height": 0.7},
        "created_at": "2024-12-01",
        "updated_at": "2024-12-01"    # <------- new field
    },
    {
        "id": "4",
        "name": "charmander",
        "size": {"weight": 8.5, "height": 0.6},
        "created_at": "2024-09-01",
        "updated_at": "2024-09-01"    # <------- new field
    },
    {
        "id": "25",
        "name": "pikachu",
        "size": {"weight": 9, "height": 0.4}, # <----- pikachu gained weight from 6 to 9
        "created_at": "2023-06-01",
        "updated_at": "2024-12-16"    # <------- new field, information about pikachu has updated
    },
]

import dlt

@dlt.resource(
    name="pokemon",
    write_disposition="merge",  # <--- change write disposition from 'append' to 'merge'
    primary_key="id",  # <--- set a primary key
)
def pokemon(cursor_date=dlt.sources.incremental("updated_at", initial_value="2024-01-01")):  # <--- change the cursor name from 'created_at' to 'updated_at'
    yield data

pipeline = dlt.pipeline(
    pipeline_name="poke_pipeline_dedup",
    destination="duckdb",
    dataset_name="pokemon_data",
)

load_info = pipeline.run(pokemon)
print(load_info)

# explore loaded data
pipeline.dataset(dataset_type="default").pokemon.df()