# Sample data containing pokemon details
data = [
    {"id": "1", "name": "bulbasaur", "size": {"weight": 6.9, "height": 0.7}},
    {"id": "4", "name": "charmander", "size": {"weight": 8.5, "height": 0.6}},
    {"id": "25", "name": "pikachu", "size": {"weight": 6, "height": 0.4}},
]

import dlt

@dlt.resource(
    name='pokemon',
    write_disposition='append', # <--- add new argument into decorator
)
def pokemon():
    yield data


pipeline = dlt.pipeline(
    pipeline_name="poke_pipeline",
    destination="duckdb",
    dataset_name="pokemon_data",
)

load_info = pipeline.run(pokemon)
print(load_info)

# explore loaded data
pipeline.dataset(dataset_type="default").pokemon.df()