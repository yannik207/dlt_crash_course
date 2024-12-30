### This Project is to get a better understanding of dlt from dlthub
https://github.com/dlt-hub/dlthub-education/tree/main/courses/dlt_fundamentals_dec_2024

## Lesson 1: Quick Start

Discover what dlt is, run your first pipeline with toy data, and explore it like a pro using DuckDB, sql_client, and dlt datasets!

## Lesson 2: dlt Resources and Sources

Learn to run pipelines with diverse data sources (dataframes, databases, and REST APIs), master dlt.resource, dlt.source, and dlt.transformer, and create your first REST API pipeline!

## Lesson 3: Pagination & Authentication & dlt Configuration

Since it is never a good idea to publicly put your API keys into your code, different environments have different methods to set and access these secret keys. dlt is no different. Master pagination and authentication for REST APIs, explore dlt's RESTClient and manage secrets and configs.

## Lesson 4: Using dlt’s pre-built Sources and Destinations

Now that you took a data source and loaded it into a duckdb destination, it is time to look into what other possibilities dlt offers. In this notebook we will take a look at pre-built verified sources and destinations and how to use them.

## Lesson 5: Write disposition and incremental loading

Learn to control data behavior with dlt write dispositions (Append, Replace, Merge), master incremental loading, and efficiently update and deduplicate your datasets.

## Lesson 6: How dlt works

Discover the magic behind dlt! Learn its three main steps — Extract, Normalize, Load — along with default behaviors and supported file formats.

## Lesson 7: Inspecting & Adjusting Schema

dlt creates and manages the schema automatically, but what if you want to control it yourself? Explore the schema and customize it to your needs easily with dlt!

## Lesson 8: Understanding Pipeline Metadata

After having learnt about pipelines and how to move data from one place to another. We now learn about information about the pipeline itself. Or, metadata of a pipeline that can be accessed and edited through dlt. This notebook explores dlt states, what it collected and where this extra information is stored. It also expands a bit more on what the load info and trace in dlt is capable of.