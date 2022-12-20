# blablacar-data-engineer-case-study

Hi, I'm [Elliott](https://www.linkedin.com/in/elliott-audo-1aab17a7/) and this my production for the Confirmed Data Engineer Case Study relating to BlaBlaCar interview process.
I hope this will be relevant and look forward discussing it soon :)

## Getting started in local

### Prerequisites
- Python 3
- As the chosen DB solution is BigQuery in this example, this assumes you have access to GCP and can import a `keyfile.json` file in the `config/` directory.

### Install dependencies
- ```cd python_script```
- ```pip3 install -r requirements.txt```

### Set and export environement variables

Make sure to duplicate the `.env.dist` in a `.env` file and populate it with correct variables.

- ```export $(cat .env)```

### Run
- ```python3 extract_and_load.py```


## Repository composition

Under the `/sql_resources` folder, you will find:

- `lines_ddl.sql`: This is the Data Definition Language script which contains the query to create the destination table.

- `merge_dml.sql`: This is the Data Manipulation Language script which contains the query to merge the data loaded in a temporary table to the destination table.


Under the `/python_script` folder, you will find:

- `extract_and_load.py`: This is the a Python script which queries the endpoint, extract responses, prepare the data and load it. It has been improved to handle edge cases and production scenario. I have chosen to apply a merge strategy in which data are loaded to a temporary destination and merge is performed in SQL. In this context, script also handles the deletion of the temporary table.

- `requirements.txt`: This holds the python dependencies version for the project.

- `.env.dist` and `.env`: The first one is a variable-less version of the `.env` file to create.

- `/config`: This is a folder that holds the `keyfile.json` which contains authentication configuration parameters to the Google Cloud Platform.


Under the `/airflow_dag` folder, you will find:

- `extract_and_load_dag.py`: This is the DAG code base.

- `/custom_operator` that contains one custom operator `custom_clean_files_operator.py` which allows to delete the files processed during the ETL.

The Airflow DAG implementation implies that we are using a Composer cluster (Airflow as a service) with embedded access to GCP and a back-end that relies on Google Cloud Storage.


Under the `/data_modelisation` folder, you will find:

-  `analytic_data_model.pdf`: This is a data model made using [LucidChart](https://www.lucidchart.com) and aims at translating the given scenario in a data structure usable by data analysts & business analysts.

- `analytic_data_model_documentation.md`: This is the centralisation of documentation concerning the analytic data model designed. It contains table description, relationships explanation, columns documentation added to partition / clustering informations.

- `analytic_data_model_query_sample.md`: A set of query to demonstrate how the data could be consumed in this scenario.

## Destination table specification

The query to produce the table is the following:

```
CREATE TABLE IF NOT EXISTS `destination_project.destination_dataset.lines` (
  `uuid_line` STRING NOT NULL,
  `pk_line_id` STRING NOT NULL,
  `line_name` STRING,
  `transport_type` STRING,
  `line_public_number` STRING,
  `data_owner_code` STRING NOT NULL,
  `destination_name_50` STRING,
  `line_planning_number` STRING NOT NULL,
  `line_direction` INTEGER NOT NULL,
  `load_timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  `source_system` STRING NOT NULL
)
PARTITION BY DATE(`load_timestamp`)
CLUSTER BY `line_public_number`
OPTIONS (
  description = 'Consume the public API for “Transport for The Netherlands” which provides information about OVAPI, country-wide public transport',
  labels = [('org_unit', 'transport_for_netherlands'), ('information_type', 'ovapi')]
);
```

It has been designed according to this [documentation](https://github.com/koch-t/KV78Turbo-OVAPI/wiki/Line)
And it result in the following structure:

| Column Name | Data Type | Nullable | Description |
| ----------- | --------- | -------- | ----------- |  
| uuid_line | STRING | NO | A unique identifier generated through the ETL process. |
| pk_line_id | STRING | NO | Primary key of the table. A line is a predetermined route along several timingpoints. |
| line_name | STRING | YES | Name of the line. |
| transport_type | STRING | YES | Type of transport, it has to be one of: BUS, TRAIN, METRO, BOAT, TRAM. |
| line_public_number | STRING | YES | Line number used when communicated with travellers. Communicated as STRING from source of truth. |
| data_owner_code | STRING | NO | Data owner code. |
| destination_name_50 | STRING | YES | Destination name. |
| line_planning_number | STRING | NO | Line planning number. Communicated as STRING from source of truth. |
| line_direction | INTEGER | NO | Direction of the line. |
| load_timestamp | TIMESTAMP | YES | Technical data corresponding to latest load date and time. |
| source_system | STRING | NO | Source system from which the data has been extracted. |


There are a couple of thing to discuss about this but mainly:

- we can find all the possible fields retrieved by the `/line/` endpoint that are documented, some being NULLABLE as optionally provided by the API
- a technical column `uuid_line` has been added to the structure, this is an internal identifier produce along the ETL process which mainly serves a deduplication/quality purpose in case there would be issues when loading.
- a technical column `load_timestamp` has been added to the structure, this is also generated along the ETL process and serves partitioning purpose (debatable) as well as quality and deduplication matters.
- a technical column `source_system` which allows to identify the source of truth for the data.

As mentioned, this table would be partitioned by ingestion date (`load_timestamp`) which is debatable here regarding the context and the non-incremental loads but could be adapted in a production use case.

Moreover, the table would also be clustered by `line_public_number` as this is likely a parameter that would be used a lot when consuming this table. Clustering in BigQuery is a way to improve query performance by physically organizing the data based on the values in one or more columns. When we cluster a table, BigQuery stores the rows in the table based on the order of the values in the clustering columns. This can make it faster to retrieve rows that have the same values in the clustering columns, because those rows are stored together on the same node.


## Improvements solution for the Python script

Here are couple of ideas I couldn't implement by lack of time / in order to keep the basic solution simple but in order to handle production scenario in which ETL should be able to handle large amount of data (ingestion, transformation & loading) the following ideas could be tested:

- Use a library like `pyarrow` to stream data directly from the API response to BigQuery, rather than storing it in a local Pandas dataframe first. This can reduce the amount of memory required and make the process much more efficient.

- Use the `pandas.read_json()` function to parse the API response, rather than calling `.json()` and then iterating over the resulting dictionary. This can be more efficient, especially when dealing with large amounts of data. Some edge cases could then be dealt differently.

- Use the `pandas.DataFrame.to_gbq()` function to write data directly to BigQuery, rather than using the BigQuery client library. This can be more efficient and easier to use, especially when dealing with large amounts of data. I have not yet had occasion to test this out but this seems great.

- Consider using a staging table to temporarily store the data before inserting it into the final destination table. This can allow us to perform data transformations or quality checks on the data before making it available to users. This is similar to the solution I implemented but in this scenario it would likely be in addition to using one of the solution previously mentioned and performing transformations + quality checks in SQL before the merge.

- Use and improve the table partitioning and clustering to optimize the organization of the data in BigQuery. This can improve the performance of our queries, especially when dealing with large tables.

- Consider using a batch processing framework to parallelize the data processing and improve the performance of the ETL pipeline.


## Improvements solution specific to the Airflow context

- Use Airflow's `BranchPythonOperator` to perform a quick check on the API response before proceeding with the rest of the DAG. For example, we could check the status code and skip the rest of the DAG if it is not 200. This can save time and resources if the API is unavailable or if the data is not what we expect.

- Use Airflow's `BigQueryOperator` to run our BigQuery queries and table operations, rather than using the BigQuery client library. This can make it easier to manage the DAG and take advantage of Airflow's built-in retry and backoff functionality.

- Consider using Airflow's `SubDagOperator` to split the DAG into smaller sub-DAGs, each with its own set of tasks. This can make it easier to manage and troubleshoot the DAG, especially if it becomes large and complex. In this simple context this doesn't seem to be necessary but it is a great way to improve solutions.

- In specific scenarios, we could use Airflow's `BaseOperator` and `PythonOperator` to build custom operators that encapsulate specific logic or functionality. This can make it easier to reuse code and abstract away complex details and therefore would be very interesting if this code has to be mutualized with other DAGs.
