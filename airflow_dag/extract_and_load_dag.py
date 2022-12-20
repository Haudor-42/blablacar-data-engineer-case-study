# !/usr/bin/python
# -*- coding: utf-8 -*-

# Modules import
import uuid
import time
import airflow
import pendulum
import datetime
import pandas as pd
from airflow.operators import python_operator
from airflow.contrib.operators import gcs_to_bq, bigquery_operator, bigquery_table_delete_operator

# Custom modules
import custom_operator.custom_clean_files_operator as custom_clean_files_operator

# Set the project, dataset and table name
GCP_PROJECT_NAME = 'test_project'
GCP_DATASET_NAME = 'dw_test'
GCP_TABLE_NAME = 'lines'

# Define the local timezone
local_tz = pendulum.timezone('Europe/Paris')

# Build GCP destination and temporary destination using project, dataset and table names
gcp_destination = GCP_PROJECT_NAME + "." + GCP_DATASET_NAME + "." + GCP_TABLE_NAME
gcp_temporary = GCP_PROJECT_NAME + ".dw_temporary." + GCP_TABLE_NAME

# Define default_args
default_args = {
    'owner': 'Lines SQUAD',
    'depends_on_past': False,
    'email': ['alias-lines-squad@mail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2022, 12, 20),
    'end_date': datetime.datetime(2022, 12, 25)
}

# Define a function that represents the task we want to perform
def extract_and_transform_data(**kwargs):
    """
    Method used to extract and transform data.
    """

    # Set the base URL and endpoint for the API --> In an Airflow context, we'd prefer to use set-up Connection rather than environment variables
    # Additionally this might result in the creation of a dedicated Operator if it makes sense
    base_url = os.environ.get("SOURCE_API_BASE_URL", "default_base_url")
    endpoint = os.environ.get("SOURCE_API_ENDPOINT", "default_endpoint")

    # Send the request to the API
    response = requests.get(base_url + endpoint)

    # Check if the request was successful
    if response.status_code == 200:

        # Extract the data from the response
        data = response.json()

        # Create a list to store the data
        rows = []

        # Iterate over the lines in the data
        for pk_line_id, line_data in data.items():
            # Extract the fields we are interested in
            line_name = line_data.get("LineName", None)
            transport_type = line_data.get("TransportType") if line_data.get("TransportType") in ['BUS', 'TRAIN', 'METRO', 'BOAT', 'TRAM'] else None
            line_public_number = line_data.get("LinePublicNumber", None)
            data_owner_code = line_data.get("DataOwnerCode", None)
            destination_name_50 = line_data.get("DestinationName50", None)
            line_planning_number = line_data.get("LinePlanningNumber", None)
            line_direction = line_data.get("LineDirection", None)

            # Add technical columns such as the source system and unique identifier generated through the ETL
            source_system = "http://v0.ovapi.nl/line/"
            uuid_line = str(uuid.uuid4())

            # Add the data to the list
            rows.append((uuid_line, pk_line_id, line_name, transport_type, line_public_number, data_owner_code, destination_name_50, line_planning_number, line_direction, source_system))

        # Convert the list to a Pandas dataframe
        df = pd.DataFrame(rows, columns=["uuid_line", "pk_line_id", "line_name", "transport_type", "line_public_number", "data_owner_code", "destination_name_50", "line_planning_number", "line_direction", "source_system"])

        df.to_parquet('/home/airflow/gcs/data/' + GCP_DATASET_NAME + '.' + GCP_TABLE_NAME + '_transformed_' + kwargs['execution_date'].strftime('%Y-%m-%d') + '.parquet', index=False)

    return {
        'task_status': 'Transformation step: success',
        'result_length': len(final_df.index)
    }

# Create a DAG instance
with airflow.DAG(
        GCP_DATASET_NAME + '.' + GCP_TABLE_NAME,
        'catchup=False',
        default_args=default_args,
        schedule_interval='@daily') as dag:

    # Create an instance of PythonOperator to extract and transform data, output to cloud storage and assumes we are using Composer (Airflow as a Service), this can easily be adapted
    extract_and_transform_op = python_operator.PythonOperator(
        task_id='transform_step',
        provide_context=True,
        python_callable=extract_and_transform_data,
        dag=dag
    )

    # Use the Cloud Storage to BigQuery operator and use the PARQUET generated file to load data to temporary table
    gcs_to_bq_op = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='to_bq_step',
        bucket='{{ var.value.GCP_BUCKET_NAME }}',
        source_objects=['data/' + GCP_DATASET_NAME + '.' + GCP_TABLE_NAME + '_transformed_{{ ds }}.parquet'],
        destination_project_dataset_table='{{ var.value.GCP_PROJECT_NAME }}:dw_temporary' + '.' + GCP_TABLE_NAME + '_{{ ds_nodash }}',
        schema_fields=[
            {'name': 'uuid_line', 'type': 'STRING', 'mode': 'REQUIRED', 'description': 'A unique identifier generated through the ETL process.'},
            {'name': 'pk_line_id', 'type': 'STRING', 'mode': 'REQUIRED', 'description': 'Primary key of the table. A line is a predetermined route along several timingpoints.'},
            {'name': 'line_name', 'type': 'STRING', 'mode': 'NULLABLE', 'description': 'Name of the line.'},
            {'name': 'transport_type', 'type': 'STRING', 'mode': 'NULLABLE',  'description': 'Type of transport, it has to be one of: BUS, TRAIN, METRO, BOAT, TRAM.'},
            {'name': 'line_public_number', 'type': 'STRING', 'mode': 'NULLABLE',  'description': 'Line number used when communicated with travellers. Communicated as STRING from source of truth.'},
            {'name': 'data_owner_code', 'type': 'STRING', 'mode': 'REQUIRED', 'description': 'Data owner code.'},
            {'name': 'destination_name_50', 'type': 'STRING', 'mode': 'NULLABLE',  'description': 'Destination name.'},
            {'name': 'line_planning_number', 'type': 'STRING', 'mode': 'REQUIRED', 'description': 'Line planning number. Communicated as STRING from source of truth.'},
            {'name': 'line_direction', 'type': 'INTEGER', 'mode': 'REQUIRED', 'description': 'Direction of the line.'},
            {'name': 'load_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE', 'defaultValueExpression': 'CURRENT_TIMESTAMP()', 'description': 'Technical data corresponding to latest load date and time.'},
            {'name': 'source_system', 'type': 'STRING', 'mode': 'REQUIRED', 'description': 'Source system from which the data has been extracted.'}
        ],
        source_format='PARQUET',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        dag=dag
    )

    # Merge the previously loaded data in the destination table
    bq_merge_query_op = bigquery_operator.BigQueryOperator(
        task_id='merge_bq_step',
        sql="""
            MERGE dw_test.lines B
            USING dw_temporary.lines_{{ ds_nodash }} N
            ON B.pk_line_id = N.pk_line_id
            WHEN MATCHED THEN
              UPDATE SET
                uuid_line = N.uuid_line,
                line_name = N.line_name,
                transport_type = N.transport_type,
                line_public_number = N.line_public_number,
                data_owner_code = N.data_owner_code,
                destination_name_50 = N.destination_name_50,
                line_planning_number = N.line_planning_number,
                line_direction = N.line_direction,
                load_timestamp = CURRENT_TIMESTAMP(),
                source_system = N.source_system
            WHEN NOT MATCHED THEN
              INSERT (
                uuid_line,
                pk_line_id,
                line_name,
                transport_type,
                line_public_number,
                data_owner_code,
                destination_name_50,
                line_planning_number,
                line_direction,
                load_timestamp,
                source_system
              ) VALUES(
                N.uuid_line,
                N.pk_line_id,
                N.line_name,
                N.transport_type,
                N.line_public_number,
                N.data_owner_code,
                N.destination_name_50,
                N.line_planning_number,
                N.line_direction,
                CURRENT_TIMESTAMP(),
                N.source_system
              ) """,
        use_legacy_sql=False,
        dag=dag
    )

    # Delete the temporary table
    bq_delete_op = bigquery_table_delete_operator.BigQueryTableDeleteOperator(
        task_id='delete_bq_step',
        deletion_dataset_table='{{ var.value.GCP_PROJECT_NAME }}.dw_temporary' + '.' + GCP_TABLE_NAME + '_{{ ds_nodash }}',
    )

    # Clean the generated files
    clean_file_op = custom_clean_files_operator.CustomCleanFilesOperator(
        task_id='clean_file_step',
        files=['_transformed_', '_from_lucca_'],
        dataset=GCP_DATASET_NAME,
        table_name=GCP_TABLE_NAME,
        date_str='{{ ds }}',
        dag=dag
    )

    # Set the task dependencies being: extract & transform -> load in tmp table -> merge with destination table -> clean tmp table -> clean generated files
    extract_and_transform_op >> gcs_to_bq_op >> bq_merge_query_op >> bq_delete_op >> clean_file_op
