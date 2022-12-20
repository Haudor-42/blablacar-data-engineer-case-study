# !/usr/bin/python
# -*- coding: utf-8 -*-

# Import packages
import os
import sys
import uuid
import requests
import pandas as pd
from google.cloud import bigquery, exceptions

# Import keyfile
service_account_json = os.environ.get("GCP_SERVICE_ACCOUNT_FILEPATH", "default_file_path")

# Set the project, dataset and table name
gcp_project = os.environ.get("GCP_PROJECT", "default_gcp_project")
gcp_dataset = os.environ.get("GCP_DATASET", "default_gcp_dataset")
gcp_table = os.environ.get("GCP_TABLE", "default_gcp_table")

# Build GCP destination and temporary destination using project, dataset and table names
gcp_destination = gcp_project + "." + gcp_dataset + "." + gcp_table
gcp_temporary = gcp_project + ".dw_temporary." + gcp_table

# Set the base URL and endpoint for the API
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

    # Connect to BigQuery
    client = bigquery.Client.from_service_account_json(service_account_json)

    # Create the table if it doesn't exist
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS """ + gcp_destination + """ (
          uuid_line STRING NOT NULL OPTIONS (description = 'A unique identifier generated through the ETL process.'),
          pk_line_id STRING NOT NULL OPTIONS (description = 'Primary key of the table. A line is a predetermined route along several timingpoints.'),
          line_name STRING OPTIONS (description = 'Name of the line.'),
          transport_type STRING OPTIONS (description = 'Type of transport, it has to be one of: BUS, TRAIN, METRO, BOAT, TRAM.'),
          line_public_number STRING OPTIONS (description = 'Line number used when communicated with travellers. Communicated as STRING from source of truth.'),
          data_owner_code STRING NOT NULL OPTIONS (description = 'Data owner code.'),
          destination_name_50 STRING OPTIONS (description = 'Destination name.'),
          line_planning_number STRING NOT NULL OPTIONS (description = 'Line planning number. Communicated as STRING from source of truth.'),
          line_direction INTEGER NOT NULL OPTIONS (description = 'Direction of the line.'),
          load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS (description = 'Technical data corresponding to latest load date and time.'),
          source_system STRING NOT NULL OPTIONS (description = 'Source system from which the data has been extracted.'))
          PARTITION BY DATE(load_timestamp)
          CLUSTER BY line_public_number
          OPTIONS (
            description = 'Consume the public API for “Transport for The Netherlands” which provides information about OVAPI, country-wide public transport',
            labels = [('org_unit', 'transport_for_netherlands'), ('information_type', 'ovapi')]
          );
    """

    try:
        # Run the query
        create_job = client.query(create_table_sql)

        # Wait for the job to complete
        create_job.result()

        print("Table created or the table already exist: %s" % gcp_destination)

    except exceptions.BadRequest as e:
        # Catch any errors relating to Bad Request that might occur and print the error message
        print(e)
        sys.exit()

    except exceptions.Forbidden as e:
        # Catch any errors relating to permission and rights that might occur and print the error message
        print(e)
        sys.exit()

    except Exception as e:
        # Catch any other errors that might occur and print the error message
        print(e)
        sys.exit()


    try:
        # Load the data from the dataframe to the table
        load_job = client.load_table_from_dataframe(df, gcp_temporary, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))

        # Wait for the job to complete
        load_job.result()

        print("Data loaded successfully to temporary table: %s" % gcp_temporary)

    except exceptions.BadRequest as e:
        # Catch any errors relating to Bad Request that might occur and print the error message
        print(e)
        sys.exit()

    except exceptions.Forbidden as e:
        # Catch any errors relating to permission and rights that might occur and print the error message
        print(e)
        sys.exit()

    except Exception as e:
        # Catch any other errors that might occur and print the error message
        print(e)
        sys.exit()

    # Merge data stored into temporary table to the destination table
    merge_sql = """
        MERGE """ + gcp_destination + """ B
        USING """ + gcp_temporary + """ N
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
          ) """

    try:
        # Merge the data from temporary table to destination table
        merge_job = client.query(merge_sql)

        # Wait for the job to complete
        merge_job.result()

        print("Data merged successfully to the destination table %s" % gcp_destination)

    except exceptions.BadRequest as e:
        # Catch any errors relating to Bad Request that might occur and print the error message
        print(e)
        sys.exit()

    except exceptions.Forbidden as e:
        # Catch any errors relating to permission and rights that might occur and print the error message
        print(e)
        sys.exit()

    except Exception as e:
        # Catch any other errors that might occur and print the error message
        print(e)
        sys.exit()

    # Delete the temporary table
    delete_temporary_table_sql = """
        DROP TABLE """ + gcp_temporary + """;
    """

    try:
        # Merge the data from temporary table to destination table
        delete_job = client.query(delete_temporary_table_sql)

        # Wait for the job to complete
        delete_job.result()

        print("Temporary table deleted: %s" % gcp_temporary)

    except exceptions.BadRequest as e:
        # Catch any errors relating to Bad Request that might occur and print the error message
        print(e)
        sys.exit()

    except exceptions.Forbidden as e:
        # Catch any errors relating to permission and rights that might occur and print the error message
        print(e)
        sys.exit()

    except Exception as e:
        # Catch any other errors that might occur and print the error message
        print(e)
        sys.exit()
else:
    # Print an error message if the request was not successful
    print("Error: API request failed with status code {}".format(response.status_code))
    sys.exit()
