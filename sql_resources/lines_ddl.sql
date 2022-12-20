CREATE TABLE IF NOT EXISTS destination_project.destintation_dataset.lines (
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
