MERGE destination_project.destintation_dataset.lines B
USING destination_project.temporary_dataset.lines N
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
)
