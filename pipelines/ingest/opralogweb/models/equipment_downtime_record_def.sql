CREATE TABLE IF NOT EXISTS equipment_downtime_record (
  entry_id LONG,
  time_logged TIMESTAMP,
  cycle_name STRING,
  cycle_interval STRING,
  equipment STRING,
  downtime_mins DOUBLE,
  group STRING,
  comment_text STRING
) USING iceberg PARTITIONED BY (MONTH(time_logged), equipment)
