CREATE TABLE IF NOT EXISTS equipment_downtime_record_proposed (
  equipment STRING,
  fault_date DATE,
  cycle_name STRING,
  cycle_interval STRING,
  downtime_mins DOUBLE,
  fault_time TIMESTAMP,
  group STRING,
  fault_description STRING,
  managers_comments STRING
) USING iceberg PARTITIONED BY (MONTH(fault_time), equipment)
