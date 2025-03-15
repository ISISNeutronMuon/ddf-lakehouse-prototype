CREATE TABLE IF NOT EXISTS running_schedule (
  `type` STRING,
  `label` STRING,
  `start` TIMESTAMP,
  `end` TIMESTAMP
) USING iceberg
