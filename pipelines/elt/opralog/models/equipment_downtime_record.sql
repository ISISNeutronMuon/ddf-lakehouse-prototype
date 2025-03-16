WITH logbook_denorm AS (
  SELECT
    CAST(entries.`entry_id` AS LONG) AS entry_id,
    CAST(logbooks.`logbook_id` AS LONG) AS logbook_id,
    CAST(TRIM(`logbook_name`) AS STRING) AS logbook_name,
    `time_logged` AS time_logged,
    CAST(TRIM(`entry_description`) AS STRING) AS description,
    CAST(TRIM(`col_title`) AS STRING) AS column_title,
    CAST(TRIM(`col_data`) AS STRING) AS string_data,
    CAST(`number_value` AS DOUBLE) AS number_data,
    CAST(TRIM(`shadow_comment`) AS STRING) AS comment_text
  FROM
    entries
    JOIN logbook_entries ON logbook_entries.entry_id = entries.entry_id
    JOIN logbook_chapter ON logbook_chapter.logbook_no = logbook_entries.logbook_no
    AND logbook_chapter.logbook_id = logbook_entries.logbook_id
    JOIN logbooks ON logbooks.logbook_id = logbook_entries.logbook_id
    LEFT OUTER JOIN more_entry_columns ON more_entry_columns.entry_id = entries.entry_id
    LEFT OUTER JOIN additional_columns ON additional_columns.column_no = more_entry_columns.column_no
    AND additional_columns.entry_type_id = more_entry_columns.entry_type_id
  WHERE
    logbook_name = 'MCR Running Log'
    AND (
      col_data IS NOT NULL
      OR number_value IS NOT NULL
    )
    AND logbook_entries.logbook_id = principal_logbook
),
losttime AS (
  SELECT
    *
  FROM
    logbook_denorm
  WHERE
    `column_title` = 'Lost Time'
),
groups AS (
  SELECT
    `entry_id`,
    `string_data`
  FROM
    logbook_denorm
  WHERE
    `column_title` = 'Group'
),
equipment AS (
  SELECT
    `entry_id`,
    `string_data`
  FROM
    logbook_denorm
  WHERE
    `column_title` = 'Equipment'
)
SELECT
  l.`entry_id`,
  l.`time_logged`,
  r.`cycle_name` AS cycle,
  r.`label` AS cycle_interval,
  e.`string_data` AS equipment,
  l.`number_data` AS downtime_mins,
  g.`string_data` AS group,
  l.`comment_text`
FROM
  losttime l
  INNER JOIN groups g
  INNER JOIN equipment e
  INNER JOIN facility.running_schedule r ON l.`entry_id` = g.`entry_id`
  AND g.`entry_id` = e.`entry_id`
  AND l.`time_logged` BETWEEN r.`start`
  AND r.`end`
