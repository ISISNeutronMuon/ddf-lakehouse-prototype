WITH logbook_denorm AS (
  SELECT
    CAST(entries.`entry_id` AS LONG) AS entry_id,
    CAST(logbooks.`logbook_id` AS LONG) AS logbook_id,
    CAST(TRIM(`logbook_name`) AS STRING) AS logbook_name,
    `entry_timestamp` AS fault_time,
    CAST(TRIM(`entry_description`) AS STRING) AS description,
    CAST(TRIM(`col_title`) AS STRING) AS column_title,
    CAST(TRIM(`col_data`) AS STRING) AS string_data,
    CAST(`number_value` AS DOUBLE) AS number_data,
    CAST(TRIM(`shadow_comment`) AS STRING) AS fault_description
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
    AND TRIM(`col_title`) IN ('Equipment', 'Group', 'Lost Time')
    AND (
      `col_data` IS NOT NULL
      OR `number_value` IS NOT NULL
    )
    AND logbook_entries.`logbook_id` = `principal_logbook`
),
downtime_records AS (
  SELECT
    *
  FROM
    (
      SELECT
        `entry_id`,
        TO_DATE(`fault_time`) AS fault_date,
        `fault_time`,
        MIN(
          CASE
            `column_title`
            WHEN 'Equipment' THEN `string_data`
          END
        ) AS equipment,
        MIN(
          CASE
            `column_title`
            WHEN 'Lost Time' THEN `number_data`
          END
        ) AS downtime_mins,
        MIN(
          CASE
            `column_title`
            WHEN 'Group' THEN `string_data`
          END
        ) AS group,
        `fault_description`,
        MIN(
          CASE
            `column_title`
            WHEN 'Group Leader comments' THEN `string_data`
          END
        ) AS managers_comments
      FROM
        logbook_denorm
      GROUP BY
        `entry_id`,
        `fault_time`,
        `fault_date`,
        `fault_description`
    )
  WHERE
    `equipment` IS NOT NULL
    AND `downtime_mins` IS NOT NULL
    AND `group` IS NOT NULL
    AND -- Opralog started being used from cycle 2017/01
    `fault_time` >= '2017-04-25' -- Opralog started being used from cycle 2017/01
)
SELECT
  d.`equipment`,
  d.`fault_date`,
  (
    SELECT
      FIRST(r.`cycle_name`)
    FROM
      isis.facility.running_schedule r
    WHERE
      d.`fault_time` >= r.`start`
      AND d.`fault_time` <= r.`end`
  ) AS cycle_name,
  (
    SELECT
      FIRST(r.`label`)
    FROM
      isis.facility.running_schedule r
    WHERE
      d.`fault_time` >= r.`start`
      AND d.`fault_time` <= r.`end`
  ) AS cycle_interval,
  d.`downtime_mins`,
  d.`fault_time`,
  d.`group`,
  d.`fault_description`,
  d.`managers_comments`
FROM
  downtime_records d
