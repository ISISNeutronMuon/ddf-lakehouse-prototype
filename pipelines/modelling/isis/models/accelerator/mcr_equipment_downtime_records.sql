{{ config(
    pre_hook=[
      "{{ create_temp_view_from_parquet('entries', 's3://prod-lakehouse-source-opralogweb/opralogwebdb/entries/') }}",
      "{{ create_temp_view_from_parquet('chapter_entry', 's3://prod-lakehouse-source-opralogweb/opralogwebdb/chapter_entry/') }}",
      "{{ create_temp_view_from_parquet('logbook_chapter', 's3://prod-lakehouse-source-opralogweb/opralogwebdb/logbook_chapter/') }}",
      "{{ create_temp_view_from_parquet('logbooks', 's3://prod-lakehouse-source-opralogweb/opralogwebdb/logbooks/') }}",
      "{{ create_temp_view_from_parquet('more_entry_columns', 's3://prod-lakehouse-source-opralogweb/opralogwebdb/more_entry_columns/') }}",
      "{{ create_temp_view_from_parquet('additional_columns', 's3://prod-lakehouse-source-opralogweb/opralogwebdb/additional_columns/') }}"
    ],
    post_hook=[
      "ALTER TABLE {{ this }} WRITE ORDERED BY fault_occurred_at ASC NULLS LAST"
    ],
    file_format= "iceberg",
    partition_by=["cycle_name"]

) }}
{% set LOGBOOK_NAME = 'MCR Running Log' %}
{% set OPRALOG_EPOCH = '2017-04-25' %} -- Opralog started being used from cycle 2017/01

WITH logbook_denorm AS (
  SELECT
    entries.entry_id AS entry_id,
    entries.entry_timestamp AS fault_occurred_at,
    TRIM(additional_columns.col_title) AS column_title,
    TRIM(more_entry_columns.col_data) AS string_data,
    more_entry_columns.number_value AS number_data,
    entries.additional_comment AS fault_description
  FROM
    entries
    JOIN chapter_entry ON chapter_entry.entry_id = entries.entry_id
    JOIN logbook_chapter ON logbook_chapter.logbook_chapter_no = chapter_entry.logbook_chapter_no
    JOIN logbooks ON logbooks.logbook_id = chapter_entry.logbook_id
    LEFT OUTER JOIN more_entry_columns ON more_entry_columns.entry_id = entries.entry_id
    LEFT OUTER JOIN additional_columns ON additional_columns.additional_column_id = more_entry_columns.additional_column_id
  WHERE
    logbooks.logbook_name = '{{ LOGBOOK_NAME }}'
    AND chapter_entry.logbook_id = chapter_entry.principal_logbook
    AND TRIM(additional_columns.col_title) IN ('Equipment', 'Group', 'Lost Time', 'Group Leader comments')
    AND (
      more_entry_columns.col_data IS NOT NULL
      OR more_entry_columns.number_value IS NOT NULL
    )
),
-- downtime records without cycle information
downtime_records AS (
  SELECT
    *
  FROM
    (
      SELECT
        `entry_id`,
        TO_DATE(`fault_occurred_at`) AS fault_date,
        `fault_occurred_at`,
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
        `fault_occurred_at`,
        `fault_date`,
        `fault_description`
    )
  WHERE
    `equipment` IS NOT NULL
    AND `downtime_mins` IS NOT NULL
    AND `group` IS NOT NULL
    AND -- Opralog started being used from cycle 2017/01
    `fault_occurred_at` >= '{{ OPRALOG_EPOCH }}'
)
SELECT
  d.`equipment`,
  d.`fault_date`,
  (
    SELECT
      FIRST(r.`cycle_name`)
    FROM
      {{ ref('cycles') }} r
    WHERE
      d.`fault_occurred_at` >= r.`started_at`
      AND d.`fault_occurred_at` <= r.`ended_at`
  ) AS cycle_name,
  (
    SELECT
      FIRST(r.`label`)
    FROM
      {{ ref('cycles') }} r
    WHERE
      d.`fault_occurred_at` >= r.`started_at`
      AND d.`fault_occurred_at` <= r.`ended_at`
  ) AS cycle_interval,
  d.`downtime_mins`,
  d.`fault_occurred_at`,
  d.`group`,
  d.`fault_description`,
  d.`managers_comments`
FROM
  downtime_records d
ORDER BY fault_occurred_at ASC NULLS LAST
