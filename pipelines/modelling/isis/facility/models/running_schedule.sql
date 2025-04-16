{{ config(
    pre_hook=[
      "{{ create_temp_view_from_parquet('source_statusdisplay_schedule', 's3://prod-lakehouse-source-statusdisplay/statusdisplay_api/schedule') }}"
    ]
) }}

WITH schedule_grouped_by_date_block AS (
  SELECT
    `type`,
    `label`,
    `start`,
    `end`,
    COUNT(group_number) OVER (
      ORDER BY
        START
    ) AS group_number
  FROM
    (
      SELECT
        `type`,
        `label`,
        `start`,
        `end`,
        CASE
          lag(`end`) over (
            ORDER BY
              START
          )
          WHEN `start` THEN NULL
          ELSE `row_number`
        END AS group_number
      FROM
        (
          SELECT
            row_number() OVER (
              ORDER BY
                START ASC
            ) AS `row_number`,
            `type`,
            `label`,
            `start`,
            `end`
          FROM
            source_statusdisplay_schedule
        )
    )
)
SELECT
  t.`type`,
  t.`label`,
  t.`start`,
  t.`end`,
  (
    SELECT
      FIRST(g.label)
    FROM
      schedule_grouped_by_date_block g
    WHERE
      g.`type` = 'cycle'
      AND t.group_number = g.group_number
  ) AS cycle_name
FROM
  schedule_grouped_by_date_block t
