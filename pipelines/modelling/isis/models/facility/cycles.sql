{{ config(
  pre_hook=[
    "{{ create_temp_view_from_parquet('source_statusdisplay_schedule', 's3://prod-lakehouse-source-statusdisplay/statusdisplay_api/schedule') }}"
    ],
  post_hook=[
    "ALTER TABLE {{ this }} WRITE ORDERED BY started_at ASC NULLS LAST"
    ]
) }}

{# The schedule table defines pre- & post-cycle events but does not attribute #}
{# them to a named cycle, e.g. #}

{# type        label            start                  end                  #}
{# ------------------------------------------------------------------------ #}
{# pre-cycle   Commissioning    2022-01-10 08:30:00    2022-02-21 08:30:00  #}
{# pre-cycle   Run-up           2022-02-21 08:30:00    2022-03-01 08:30:00  #}
{# cycle       2021/2           2022-03-01 08:30:00    2022-04-08 08:30:00  #}
{# post-cycle  Machine physics  2022-04-08 08:30:00    2022-04-11 08:30:00  #}

{# We want to label these date ranges as YYYY/NO also. #}
{# This model assumes that a cycle is a contiguous block of dates where #}
{# start(interval) = end(previous_interval) and uses windows functions to #}
{# group each contiguous block and select the label where type='cycle' #}


WITH schedule_grouped_by_date_block AS (
  SELECT
    `type`,
    `label`,
    `start`,
    `end`,
    COUNT(group_number) OVER (
      ORDER BY
        `start`
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
              `start`
          )
          WHEN `start` THEN NULL
          ELSE `row_number`
        END AS group_number
      FROM
        (
          SELECT
            row_number() OVER (
              ORDER BY
                `start` ASC
            ) AS `row_number`,
            `type`,
            `label`,
            `start`,
            `end`
          FROM
            source_statusdisplay_schedule
        )
    )
),
renamed AS (
  SELECT
    t.`type`,
    t.`label`,
    t.`start` AS started_at,
    t.`end` AS ended_at,
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
  ORDER BY started_at ASC NULLS LAST
)

select * from renamed
