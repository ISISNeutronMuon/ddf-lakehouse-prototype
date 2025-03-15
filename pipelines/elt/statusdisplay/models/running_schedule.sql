WITH _schedule_grouped_by_date_block AS (
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
            schedule
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
      _schedule_grouped_by_date_block g
    WHERE
      g.`type` = 'cycle'
      AND t.group_number = g.group_number
  ) AS cycle_name
FROM
  _schedule_grouped_by_date_block t
