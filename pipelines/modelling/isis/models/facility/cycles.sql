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


with schedule_grouped_by_date_block as (
    select
        `type`,
        `label`,
        `start`,
        `end`,
        count(group_number) over (
            order by
                `start`
        ) as group_number
    from
        (
            select
                `type`,
                `label`,
                `start`,
                `end`,
                case
                lag(`end`)
                    over (
                        order by
                            `start`
                    )
                    when `start` then null
                    else `row_number`
                end as group_number
            from
                (
                    select
                        `type`,
                        `label`,
                        `start`,
                        `end`,
                        row_number() over (
                            order by
                                `start` asc
                        ) as `row_number`
                    from
                        {{ source('sources__statusdisplay_api', 'schedule') }}
                )
        )
),

renamed as (
    select
        t.`type`,
        t.`label`,
        t.`start` as started_at,
        t.`end` as ended_at,
        (
            select first(g.label) as first_label
            from
                schedule_grouped_by_date_block as g
            where
                g.`type` = 'cycle'
                and t.group_number = g.group_number
        ) as cycle_name
    from
        schedule_grouped_by_date_block as t
)

select * from renamed
