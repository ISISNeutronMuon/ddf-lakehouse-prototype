{# The schedule table defines pre- & post-cycle events but does not assign #}
{# an identifier to attribute them to the same "group" #}

{# type        label            start                  end                  #}
{# ------------------------------------------------------------------------ #}
{# pre-cycle   Commissioning    2022-01-10 08:30:00    2022-02-21 08:30:00  #}
{# pre-cycle   Run-up           2022-02-21 08:30:00    2022-03-01 08:30:00  #}
{# cycle       2021/2           2022-03-01 08:30:00    2022-04-08 08:30:00  #}
{# post-cycle  Machine physics  2022-04-08 08:30:00    2022-04-11 08:30:00  #}

{# Here we assume a contiguous block of dates where start(interval) = end(previous_interval) #}
{# and use windows functions to group each contiguous block. #}
{# Each contiguous block is assigned a unique identifier #}

with

source as (

  select * from {{ source('src_statusdisplay', 'schedule') }}

),

renamed as (

  select
    type,
    label,
    start AS started_at,
    {{ identifier("end") }} AS ended_at,
    count(group_number) over (
        order by start
    ) as group_number
  from
    (
        select
            type,
            label,
            start,
            {{ identifier("end") }},
            case
                lag({{ identifier("end") }})
                    over (
                        order by
                            start
                    )
                    when start then null
                    else row_number
            end as group_number
        from
            (
                select
                    type,
                    label,
                    start,
                    {{ identifier("end") }},
                    row_number() over (
                        order by
                            start asc
                    ) as row_number
                from
                    source
            )
    )

)

select * from renamed
