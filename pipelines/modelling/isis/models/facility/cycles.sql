with

staging as (

    select
      *
    from
      {{ ref('stg_statusdisplay__schedule') }}

)
select
    l.type,
    l.label,
    l.started_at,
    l.ended_at,
    (
        select {{ dbt.any_value("r.label") }} as first_label
        from
          staging r
        where
            r.type = {{  dbt.string_literal("cycle") }}
            and l.group_number = r.group_number
    ) as cycle_name
from
    staging l
