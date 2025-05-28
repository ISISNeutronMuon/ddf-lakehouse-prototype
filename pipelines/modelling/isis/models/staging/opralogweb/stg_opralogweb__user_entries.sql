with

source as (
  select * from {{ source('src_opralogweb', 'user_entries') }}
)

select
    entry_id,
    entry_timestamp as fault_occurred_at,
    cast({{ identifier("entry_timestamp") }} as date) as fault_date,
    trim(additional_comment) as fault_description
from
  source
