with

source as (
  select * from {{ source('src_opralogweb', 'user_entries') }}
)

select
    entry_id,
    entry_timestamp AS fault_occurred_at,
    to_date(`entry_timestamp`) AS fault_date,
    TRIM(additional_comment) AS fault_description
from
  source
