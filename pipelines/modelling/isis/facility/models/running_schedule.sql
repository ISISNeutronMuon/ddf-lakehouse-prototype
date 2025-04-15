SELECT
  *
FROM
  {{ source("raw", "schedule") }}
