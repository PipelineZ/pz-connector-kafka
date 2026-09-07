INSERT INTO {{ sink('events', 'orders_out') }}
select
    "offset" as id,
    json_extract_string(value, '$.name') as name,
    "timestamp" as seen_at
from {{ source('events', 'orders_in') }}
order by "partition", "offset"
