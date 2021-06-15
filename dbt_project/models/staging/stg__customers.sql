with source as (

    select * from {{ source('data_lake_silver', 'customers') }}

)
select
    customer_id::varchar,
    customer_unique_id::varchar,
    customer_zip_code_prefix::varchar,
    customer_city::varchar,
    customer_state::varchar
from source