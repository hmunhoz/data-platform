with source as (

    select * from {{ source('data_lake_silver', 'sellers') }}

)
select
    seller_id::varchar,
    seller_zip_code_prefix::varchar,
    seller_city::varchar,
    seller_state::varchar
from source