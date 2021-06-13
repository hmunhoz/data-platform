with source as (

    select * from {{ source('data_lake_silver', 'order_items') }}

)
select
    order_id::varchar,
    order_item_qty::int as item_quantity,
    product_id::varchar,
    seller_id::varchar,
    shipping_limit_date::date,
    price::decimal as price,
    freight_value::decimal as freight_value,
    order_item_id::varchar
from source