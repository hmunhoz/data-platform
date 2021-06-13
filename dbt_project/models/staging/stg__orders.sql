with source as (

    select * from {{ source('data_lake_silver', 'orders') }}

)

select
    order_id::varchar,
    customer_id::varchar,
    order_status::varchar,
    order_purchase_timestamp::timestamp,
    order_approved_at::date as order_approved_date,
    order_delivered_carrier_date::date,
    order_delivered_customer_date::date,
    order_estimated_delivery_date::date,
    order_purchase_date::date
from source