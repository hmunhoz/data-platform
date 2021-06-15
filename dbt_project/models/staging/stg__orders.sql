with source as (

    select * from {{ source('data_lake_silver', 'orders') }}

)
select
    order_id::varchar,
    customer_id::varchar,
    lower(order_status)::varchar as order_status,
    order_purchase_timestamp::timestamp,
    order_approved_at::date as order_approved_date,
    order_delivered_carrier_date::date,
    order_delivered_customer_date::date,
    order_estimated_delivery_date::date,
    order_purchase_date::date,
    extract (year from order_purchase_date)::int as order_purchase_year,
    extract (month from order_purchase_date)::int as order_purchase_month,
    extract (quarter from order_purchase_date)::int as order_purchase_quarter
from source
