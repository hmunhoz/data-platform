with source as (

    select * from {{ source('data_lake_silver', 'order_payments') }}

)
select
    order_id::varchar,
    lower(payment_type)::varchar as payment_type,
    payment_installments::int
from source