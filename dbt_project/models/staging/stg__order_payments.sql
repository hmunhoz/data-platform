with source as (

    select * from {{ source('data_lake_silver', 'order_payments') }}

)
select
    order_id::varchar,
    payment_type::varchar,
    payment_installments::int
from source