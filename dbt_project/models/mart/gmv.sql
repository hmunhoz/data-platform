with orders as (

    select * from {{ ref('stg__orders') }}

),

order_items as (

    select * from {{ ref('stg__order_items') }}

),

order_payments as (

    select * from {{ ref('stg__order_payments') }}

)
select
    EXTRACT(YEAR FROM orders.order_approved_date) as order_year,
    EXTRACT(MONTH FROM orders.order_approved_date) as order_month,
    orders.order_approved_date,
    orders.order_id,
    orders.order_status,
    order_items.seller_id,
    order_items.product_id,
    order_items.item_quantity,
    order_payments.payment_type,
    order_payments.payment_installments,
    order_items.price,
    order_items.freight_value,
    order_items.price + order_items.freight_value as gmv,
    order_items.freight_value * 1.0 / order_items.price as freight_ratio
from order_items
join orders on order_items.order_id = orders.order_id
join order_payments on order_items.order_id = order_payments.order_id