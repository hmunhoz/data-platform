with orders as (

    select * from {{ ref('stg__orders') }}

),

order_items as (

    select * from {{ ref('stg__order_items') }}

),

order_reviews as (

    select * from {{ ref('stg__order_reviews') }}

),

order_payments as (

    select * from {{ ref('stg__order_payments') }}

),

products as (

    select * from {{ ref('stg__products') }}

)

select
    orders.order_purchase_date,
    orders.order_id,
    orders.order_status,
    orders.customer_id,
    products.product_id,
    products.product_category_name,
    order_items.order_item_qty,
    order_items.seller_id,
    order_items.price,
    order_items.freight_value,
    order_items.gmv
from order_items
join orders on order_items.order_id = orders.order_id
join products on products.product_id = order_item.product_id
join order_payments on order_payments.order_id = order_items.order_id
left join order_reviews on order_items.order_id = order_reviews.order_id

--select
--    EXTRACT(YEAR FROM orders.order_approved_date) as order_year,
--    EXTRACT(MONTH FROM orders.order_approved_date) as order_month,
--    orders.order_approved_date,
--    orders.order_id,
--    orders.order_status,
--    order_items.seller_id,
--    order_items.product_id,
--    order_items.item_quantity,
--    order_payments.payment_type,
--    order_payments.payment_installments,
--    order_items.price,
--    order_items.freight_value,
--    order_items.price + order_items.freight_value as gmv,
--    order_items.freight_value * 1.0 / order_items.price as freight_ratio
--from order_items
--join orders on order_items.order_id = orders.order_id
--join order_payments on order_items.order_id = order_payments.order_id