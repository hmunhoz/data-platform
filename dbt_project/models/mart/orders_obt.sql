{{
    config(
        materialized='table'
    )
}}

with order_items as (

    select * from {{ ref('stg__order_items') }}

),

products as (

    select * from {{ ref('stg__products') }}

),

summarized_items as (

    select
        order_items.order_id as order_id,
        order_items.seller_id as seller_id,
        products.product_id as product_id,
        products.product_category_name as product_category,
        sum(order_items.item_quantity) as item_quantity,
        sum(order_items.gmv) as gmv
    from order_items
    join products on order_items.product_id = products.product_id
    group by
        order_items.order_id,
        order_items.seller_id,
        products.product_id,
        products.product_category_name

),



orders as (

    select * from {{ ref('stg__orders') }}

),

order_reviews as (

    select * from {{ ref('stg__order_reviews') }}

),

order_payments as (

    select * from {{ ref('stg__order_payments') }}

),

summarized_orders as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_status,
        orders.order_purchase_date,
        orders.order_purchase_year,
        orders.order_purchase_month,
        orders.order_purchase_quarter,
        order_reviews.review_score,
        order_payments.payment_type,
        order_payments.payment_installments
    from orders
    left join order_reviews on orders.order_id = order_reviews.order_id
    left join order_payments on orders.order_id = order_payments.order_id

),

orders_obt as (

    select
        summarized_orders.order_id,
        summarized_orders.customer_id,
        summarized_orders.order_status,
        summarized_orders.order_purchase_date,
        summarized_orders.order_purchase_year,
        summarized_orders.order_purchase_month,
        summarized_orders.order_purchase_quarter,
        summarized_orders.review_score,
        summarized_orders.payment_type,
        summarized_orders.payment_installments,
        summarized_items.seller_id,
        summarized_items.product_id,
        summarized_items.product_category,
        summarized_items.item_quantity,
        summarized_items.gmv
    from summarized_orders
    join summarized_items on summarized_orders.order_id = summarized_items.order_id

)

select * from orders_obt