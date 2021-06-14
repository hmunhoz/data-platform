with source as (

    select * from {{ source('data_lake_silver', 'products') }}

)
select
    product_id::varchar
    lower(product_category_name)::varchar as product_category_name,
    product_name_length::int,
    product_description_length::int,
    product_photos_qty::int,
    product_weight_g::float,
    product_length_cm::float,
    product_height_cm::float,
    product_width_cm::float
from source