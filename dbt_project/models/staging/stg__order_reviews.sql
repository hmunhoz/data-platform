with source as (

    select * from {{ source('data_lake_silver', 'order_reviews') }}

)
select
    review_id::varchar,
    order_id::varchar,
    review_score::int,
    review_comment_title::varchar,
    review_comment_message::varchar,
    review_creation_date::date,
    review_answer_timestamp::timestamp
from source