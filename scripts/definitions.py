from data_platform.definitions import db_name

# SQL STATEMENTS TO CREATE TABLES
geolocation = f"""
CREATE TABLE IF NOT EXISTS {db_name}.geolocation (
    geolocation_zip_code_prefix   VARCHAR,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city  VARCHAR,
    geolocation_state   VARCHAR
);
"""

product_category_name_translation = f"""
CREATE TABLE IF NOT EXISTS {db_name}.product_category_name_translation (
    product_category_name  VARCHAR,
    product_category_name_english  VARCHAR,
    PRIMARY KEY (product_category_name)
);
"""

products = f"""
CREATE TABLE IF NOT EXISTS ecommerce.products (
    product_id UUID,
    product_category_name  VARCHAR,
    product_name_length FLOAT,
    product_description_length FLOAT,
    product_photos_qty FLOAT,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT,
    PRIMARY KEY (product_id),
    FOREIGN KEY (product_category_name) REFERENCES ecommerce.product_category_name_translation (product_category_name)
);
"""

sellers = f"""
CREATE TABLE IF NOT EXISTS {db_name}.sellers (
    seller_id UUID,
    seller_zip_code_prefix   VARCHAR,
    seller_city  VARCHAR,
    seller_state   VARCHAR,
    PRIMARY KEY (seller_id)
);
"""

customers = f"""
CREATE TABLE IF NOT EXISTS {db_name}.customers (
    customer_id UUID,
    customer_unique_id UUID,
    customer_zip_code_prefix   VARCHAR,
    customer_city  VARCHAR,
    customer_state   VARCHAR,
    PRIMARY KEY (customer_id)
);
"""

orders = f"""
CREATE TABLE IF NOT EXISTS {db_name}.orders (
    order_id UUID,
    customer_id UUID,
    order_status  VARCHAR,
    order_purchase_timestamp DATE,
    order_approved_at DATE,
    order_delivered_carrier_date DATE,
    order_delivered_customer_date DATE,
    order_estimated_delivery_date DATE,
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES {db_name}.customers (customer_id)
);
"""

order_reviews = f"""
CREATE TABLE IF NOT EXISTS {db_name}.order_reviews (
    review_id UUID,
    order_id UUID,
    review_score INT,
    review_comment_title  VARCHAR,
    review_comment_message  VARCHAR,
    review_creation_date DATE,
    review_answer_timestamp DATE,
    PRIMARY KEY (review_id),
    FOREIGN KEY (order_id) REFERENCES {db_name}.orders (order_id)
);
"""

order_items = f"""
CREATE TABLE IF NOT EXISTS ecommerce.order_items (
    order_id UUID,
    order_item_qty INT,
    product_id UUID,
    seller_id UUID,
    shipping_limit_date DATE,
    price FLOAT(2),
    freight_value FLOAT(2),
    FOREIGN KEY (order_id) REFERENCES ecommerce.orders (order_id),
    FOREIGN KEY (product_id) REFERENCES ecommerce.products (product_id),
    FOREIGN KEY (seller_id) REFERENCES ecommerce.sellers (seller_id)
);
"""

order_payments = f"""
CREATE TABLE IF NOT EXISTS {db_name}.order_payments (
    order_id UUID,
    payment_sequential INT,
    payment_type  VARCHAR,
    payment_installments INT,
    payment_value FLOAT(2),
    FOREIGN KEY (order_id) REFERENCES {db_name}.orders (order_id)
);
"""

sql_list = [
    geolocation,
    product_category_name_translation,
    products,
    sellers,
    customers,
    orders,
    order_reviews,
    order_items,
    order_payments,
]

# DATA URLs (Public S3)
product_category_name_url = "https://ecommerce-olist-datasets.s3.amazonaws.com/product_category_name_translation.csv"
customers_url = "https://ecommerce-olist-datasets.s3.amazonaws.com/olist_customers_dataset.csv"
geolocation_url = "https://ecommerce-olist-datasets.s3.amazonaws.com/olist_geolocation_dataset.csv"
order_items_url = "https://ecommerce-olist-datasets.s3.amazonaws.com/olist_order_items_dataset.csv"
order_payments_url = "https://ecommerce-olist-datasets.s3.amazonaws.com/olist_order_payments_dataset.csv"
order_reviews_url = "https://ecommerce-olist-datasets.s3.amazonaws.com/olist_order_reviews_dataset.csv"
orders_url = "https://ecommerce-olist-datasets.s3.amazonaws.com/olist_orders_dataset.csv"
products_url = "https://ecommerce-olist-datasets.s3.amazonaws.com/olist_products_dataset.csv"
sellers_url = "https://ecommerce-olist-datasets.s3.amazonaws.com/olist_sellers_dataset.csv"

url_list = [
    geolocation_url,
    product_category_name_url,
    products_url,
    sellers_url,
    customers_url,
    orders_url,
    order_reviews_url,
    order_items_url,
    order_payments_url,
]

# TABLE NAMES list
table_names_list = [
    f"{db_name}.geolocation",
    f"{db_name}.product_category_name_translation",
    f"{db_name}.products",
    f"{db_name}.sellers",
    f"{db_name}.customers",
    f"{db_name}.orders",
    f"{db_name}.order_reviews",
    f"{db_name}.order_items",
    f"{db_name}.order_payments",
]

