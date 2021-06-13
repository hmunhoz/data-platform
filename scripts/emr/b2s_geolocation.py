#!/usr/bin/env python
# coding: utf-8

import re
import boto3
import uuid
import logging

from unidecode import unidecode

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lpad, lit, concat, regexp_replace
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(
    format="[%(asctime)s] %(levelname)s – %(message)s", level=logging.INFO
)


def create_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.2"
    ).getOrCreate()
    spark._jsc.hadoopConfiguration().set(
        "fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    return spark


def get_table_location(glue_database: dict, table_name: str):
    """Return s3 location"""
    s3_location = glue.get_table(DatabaseName=glue_database["Name"], Name=table_name)

    return s3_location["Table"]["StorageDescriptor"]["Location"]


def get_output_location(glue_database: dict, table_name: str):
    return glue_database["LocationUri"] + "/ecommerce_rds/ecommerce/" + table_name


def first_n_digits(df, n, column_name):
    df = df.withColumn(f"{column_name}_{n}", df[column_name].substr(1, n))
    return df


def fix_city_string_latin(city_string: str):
    s = unidecode(city_string.encode("latin1").decode("utf8")).title()
    return re.sub("[^\w\s]", " ", s)


def fix_state_name(state_name: str):
    s = unidecode(state_name.encode("latin1").decode("utf8")).upper()
    return re.sub("[^\w\s]", " ", s)


def process_geolocation():
    # read data
    s3_geolocation = get_table_location(bronze_db, "geolocation")
    geolocation_df = spark.read.parquet(s3_geolocation)

    # Remove locations outside Brazil area (use lat lon)
    west_lim = -73.99222222222222
    east_lim = -34.793055555555554
    south_lim = -33.75083333333333
    north_lim = 5.272222222222222

    geolocation_df = geolocation_df.filter(
        (geolocation_df["geolocation_lng"] >= west_lim)
        & (geolocation_df["geolocation_lng"] <= east_lim)
    ).filter(
        (geolocation_df["geolocation_lat"] >= south_lim)
        & (geolocation_df["geolocation_lat"] <= north_lim)
    )

    # Fix prefix in 5 digits (trailing 0)
    geolocation_df = geolocation_df.withColumn(
        "geolocation_zip_code_prefix",
        lpad(geolocation_df["geolocation_zip_code_prefix"], 5, "0"),
    )

    # create columns with expanded prefix (1 digit, 2 digits, 3 digits, 4 digits)
    for i in range(1, 5):
        geolocation_df = first_n_digits(
            geolocation_df, i, "geolocation_zip_code_prefix"
        )

    # fix city names
    geolocation_df = geolocation_df.withColumn(
        "geolocation_city", fix_city_latin_udf("geolocation_city")
    )

    # fix state name (add validation for state names)
    geolocation_df = geolocation_df.withColumn(
        "geolocation_state", fix_state_udf("geolocation_state")
    )

    # extracted at to timestamp
    geolocation_df = geolocation_df.withColumn(
        "extracted_at", F.to_timestamp("extracted_at", "yyyy-MM-dd HH:mm:ss.SSSSSS")
    )

    # Fix Embu Das Artes
    geolocation_df = geolocation_df.withColumn(
        "geolocation_city",
        regexp_replace("geolocation_city", "^Embu$", "Embu Das Artes"),
    )
    # Fix Sao Paulo
    geolocation_df = geolocation_df.withColumn(
        "geolocation_city", regexp_replace("geolocation_city", "Sp", "Sao Paulo")
    )
    # save file
    output_location = get_output_location(silver_db, "geolocation")
    geolocation_df.write.parquet(output_location, mode="Overwrite")


def process_sellers():
    s3_sellers = get_table_location(bronze_db, "sellers")
    sellers_df = spark.read.parquet(s3_sellers)
    # extracted at to timestamp
    sellers_df = sellers_df.withColumn(
        "extracted_at", F.to_timestamp("extracted_at", "yyyy-MM-dd HH:mm:ss.SSSSSS")
    )
    # correct zip code, city, state
    sellers_df = sellers_df.withColumn(
        "seller_zip_code_prefix", lpad(sellers_df["seller_zip_code_prefix"], 5, "0")
    )
    sellers_df = sellers_df.withColumn("seller_city", fix_city_latin_udf("seller_city"))
    # fix state name (add validation for state names?)
    sellers_df = sellers_df.withColumn("seller_state", fix_state_udf("seller_state"))
    # write to silver
    output_location = get_output_location(silver_db, "sellers")
    sellers_df.write.parquet(output_location, mode="Overwrite")


def process_products():
    s3_products = get_table_location(bronze_db, "products")
    products_df = spark.read.parquet(s3_products)
    # extracted at to timestamp
    products_df = products_df.withColumn(
        "extracted_at", F.to_timestamp("extracted_at", "yyyy-MM-dd HH:mm:ss.SSSSSS")
    )
    # write to silver
    output_location = get_output_location(silver_db, "products")
    products_df.write.parquet(output_location, mode="Overwrite")


def process_orders():
    s3_orders = get_table_location(bronze_db, "orders")
    orders_df = spark.read.parquet(s3_orders)
    # dates
    orders_df = orders_df.withColumn(
        "order_purchase_date", F.to_date("order_purchase_timestamp")
    )
    orders_df = orders_df.withColumn(
        "order_delivered_carrier_date", F.to_date("order_delivered_carrier_date")
    )
    orders_df = orders_df.withColumn(
        "order_delivered_customer_date", F.to_date("order_delivered_customer_date")
    )
    orders_df = orders_df.withColumn(
        "order_estimated_delivery_date", F.to_date("order_estimated_delivery_date")
    )
    orders_df = orders_df.withColumn(
        "order_approved_at", F.to_date("order_approved_at")
    )

    # timestamps
    orders_df = orders_df.withColumn(
        "extracted_at", F.to_timestamp("extracted_at", "yyyy-MM-dd HH:mm:ss.SSSSSS")
    )
    orders_df = orders_df.withColumn(
        "order_purchase_timestamp",
        F.to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss.SSSSSS"),
    )
    # partition
    orders_df = (
        orders_df.withColumn("Year", year("order_approved_at"))
        .withColumn("Month", month("order_approved_at"))
        .withColumn("Day", dayofmonth("order_approved_at"))
    )
    orders_df = orders_df.repartition("Year", "Month", "Day")
    # write to silver
    output_location = get_output_location(silver_db, "orders")
    orders_df.write.partitionBy("Year", "Month", "Day").mode("overwrite").parquet(
        output_location
    )


def process_order_items():
    s3_order_items = get_table_location(bronze_db, "order_items")
    order_items_df = spark.read.parquet(s3_order_items)
    # process columns
    order_items_df = order_items_df.withColumn("order_item_id", uuid_udf())

    # write to silver
    output_location = get_output_location(silver_db, "order_items")
    order_items_df.write.parquet(output_location, mode="Overwrite")


def process_order_payments():
    s3_order_payments = get_table_location(bronze_db, "order_payments")
    order_payments_df = spark.read.parquet(s3_order_payments)
    order_payments_df = order_payments_df.withColumn(
        "extracted_at", F.to_timestamp("extracted_at", "yyyy-MM-dd HH:mm:ss.SSSSSS")
    )
    # write to silver
    output_location = get_output_location(silver_db, "order_payments")
    order_payments_df.write.parquet(output_location, mode="Overwrite")


def process_customers():
    s3_customers = get_table_location(bronze_db, "customers")
    customers_df = spark.read.parquet(s3_customers)
    customers_df = customers_df.withColumn(
        "extracted_at", F.to_timestamp("extracted_at", "yyyy-MM-dd HH:mm:ss.SSSSSS")
    )
    # Fix prefix in 5 digits (trailing 0)
    customers_df = customers_df.withColumn(
        "customer_zip_code_prefix",
        lpad(customers_df["customer_zip_code_prefix"], 5, "0"),
    )

    customers_df = customers_df.withColumn(
        "customer_city", fix_city_latin_udf(customers_df["customer_city"])
    )
    customers_df = customers_df.withColumn(
        "customer_state", fix_state_udf(customers_df["customer_state"])
    )

    # write to silver
    output_location = get_output_location(silver_db, "customers")
    customers_df.write.parquet(output_location, mode="Overwrite")


def process_order_reviews():
    s3_order_reviews = get_table_location(bronze_db, "order_reviews")
    order_reviews_df = spark.read.parquet(s3_order_reviews)

    order_reviews_df = order_reviews_df.withColumn(
        "extracted_at", F.to_timestamp("extracted_at", "yyyy-MM-dd HH:mm:ss.SSSSSS")
    )
    # partitions
    order_reviews_df = (
        order_reviews_df.withColumn("Year", year("review_creation_date"))
        .withColumn("Month", month("review_creation_date"))
        .withColumn("Day", dayofmonth("review_creation_date"))
    )
    order_reviews_df = order_reviews_df.repartition("Year", "Month", "Day")
    # write to silver
    output_location = get_output_location(silver_db, "order_reviews")
    order_reviews_df.write.partitionBy("Year", "Month", "Day").mode(
        "overwrite"
    ).parquet(output_location)


if __name__ == "__main__":
    # create spark session
    spark = create_spark_session()
    # get databases in glue
    glue = boto3.client("glue", region_name="us-east-1")
    databaseList = glue.get_databases()["DatabaseList"]

    bronze_db = list(
        filter(
            lambda db_list: db_list["Name"]
            == "glue_ecommerce_production_data_lake_bronze",
            databaseList,
        )
    )[0]

    silver_db = list(
        filter(
            lambda db_list: db_list["Name"]
            == "glue_ecommerce_production_data_lake_silver",
            databaseList,
        )
    )[0]

    # User Defined Functions:
    uuid_udf = F.udf(lambda: str(uuid.uuid4()), F.StringType())
    fix_city_latin_udf = F.udf(fix_city_string_latin, T.StringType())
    fix_state_udf = F.udf(fix_state_name, T.StringType())

    # process datasets
    process_geolocation()
    process_sellers()
    process_products()
    process_orders()
    process_order_items()
    process_order_payments()
    process_customers()
    process_order_reviews()
