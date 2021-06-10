#!/usr/bin/env python
# coding: utf-8

import os
import boto3
import logging
import configparser

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lpad
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(format='[%(asctime)s] %(levelname)s – %(message)s', level=logging.INFO)

# config = configparser.ConfigParser()
# config.read(os.path.expanduser("~/.aws/credentials"))
#
# aws_profile = 'default' # your AWS profile to use
#
# os.environ["AWS_ACCESS_KEY_ID"]= config.get(aws_profile, "aws_access_key_id")
# os.environ["AWS_SECRET_ACCESS_KEY"]= config.get(aws_profile, "aws_secret_access_key")

def create_spark_session():
    spark = SparkSession\
        .builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.2")\
        .getOrCreate()
    
    # Only needed if you use s3://
    spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    return spark


spark = create_spark_session()

glue = boto3.client('glue', region_name="us-east-1")
responseGetDatabases = glue.get_databases()
databaseList = responseGetDatabases['DatabaseList']

# Getting bronze and silver dbs
bronze_db = list(filter(lambda dbList: dbList['Name'] == 'glue_ecommerce_production_data_lake_bronze', databaseList))[0]
silver_db = list(filter(lambda dbList: dbList['Name'] == 'glue_ecommerce_production_data_lake_silver', databaseList))[0]

# Geolocation dataset
# Geolocation bronze table
bronze_geolocation = glue.get_table(
    DatabaseName=bronze_db['Name'],
    Name='geolocation'
)
bronze_geolocation_parquet = f"{bronze_geolocation['Table']['StorageDescriptor']['Location']}*.parquet"
# read bronze_geolocation data
df = spark.read.parquet(bronze_geolocation_parquet)


def first_n_digits(df, n, column_name):
    df = df.withColumn(f"{column_name}_{n}", df[column_name].substr(1, n))
    return df


west_lim = -73.99222222222222
east_lim = -34.793055555555554
south_lim = -33.75083333333333
north_lim = 5.272222222222222

filtered_df = df\
    .filter((df.geolocation_lng >= west_lim) & (df.geolocation_lng <= east_lim))\
    .filter((df.geolocation_lat >= south_lim) & (df.geolocation_lng <= north_lim))

# Fix prefix in 5 digits (trailing 0)
df_prefix = filtered_df.withColumn('geolocation_zip_code_prefix', lpad(filtered_df.geolocation_zip_code_prefix, 5, '0'))

# expand prefix (1 digit, 2 digits, 3 digits, 4 digits)
for i in range(1, 5):
    df_prefix = first_n_digits(df_prefix, i, 'geolocation_zip_code_prefix')

output_file = silver_db['LocationUri'] + '/ecommerce_rds/ecommerce/geolocation'
# write to silver
df_prefix.write.parquet(output_file, mode='Overwrite')
