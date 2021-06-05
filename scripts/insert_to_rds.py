import io

import requests
import psycopg2

from data_platform.definitions import db_name, db_password, db_username
from definitions import table_names_list, sql_list, url_list

# CONNECTION PARAMS
dsn = (
    "dbname={dbname} "
    "user={user} "
    "password={password} "
    "port={port} "
    "host={host} ".format(
        dbname=db_name,
        user=db_username,
        password=db_password,
        port=5432,
        host="rds-production-ecommerce-db.c8xsxbqsw6zh.us-east-1.rds.amazonaws.com",
    )
)

# CREATE SCHEMA, TABLES and load data
table_info = zip(sql_list, table_names_list, url_list)

conn = psycopg2.connect(dsn)
print("connected")
conn.set_session(autocommit=True)
try:
    cur = conn.cursor()
    # Create Schema
    print("Creating Schema\n")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db_name}")
    # Create tables
    for create_table, table_name, url in table_info:
        print(f"Creating {table_name}")
        cur.execute(create_table)
        print(f"Getting data from {url}")
        r = requests.get(url)
        assert r.status_code == 200
        buff = io.StringIO(r.text)
        # Exclude Header
        next(buff)
        # Insert data
        print("Inserting data")
        # cur.copy_from(buff, table_name, sep=",")
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV)", buff)
        print(f"Finished {db_name}\n")
finally:
    conn.close()

