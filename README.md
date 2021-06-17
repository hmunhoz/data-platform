# Project: Data Platform on AWS

**Project Description**: An e-commerce company wants to leverage its data in their business decisions. In order to achieve this, the company wants to build a cloud-based data platform that would enable data analysts and other employees to make data-based decisions more easily.  
As their data engineer, I was tasked with building an ELT (Extract, Load and Transform) pipeline that extracts the company's data from an PostgreSQL database. The data will be saved into a data lake, partitioned and compressed, then modeled into curated tables and served by a cloud-based data warehouse.  
This will create a single source of truth which will make data usage and reporting much more reliable across the company, while also relieving the load on the production database.

### Data Platform Architecture

![data-platform-diagram](images/data-platform-silver.jpg)

The focus of our project is implementing the above infrastructure in a testable and replicable way. In order to achieve this, we make use of Infrastructure as Code (IaC), by declaring the components using `AWS CloudFormation/CDK` and `dbt`.  
The data flows as follows:  
1. A `Database Migration Service` replication task is created to load the data from the PostgreSQL database into an `S3 datalake bronze/raw` bucket.
2. `PySpark` jobs will run on an `EMR` cluster, managed with `Airflow`. These jobs will partition and compress the _raw_ data into the `S3 datalake silver/staged` layer.
3. We create a `Glue Data Catalog` that stores metadata about the data contained in the _bronze_ and _silver S3 buckets_. The metadata is kept up-to-date by `Glue Crawlers` that run on a schedule.
4. `Amazon Athena` is configured in order to be used by more experienced data engineers, analysts and scientists that might want to access _raw_ data.
5. The `data warehouse` cluster is created in `Amazon Redshift`, and will hold the curated tables and views for general consumption.
6. `dbt` is used to transform, standardize and model the company's data with SQL, creating a documented single source of truth.
7. We can then plug a BI tool to our data warehouse and start exploring and sharing visualizations and reports.  

## Data

The dataset is available in this [Kaggle](https://www.kaggle.com/olistbr/brazilian-ecommerce?select=olist_orders_dataset.csv) repository.

![olist_dataset](images/olist_data.png)

It is composed by 8 _.csv_ files, that represent a sample snapshot of the production databases, that run in a _microservices_ architecture.
The tables _olist_orders_dataset_ and _olist_order_items_dataset_ compose what would be, to some extent, equivalent to a _fact table_ in a Star Schema, while the other tables serve as _dimension tables_.

## Methodology


## Project structure

```
├── Makefile           <- Makefile with commands like `make deploy-stack` or `make destroy-stack`
├── README.md          <- The top-level README for developers using this project.
├── data_platform
│   ├── airflow_mwaa    <- airflow IaC resources.
│   ├── athena          <- athena IaC resources.
│   ├── data_lake       <- data lake (s3 buckets) IaC resources.
│   ├── dms             <- Data Migration Services IaC resources.
│   ├── emr             <- Elastic Map Reduce IaC resources.
│   ├── glue_catalog    <- Glue Crawlers IaC resources.
│   ├── rds             <- RDS IaC resources.
│   ├── redshift        <- Redshift data warehouse cluster IaC resources.
│   ├── common_stack.py <- Network Resources and Default Roles IaC resources.
│   └── definitions.py  <- Default RDS settings.
│
├── dbt_project         <- directory with dbt files and resources (check models/)
│
├── images              <- README.md resources
│
├── metabase            <- metabase docker instructions
│
├── app.py              <- AWS cdk app. Defines CloudFormation resources and dependencies.
│
└── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
                          generated with `pip freeze > requirements.txt`
```

## How to run this project 

### Disclaimer

**IMPORTANT!**
- **RUNNING THIS PROJECT ON AWS MAY RESULT IN AWS CHARGES**. Even though I tried to keep the project within [AWS Free Tier](https://aws.amazon.com/free/) services, I do not guarantee it won't incur in charges for your account.  
- **PLEASE MAKE SURE YOU DELETE THE CREATED AWS RESOURCES AFTER USAGE**
- **I AM NOT TO BE HELD RESPONSIBLE FOR INCURRED COSTS IN AWS BILLING IF YOU DECIDE TO RUN THE PROJECT IN YOUR OWN ENVIRONMENT**  

This project is set up like a standard Python project. After cloning this repo, start by creating a virtual environment.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

In order to create the virtualenv, it assumes that there is a `python3`executable in your path with access to the `venv`
package


After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

Make sure you also have [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html) and [AWS CDK](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html) installed, and a valid [AWS account configured](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

The first thing we have to do is create the DMS role. Unfortunately, we have to do it manually.
To do so, run the shell script `config.sh`, located at `./data_platform/dms/`. It will create the necessary roles with
AWS cli.

Now, you can issue the following commands in order to deploy the AWS Stack/Infrastructure.

```
$ make deploy-core
```

This will deploy the infrastructure resources in AWS, except Airflow/EMR.  
_NOTE : This can take a while, as RDS usually requires 5-15 minutes._

Now, we need to populate our platform with data. For this, run the python script

```
./data_platform/insert_to_rds.py
```

Now that we have inserted data to RDS, we need to start our DMS replication task.
You can do it by starting it manually on the AWS DMS Replication Task console, or by running the script shell script 

```
./data_platform/trigger_dms.py
```

Before proceeding to next steps, please make sure that the replication task has properly started,
by visiting DMS services on the AWS console.

Now that DMS has automatically copied our data from Postgres to our bronze data lake s3 bucket, we can
deploy Airflow service (with our Spark job).

We need to have our Spark code in an S3 bucket. Start by running the script that uploads the necessary files to the
scripts bucket: 
```
./scripts/upload_to_s3.py
```

Deploy the airflow stack that runs the spark jobs. We are currently scheduling our script to run only once.  
**NOTE: Managed Apache Airflow (MWAA) is not free**
_This process can take a few minutes_

```
$ make deploy-airflow
```

Our Spark job will partition and load our data to the s3 silver layer.  

Now it is time to set up our Redshift data warehouse.  

Navigate to IAM Console and select the `Roles` option.  
Search for the `production-redshift-stack` role and select it.
Copy the `role arn`.

![data-platform-diagram](images/iam_panel.png)

![data-platform-diagram](images/iam_role_arn.png)

Open Redshift Query Editor panel, and run the following query:

```sqlite-psql
CREATE EXTERNAL SCHEMA data_lake_silver
FROM DATA CATALOG
DATABASE 'glue_ecommerce_production_data_lake_silver'
REGION <your_region_name_here : eg 'us-east-1'>
IAM_ROLE <role_arn example 'arn:aws:iam::0111848002:role/production-redshift-stack-iamproductionredshiftspe-19AHN3Q0'>
```

Now that our data warehouse is in place, we can define our data model with dbt. You can follow the steps described in
the dbt_project/README.md file.

After running our dbt models, you can connect to our data warehouse using the BI tool of your choice.  
Use the same credentials available on AWS Secrets Manager, and start exploring our curated data.  
You can use Metabase as the BI tool by following the instructions in the metabase/README.md file. 

## Improvements

- Automatic deploy inside docker container
- Use PySpark Streaming + Delta Lake/Apache Hudi: This would make ingestion to data lake silver layer more robust.
- Ingest events with Kafka or Kinesis.
- Add ci/cd and tests
