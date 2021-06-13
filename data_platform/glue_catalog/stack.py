from aws_cdk import core

from data_platform.active_environment import active_environment
from data_platform.data_lake.base import BaseDataLakeBucket
from data_platform.glue_catalog.base import (
    BaseDataLakeGlueDatabase,
    BaseDataLakeGlueRole,
    BaseGlueCrawler,
)


class GlueCatalogStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        data_lake_bronze_bucket: BaseDataLakeBucket,
        data_lake_silver_bucket: BaseDataLakeBucket,
        data_lake_gold_bucket: BaseDataLakeBucket,
        **kwargs,
    ) -> None:
        self.data_lake_bronze_bucket = data_lake_bronze_bucket
        self.data_lake_silver_bucket = data_lake_silver_bucket
        self.data_lake_gold_bucket = data_lake_gold_bucket
        self.deploy_env = active_environment
        super().__init__(
            scope, id=f"{self.deploy_env.value}-glue-catalog-stack", **kwargs
        )

        self.bronze_database = BaseDataLakeGlueDatabase(
            self, data_lake_bucket=self.data_lake_bronze_bucket
        )

        self.silver_database = BaseDataLakeGlueDatabase(
            self, data_lake_bucket=self.data_lake_silver_bucket
        )

        self.gold_database = BaseDataLakeGlueDatabase(
            self, data_lake_bucket=self.data_lake_gold_bucket
        )

        self.role = BaseDataLakeGlueRole(
            self,
            data_lake_bronze_bucket=self.data_lake_bronze_bucket,
            data_lake_silver_bucket=self.data_lake_silver_bucket,
            data_lake_gold_bucket=self.data_lake_gold_bucket,
        )

        self.ecommerce_bronze_crawler = BaseGlueCrawler(
            self,
            glue_database=self.bronze_database,
            glue_role=self.role,
            table_name="ecommerce_rds",
            schedule_expression="cron(0/15 * * * ? *)",
        )

        self.ecommerce_bronze_crawler.node.add_dependency(self.bronze_database)
        self.ecommerce_bronze_crawler.node.add_dependency(self.role)

        self.ecommerce_silver_crawler = BaseGlueCrawler(
            self,
            glue_database=self.silver_database,
            glue_role=self.role,
            table_name="ecommerce_rds",
            schedule_expression="cron(0/15 * * * ? *)",
        )

        self.ecommerce_silver_crawler.node.add_dependency(self.silver_database)
        self.ecommerce_silver_crawler.node.add_dependency(self.role)

        self.ecommerce_gold_crawler = BaseGlueCrawler(
            self,
            glue_database=self.gold_database,
            glue_role=self.role,
            table_name="ecommerce_rds",
            schedule_expression="cron(0/15 * * * ? *)",
        )

        self.ecommerce_gold_crawler.node.add_dependency(self.gold_database)
        self.ecommerce_gold_crawler.node.add_dependency(self.role)
