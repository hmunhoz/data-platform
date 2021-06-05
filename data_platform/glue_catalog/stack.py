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
        raw_data_lake_bucket: BaseDataLakeBucket,
        staged_data_lake_bucket: BaseDataLakeBucket,
        **kwargs,
    ) -> None:
        self.raw_data_lake_bucket = raw_data_lake_bucket
        self.processed_data_lake_bucket = staged_data_lake_bucket
        self.deploy_env = active_environment
        super().__init__(
            scope, id=f"{self.deploy_env.value}-glue-catalog-stack", **kwargs
        )

        self.raw_database = BaseDataLakeGlueDatabase(
            self, data_lake_bucket=self.raw_data_lake_bucket
        )

        self.processed_database = BaseDataLakeGlueDatabase(
            self, data_lake_bucket=self.processed_data_lake_bucket
        )

        self.role = BaseDataLakeGlueRole(self, data_lake_bucket=self.raw_data_lake_bucket)

        self.ecommerce_crawler = BaseGlueCrawler(
            self,
            glue_database=self.raw_database,
            glue_role=self.role,
            table_name="ecommerce_rds",
            schedule_expression="cron(0/15 * * * ? *)",
        )

        self.ecommerce_crawler.node.add_dependency(self.raw_database)
        self.ecommerce_crawler.node.add_dependency(self.role)