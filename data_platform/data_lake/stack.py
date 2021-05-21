from aws_cdk import core
from aws_cdk import aws_s3 as s3
from data_platform.data_lake.base import BaseDataLakeBucket, DataLakeLayer
from data_platform.active_environment import active_environment


class DataLakeStack(core.Stack):
    def __init__(self, scope: core.Construct, **kwargs):
        self.deploy_env = active_environment
        super().__init__(scope, id=f"{self.deploy_env.value}-data-lake-stack", **kwargs)

        self.data_lake_bronze_bucket = BaseDataLakeBucket(
            self, layer=DataLakeLayer.BRONZE
        )
        self.data_lake_silver_bucket = BaseDataLakeBucket(
            self, layer=DataLakeLayer.SILVER
        )
        self.data_lake_gold_bucket = BaseDataLakeBucket(self, layer=DataLakeLayer.GOLD)
