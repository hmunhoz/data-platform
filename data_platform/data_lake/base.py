from enum import Enum
from aws_cdk import core
from aws_cdk import (
    aws_s3 as s3,
)


Fn = core.Fn

class DataLakeLayer(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class BaseDataLakeBucket(s3.Bucket):
    def __init__(self, scope: core.Construct, layer: DataLakeLayer, **kwargs):
        self.layer = layer
        self.deploy_env = scope.deploy_env
        self.account_id = scope.account
        self.obj_name = (
            f"datalake-{self.deploy_env.value}-{self.layer.value}"
        )

        super().__init__(
            scope,
            id=self.obj_name,
            bucket_name=self.obj_name + f"-{self.account_id}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            **kwargs,
        )
