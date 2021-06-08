from aws_cdk import core
from aws_cdk import aws_emr as emr, aws_iam as iam
from data_platform.data_lake.base import BaseDataLakeBucket
from data_platform.active_environment import active_environment


class EMRStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        data_lake_bronze_bucket: BaseDataLakeBucket,
        data_lake_silver_bucket: BaseDataLakeBucket,
        data_lake_gold_bucket: BaseDataLakeBucket,
        **kwargs
    ) -> None:
        self.deploy_env = active_environment

