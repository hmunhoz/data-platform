from aws_cdk import core
from data_platform.common_stack import CommonResourcesStack
from data_platform.data_lake.base import BaseDataLakeBucket
from data_platform.rds.stack import RDSStack
from data_platform.dms.base import (
    ReplicationTask,
    DMSEndpoints,
    RawDMSRole,
    ReplicationInstance,
)
from data_platform.active_environment import active_environment


class DMSStack(core.Stack):
    def __init__(
            self,
            scope: core.Construct,
            common_stack: CommonResourcesStack,
            rds_stack: RDSStack,
            data_lake_bronze_bucket: BaseDataLakeBucket,
            **kwargs,
    ) -> None:
        self.deploy_env = active_environment
        self.data_lake_bronze_bucket = data_lake_bronze_bucket

        super().__init__(scope, id=f"{self.deploy_env.value}-dms-stack", **kwargs)

        # self.dms_endpoints =DMSEndpoints(
        #                 scope=scope,
        #                 rds_stack=rds_stack,
        #                 data_lake_bronze_bucket=data_lake_bronze_bucket,
        #             ),
        # self.dms_replication_instance =ReplicationInstance(
        #                 scope=scope, common_stack=common_stack
        #             ),

        self.dms_replication_task = ReplicationTask(self, common_stack=common_stack, rds_stack=rds_stack, data_lake_bronze_bucket=data_lake_bronze_bucket)
