from aws_cdk import core
from aws_cdk import aws_emr as emr, aws_iam as iam
from data_platform.data_lake.base import BaseDataLakeBucket
from data_platform.active_environment import active_environment


class EMRLayer(Enum):
    BRONZE_SILVER = "bronze_silver"
    SILVER_GOLD = "silver_gold"


class EMRRole(iam.Role):
    def __init__(
        self,
        scope: core.Construct,
        **kwargs
    ):
        self.deploy_env = active_environment

        super().__init__(
            scope=scope,
            id=f"iam-{self.deploy_env.value}-emr-role",
            role_name="EMR_Role",
            assumed_by=iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
            description="Role EMR",
        )

        self.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonElasticMapReduceRole")
        )

class EMREC2Role(iam.Role):
    def __init__(
        self,
        scope: core.Construct,
        data_lake_source_bucket: BaseDataLakeBucket,
        data_lake_target_bucket: BaseDataLakeBucket,
        **kwargs
    ):
        self.deploy_env = active_environment
        self.data_lake_source_bucket = data_lake_source_bucket
        self.data_lake_target_bucket = data_lake_target_bucket

        super().__init__(
            scope=scope,
            id=f"iam-{self.deploy_env.value}-emr-ec2-role",
            role_name="EMR_EC2_Role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            description="Role EC2 for EMR",
        )

        self.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonElasticMapReduceforEC2Role")
        )

        self.add_policy()

    def add_policy(self):
        """
        Policy that allows EC2 to save to S3
        """
        policy = iam.Policy(
            self,
            id=f"iam-{self.deploy_env.value}-data-lake-raw-dms-policy",
            policy_name=f"iam-{self.deploy_env.value}-data-lake-raw-dms-policy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:*",
                        ],
                    resources=[
                        self.data_lake_source_bucket.bucket_arn,
                        f"{self.data_lake_source_bucket.bucket_arn}/*",
                        self.data_lake_target_bucket.bucket_arn,
                        f"{self.data_lake_target_bucket.bucket_arn}/*",
                    ],
                )
            ],
        )
        self.attach_inline_policy(policy)

        return policy


class EmrEc2InstanceProfile(iam.CfnInstanceProfile):


class EMRCluster(emr.CfnCluster):
    def __init__(
            self,
            scope: core.Construct,
            layer: EMRLayer,
            data_lake_source_bucket: BaseDataLakeBucket,
            data_lake_target_bucket: BaseDataLakeBucket,
            **kwargs):
        self.deploy_env = scope.deploy_env
        self.layer = layer
        self.data_lake_source_bucket = data_lake_source_bucket
        self.data_lake_target_bucket = data_lake_target_bucket
        self.obj_name = (
            f"emr-{self.deploy_env.value}-{self.layer.value}"
        )

        self.cluster = emr.CfnCluster(
            scope=scope,
            id=self.obj_name,
            instances=emr.CfnCluster.JobFlowInstancesConfigProperty(
                core_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
                    instance_count=2,
                    instance_type="m5.xlarge",
                    name=f"emr-instance-group-{self.deploy_env.value}-{self.layer.value}",
                ),
                keep_job_flow_alive_when_no_steps=True,
                termination_protected=True
            ),
            job_flow_role=EMREC2Role(
                scope,
                data_lake_source_bucket=self.data_lake_source_bucket,
                data_lake_target_bucket=self.data_lake_target_bucket,
            ),
            name=f"emr-cluster-{self.deploy_env.value}-{self.layer.value}",
            service_role=EMRRole(scope),
            visible_to_all_users=True
        )
