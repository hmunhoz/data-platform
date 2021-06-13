from aws_cdk import core
from aws_cdk import (
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
)
from data_platform.active_environment import active_environment


class CommonResourcesStack(core.Stack):
    def __init__(self, scope: core.Construct, **kwargs):
        self.deploy_env = active_environment
        super().__init__(scope, id=f"{self.deploy_env.value}-common-stack", **kwargs)

        # Roles for DMS
        self.dms_vpc_management_role = iam.Role(
            self,
            id="dms-vpc-role",
            assumed_by=iam.ServicePrincipal("dms.amazonaws.com"),
            description="Amazon DMS VPC Management Role",
        )

        # self.dms_policy.attach_to_role(self.dms_vpc_management_role)
        self.dms_vpc_management_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AmazonDMSVPCManagementRole"
            )
        )

        # VPC
        self.custom_vpc = ec2.Vpc(
            self, id=f"vpc-{self.deploy_env.value}", nat_gateways=0, vpn_gateway=False
        )

        self.custom_vpc.node.add_dependency(self.dms_vpc_management_role)

        # Script Bucket
        self.s3_script_bucket = s3.Bucket(
            self,
            id=f"script-bucket-{self.deploy_env.value}",
            bucket_name=f"script-bucket-{self.deploy_env.value}-{self.account}-{self.region}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            **kwargs,
        )

        # Log bucket
        self.s3_log_bucket = s3.Bucket(
            self,
            id=f"log-bucket-{self.deploy_env.value}",
            bucket_name=f"log-bucket-{self.deploy_env.value}-{self.account}-{self.region}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            **kwargs,
        )
