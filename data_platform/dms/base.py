import json
from aws_cdk import core
from aws_cdk import aws_iam as iam, aws_dms as dms, aws_ec2 as ec2
from data_platform.data_lake.base import BaseDataLakeBucket
from data_platform.common_stack import CommonResourcesStack
from data_platform.rds.stack import RDSStack
from data_platform.active_environment import active_environment
from data_platform.definitions import db_password, db_username, db_name


class RawDMSRole(iam.Role):
    """
    Create DMS Role and Policies
    """

    def __init__(
        self, scope: core.Construct, data_lake_bronze_bucket: BaseDataLakeBucket, **kwargs
    ):
        self.deploy_env = active_environment
        self.data_lake_bronze_bucket = data_lake_bronze_bucket

        super().__init__(
            scope=scope,
            id=f"iam-{self.deploy_env.value}-data-lake-raw-dms-role",
            assumed_by=iam.ServicePrincipal("dms.amazonaws.com"),
            description="Role to allow DMS to save data to data lake raw",
        )

        self.add_policy()

        self.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonDMSVPCManagementRole")
        )

    def add_policy(self):
        """
        Policy that allows DMS to save to S3
        """
        policy = iam.Policy(
            self,
            id=f"iam-{self.deploy_env.value}-data-lake-raw-dms-policy",
            policy_name=f"iam-{self.deploy_env.value}-data-lake-raw-dms-policy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                    ],
                    resources=[
                        self.data_lake_bronze_bucket.bucket_arn,
                        f"{self.data_lake_bronze_bucket.bucket_arn}/*",
                    ],
                )
            ],
        )
        self.attach_inline_policy(policy)

        return policy


class DMSEndpoints:
    def __init__(
        self,
        scope: core.Construct,
        rds_stack: RDSStack,
        data_lake_bronze_bucket: BaseDataLakeBucket,
        **kwargs,
    ):
        self.deploy_env = active_environment
        self.rds_stack = rds_stack
        self.data_lake_bronze_bucket = data_lake_bronze_bucket

        self.rds_endpoint = dms.CfnEndpoint(
            scope=scope,
            id=f"dms-{self.deploy_env.value}-ecommerce-rds-endpoint",
            endpoint_type="source",
            endpoint_identifier=f"dms-source-{self.deploy_env.value}-ecommerce-rds-endpoint",
            engine_name="postgres",
            password=db_password,  # should not be hardcoded. Move to SecretsManager and use dynamic reference
            username=db_username,
            database_name=db_name,
            port=5432,
            server_name=self.rds_stack.ecommerce_rds.db_instance_endpoint_address,
            extra_connection_attributes="captureDDLs=Y",  # Capture changes in tables
        )

        self.s3_endpoint = dms.CfnEndpoint(
            scope=scope,
            id=f"dms-{self.deploy_env.value}-ecommerce-s3-endpoint",
            endpoint_type="target",
            endpoint_identifier=f"dms-target-{self.deploy_env.value}-ecommerce-s3-endpoint",
            engine_name="s3",
            extra_connection_attributes="DataFormat=parquet;maxFileSize=131072;timestampColumnName=extracted_at;includeOpForFullLoad=true;cdcMaxBatchInterval=120",
            s3_settings=dms.CfnEndpoint.S3SettingsProperty(
                bucket_name=self.data_lake_bronze_bucket.bucket_name,
                bucket_folder="ecommerce_rds",
                compression_type="gzip",
                csv_delimiter=",",
                csv_row_delimiter="\n",
                service_access_role_arn=RawDMSRole(
                    scope, self.data_lake_bronze_bucket
                ).role_arn,
            ),
        )


class ReplicationInstance:
    def __init__(
        self, scope: core.Construct, common_stack: CommonResourcesStack, **kwargs
    ):
        self.deploy_env = active_environment
        self.common_stack = common_stack
        # Security Group
        self.dms_sg = ec2.SecurityGroup(
            scope=scope,
            id=f"dms-{self.deploy_env.value}-sg",
            vpc=self.common_stack.custom_vpc,
            security_group_name=f"dms-{self.deploy_env.value}-sg",
        )
        # Subnet Group
        self.dms_subnet_group = dms.CfnReplicationSubnetGroup(
            scope=scope,
            id=f"dms-{self.deploy_env.value}-replication-subnet",
            replication_subnet_group_description="dms replication instance subnet group",
            subnet_ids=[
                subnet.subnet_id
                for subnet in self.common_stack.custom_vpc.public_subnets # use public subnet to avoid costs
            ],
            replication_subnet_group_identifier=f"dms-{self.deploy_env.value}-replication-subnet",
        )
        # Replication Instance
        self.instance = dms.CfnReplicationInstance(
            scope=scope,
            id=f"dms-{self.deploy_env.value}-replication-instance",
            allocated_storage=100,
            publicly_accessible=False,
            engine_version="3.4.4",
            replication_instance_class="dms.t2.small",
            replication_instance_identifier=f"dms-{self.deploy_env.value}-replication-instance",
            vpc_security_group_ids=[self.dms_sg.security_group_id],
            replication_subnet_group_identifier=self.dms_subnet_group.replication_subnet_group_identifier,
        )


        self.instance.node.add_dependency(self.dms_sg)
        self.instance.node.add_dependency(self.dms_subnet_group)


class ReplicationTask(dms.CfnReplicationTask):
    def __init__(
        self,
        scope: core.Construct,
        common_stack: CommonResourcesStack,
        rds_stack: RDSStack,
        data_lake_bronze_bucket: BaseDataLakeBucket,
        # dms_endpoints: DMSEndpoints,
        # dms_replication_instance: ReplicationInstance,
        **kwargs,
    ):
        self.deploy_env = active_environment
        # self.rds_endpoint = dms_endpoints.rds_endpoint
        # self.s3_endpoint = dms_endpoints.s3_endpoint
        # self.instance = dms_replication_instance

        self.endpoints = DMSEndpoints(scope=scope, rds_stack=rds_stack, data_lake_bronze_bucket=data_lake_bronze_bucket)
        self.instance = ReplicationInstance(scope=scope, common_stack=common_stack).instance


        super().__init__(
            scope=scope,
            id=f"{self.deploy_env.value}-dms-task-ecommerce-rds",
            migration_type="full-load-and-cdc",
            replication_task_identifier=f"{self.deploy_env.value}-dms-task-ecommerce-rds",
            replication_instance_arn=self.instance.ref,
            source_endpoint_arn=self.endpoints.rds_endpoint.ref,
            target_endpoint_arn=self.endpoints.s3_endpoint.ref,
            table_mappings=json.dumps(
                {
                    "rules": [
                        {
                            "rule-type": "selection",
                            "rule-id": "1",
                            "rule-name": "1",
                            "object-locator": {
                                "schema-name": "%",  # All schemas
                                "table-name": "%",  # All tables
                            },
                            "rule-action": "include",
                            "filters": [],
                        }
                    ]
                }
            ),
        )
